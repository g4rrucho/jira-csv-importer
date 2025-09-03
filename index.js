import fs from "fs";
import path from "path";
import axios from "axios";
import { parse } from "csv-parse";
import dotenv from "dotenv";

dotenv.config();

const {
  JIRA_BASE_URL,
  JIRA_EMAIL,
  JIRA_API_TOKEN,
  PROJECT_KEY,
  CSV_PATH = "./issues.csv",
  CSV_DELIMITER = ",",
} = process.env;

if (!JIRA_BASE_URL || !JIRA_EMAIL || !JIRA_API_TOKEN || !PROJECT_KEY) {
  console.error("Missing env vars. Please set JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN, PROJECT_KEY.");
  process.exit(1);
}

const api = axios.create({
  baseURL: `${JIRA_BASE_URL}/rest/api/3`,
  auth: { username: JIRA_EMAIL, password: JIRA_API_TOKEN },
  headers: { "Accept": "application/json", "Content-Type": "application/json" },
  validateStatus: () => true,
});

// -------- Helpers --------

async function requestWithRetry(fn, desc = "request") {
  const max = 5;
  let attempt = 0;
  let wait = 1000;
  while (true) {
    attempt++;
    const res = await fn();
    if (res.status < 400) return res;
    if (attempt >= max || !(res.status === 429 || res.status >= 500)) {
      throw new Error(`${desc} failed (status ${res.status}): ${JSON.stringify(res.data)}`);
    }
    const retryAfter = Number(res.headers["retry-after"]);
    const sleepMs = !isNaN(retryAfter) ? retryAfter * 1000 : wait;
    await new Promise(r => setTimeout(r, sleepMs));
    wait = Math.min(wait * 2, 15000);
  }
}

async function getFields() {
  const res = await requestWithRetry(() => api.get("/field"), "get fields");
  const epicLinkField    = res.data.find(f => f.name.toLowerCase() === "epic link");
  const storyPointsField = res.data.find(f => f.name.toLowerCase() === "story points");
  const epicNameField    = res.data.find(f => f.name.toLowerCase() === "epic name");
  return {
    epicLinkId: epicLinkField?.id || null,
    storyPointsId: storyPointsField?.id || null,
    epicNameId: epicNameField?.id || null,
  };
}

function parseCsv(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(parse({
        columns: true,
        skip_empty_lines: true,
        bom: true,
        relax_column_count: true,
        relax_quotes: true,
        delimiter: CSV_DELIMITER || ",",
        trim: true,
      }))
      .on("data", r => rows.push(r))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

// --- ADF (Atlassian Document Format) builders ---

function isLikelyUrl(s) {
  return /^https?:\/\/\S+$/i.test(s.trim());
}

function adfTextNode(text) {
  return { type: "text", text };
}

function adfLinkNode(url, text) {
  return {
    type: "text",
    text: text || url,
    marks: [{ type: "link", attrs: { href: url } }],
  };
}

function toADF(description) {
  // If no description, return null so we can omit the field
  if (!description || !String(description).trim()) return null;

  const lines = String(description).replace(/\r\n/g, "\n").split("\n");

  const content = lines.map(line => {
    const t = line.trim();
    if (!t) {
      return { type: "paragraph" }; // empty paragraph for blank line
    }
    if (isLikelyUrl(t)) {
      return { type: "paragraph", content: [adfLinkNode(t)] };
    }
    return { type: "paragraph", content: [adfTextNode(line)] };
  });

  return {
    version: 1,
    type: "doc",
    content,
  };
}

// Build Jira fields payload from a CSV row
function buildFields(row, opts) {
  const { epicLinkId, storyPointsId, epicNameId, epicMap } = opts;
  const projectKey = row["Project Key"]?.trim() || PROJECT_KEY;
  const issueType = row["Issue Type"]?.trim() || "Task";
  const summary = row["Summary"]?.trim();
  if (!summary) throw new Error("Row missing 'Summary'");

  const fields = {
    project: { key: projectKey },
    issuetype: { name: issueType },
    summary,
    labels: (row["Labels"] || "").split(/[,; ]+/).filter(Boolean),
  };

  // Description -> ADF
  const adf = toADF(row["Description"]);
  if (adf) {
    fields.description = adf;
  }

  // Story points
  if (row["Story Points"] && storyPointsId) {
    const sp = Number(row["Story Points"]);
    if (!isNaN(sp)) fields[storyPointsId] = sp;
  }

  const isTeamManaged = (String(process.env.TEAM_MANAGED || "false").toLowerCase() === "true");

  // Epics
  if (issueType.toLowerCase() === "epic") {
    // Company-managed often requires Epic Name custom field. Team-managed ignores it.
    if (!isTeamManaged && epicNameId) {
      fields[epicNameId] = row["Epic Name"]?.trim() || summary;
    }
    return fields;
  }

  // Sub-task → Parent
  const parentId = row["Parent Id"]?.trim();
  if (issueType.toLowerCase() === "sub-task" && parentId) {
    const parent = epicMap.get(parentId);
    if (parent) fields.parent = { id: parent.id };
  }

  // Link to Epic
  const epicLink = row["Epic Link"]?.trim();
  if (epicLink) {
    const epic = epicMap.get(epicLink);
    if (epic) {
      if (isTeamManaged) {
        fields.parent = { id: epic.id };
      } else if (epicLinkId) {
        fields[epicLinkId] = epic.key; // company-managed expects the Epic KEY
      }
    }
  }

  return fields;
}

async function createIssue(fields) {
  const res = await requestWithRetry(() => api.post("/issue", { fields }), "create issue");
  return res.data; // { id, key, self }
}

// -------- Main --------

(async function main() {
  try {
    const csvPath = path.resolve(CSV_PATH);
    if (!fs.existsSync(csvPath)) {
      console.error(`CSV not found at ${csvPath}`);
      process.exit(1);
    }

    console.log("Reading CSV:", csvPath);
    const rows = await parseCsv(csvPath);
    if (!rows.length) {
      console.error("CSV has no data rows.");
      process.exit(1);
    }

    const { epicLinkId, storyPointsId, epicNameId } = await getFields();
    console.log("Epic Link:", epicLinkId, "| Story Points:", storyPointsId, "| Epic Name:", epicNameId);

    const epicMap = new Map(); // CSV "Issue Id" -> { id, key }

    // Pass 1: create Epics
    for (const row of rows) {
      if ((row["Issue Type"] || "").toLowerCase() === "epic") {
        const fields = buildFields(row, { epicLinkId, storyPointsId, epicNameId, epicMap });
        const resp = await createIssue(fields);
        epicMap.set((row["Issue Id"] || row["Summary"]).trim(), { id: resp.id, key: resp.key });
        console.log("Created Epic:", resp.key, "-", row["Summary"]);
      }
    }

    // Pass 2: create the rest
    for (const row of rows) {
      if ((row["Issue Type"] || "").toLowerCase() !== "epic") {
        const fields = buildFields(row, { epicLinkId, storyPointsId, epicNameId, epicMap });
        const resp = await createIssue(fields);
        epicMap.set((row["Issue Id"] || row["Summary"]).trim(), { id: resp.id, key: resp.key });
        console.log(`Created ${row["Issue Type"]}:`, resp.key, "-", row["Summary"]);
      }
    }

    fs.writeFileSync("import-report.json", JSON.stringify([...epicMap], null, 2));
    console.log("Done. Report saved to import-report.json");
  } catch (err) {
    console.error("Import failed:", err.message);
    process.exit(1);
  }
})();
