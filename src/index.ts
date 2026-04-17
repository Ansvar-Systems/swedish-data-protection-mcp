#!/usr/bin/env node

/**
 * Swedish Data Protection MCP — stdio entry point.
 *
 * Provides MCP tools for querying IMY decisions, sanctions, and
 * data protection guidance documents.
 *
 * Tool prefix: se_dp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  searchDecisions,
  getDecision,
  searchGuidelines,
  getGuideline,
  listTopics,
} from "./db.js";
import { buildCitation } from "./utils/citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "swedish-data-protection-mcp";

// --- Tool definitions ---------------------------------------------------------

const TOOLS = [
  {
    name: "se_dp_search_decisions",
    description:
      "Full-text search across IMY decisions (tillsynsbeslut, sanctions, ingripanden). Returns matching decisions with reference, entity name, fine amount, and GDPR articles cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query in Swedish (e.g., 'samtycke cookies', 'kamerabevakning', 'Google')",
        },
        type: {
          type: "string",
          enum: ["sanction", "tillsynsbeslut", "ingripande", "yttrande"],
          description: "Filter by decision type. Optional.",
        },
        topic: {
          type: "string",
          description: "Filter by topic ID (e.g., 'samtycke', 'cookies', 'tredjelandsoverfoering'). Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "se_dp_get_decision",
    description:
      "Get a specific IMY decision by reference number (e.g., 'DI-2020-11332', 'IMY-2022-6524').",
    inputSchema: {
      type: "object" as const,
      properties: {
        reference: {
          type: "string",
          description: "IMY decision reference (e.g., 'DI-2020-11332', 'IMY-2022-6524')",
        },
      },
      required: ["reference"],
    },
  },
  {
    name: "se_dp_search_guidelines",
    description:
      "Search IMY guidance documents: vägledningar, riktlinjer, and ställningstaganden. Covers GDPR implementation, DPIA methodology, cookie consent, kamerabevakning, and more.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query in Swedish (e.g., 'kamerabevakning', 'konsekvensbedömning', 'cookies')",
        },
        type: {
          type: "string",
          enum: ["vagledning", "riktlinje", "stallningstagande", "FAQ"],
          description: "Filter by guidance type. Optional.",
        },
        topic: {
          type: "string",
          description: "Filter by topic ID (e.g., 'konsekvensbedömning', 'cookies', 'tredjelandsoverfoering'). Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "se_dp_get_guideline",
    description:
      "Get a specific IMY guidance document by its database ID.",
    inputSchema: {
      type: "object" as const,
      properties: {
        id: {
          type: "number",
          description: "Guideline database ID (from se_dp_search_guidelines results)",
        },
      },
      required: ["id"],
    },
  },
  {
    name: "se_dp_list_topics",
    description:
      "List all covered data protection topics with Swedish and English names. Use topic IDs to filter decisions and guidelines.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "se_dp_about",
    description: "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation --------------------------------------

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["sanction", "tillsynsbeslut", "ingripande", "yttrande"]).optional(),
  topic: z.string().optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetDecisionArgs = z.object({
  reference: z.string().min(1),
});

const SearchGuidelinesArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["vagledning", "riktlinje", "stallningstagande", "FAQ"]).optional(),
  topic: z.string().optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetGuidelineArgs = z.object({
  id: z.number().int().positive(),
});

// --- Helper ------------------------------------------------------------------

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

// --- Server setup ------------------------------------------------------------

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "se_dp_search_decisions": {
        const parsed = SearchDecisionsArgs.parse(args);
        const results = searchDecisions({
          query: parsed.query,
          type: parsed.type,
          topic: parsed.topic,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "se_dp_get_decision": {
        const parsed = GetDecisionArgs.parse(args);
        const decision = getDecision(parsed.reference);
        if (!decision) {
          return errorContent(`Decision not found: ${parsed.reference}`);
        }
        const _citation = buildCitation(
          parsed.reference,
          (decision as unknown as Record<string, unknown>).title as string || parsed.reference,
          "se_dp_get_decision",
          { reference: parsed.reference },
        );
        return textContent({ ...decision as unknown as Record<string, unknown>, _citation });
      }

      case "se_dp_search_guidelines": {
        const parsed = SearchGuidelinesArgs.parse(args);
        const results = searchGuidelines({
          query: parsed.query,
          type: parsed.type,
          topic: parsed.topic,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "se_dp_get_guideline": {
        const parsed = GetGuidelineArgs.parse(args);
        const guideline = getGuideline(parsed.id);
        if (!guideline) {
          return errorContent(`Guideline not found: id=${parsed.id}`);
        }
        const _citation = buildCitation(
          String(parsed.id),
          (guideline as unknown as Record<string, unknown>).title as string || `Guideline ${parsed.id}`,
          "se_dp_get_guideline",
          { id: String(parsed.id) },
        );
        return textContent({ ...guideline as unknown as Record<string, unknown>, _citation });
      }

      case "se_dp_list_topics": {
        const topics = listTopics();
        return textContent({ topics, count: topics.length });
      }

      case "se_dp_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "IMY (Integritetsskyddsmyndigheten) MCP server. Provides access to Swedish data protection authority decisions, sanctions, tillsynsbeslut, and official guidance documents.",
          data_source: "IMY (https://www.imy.se/)",
          coverage: {
            decisions: "IMY tillsynsbeslut, sanctions, and ingripanden",
            guidelines: "IMY vägledningar, riktlinjer, and ställningstaganden",
            topics: "Kamerabevakning, cookies, arbetsgivare, hälsodata, samtycke, registerutdrag, konsekvensbedömning, tredjelandsöverföring, profilering",
          },
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

// --- Main --------------------------------------------------------------------

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
