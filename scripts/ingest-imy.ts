/**
 * Ingest IMY (Integritetsskyddsmyndigheten) decisions and guidance into SQLite.
 *
 * Data sources:
 *   1. Tillsyner listing   — paginated list of supervision cases
 *   2. Praxisbeslut        — practice decisions with structured metadata
 *   3. Sanktionsavgifter   — fine amounts by entity and year
 *   4. Guidance pages      — GDPR guidance and vägledningar
 *
 * The crawler scrapes imy.se, extracts structured data from listing pages
 * and individual decision pages, then inserts into the existing SQLite schema.
 *
 * Usage:
 *   npx tsx scripts/ingest-imy.ts
 *   npx tsx scripts/ingest-imy.ts --resume      # skip already-ingested references
 *   npx tsx scripts/ingest-imy.ts --dry-run      # parse and log, do not write DB
 *   npx tsx scripts/ingest-imy.ts --force        # drop and recreate DB first
 *
 * Environment:
 *   IMY_DB_PATH  — SQLite database path (default: data/imy.db)
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["IMY_DB_PATH"] ?? "data/imy.db";
const STATE_PATH = resolve(dirname(DB_PATH), ".ingest-state.json");
const BASE_URL = "https://www.imy.se";

const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3_000;
const REQUEST_TIMEOUT_MS = 30_000;

/** Listing page size observed on imy.se */
const PAGE_SIZE = 10;
/** Max pages to crawl (safety bound) */
const MAX_PAGES = 30;

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DecisionRow {
  reference: string;
  title: string;
  date: string | null;
  type: string;
  entity_name: string | null;
  fine_amount: number | null;
  summary: string | null;
  full_text: string;
  topics: string | null;
  gdpr_articles: string | null;
  status: string;
}

interface GuidelineRow {
  reference: string | null;
  title: string;
  date: string | null;
  type: string;
  summary: string | null;
  full_text: string;
  topics: string | null;
  language: string;
}

interface TopicRow {
  id: string;
  name_local: string;
  name_en: string;
  description: string;
}

interface TillsynListItem {
  title: string;
  url: string;
  status: string | null;
  date: string | null;
  categories: string[];
  description: string | null;
}

interface SanctionEntry {
  entity_name: string;
  fine_amount: number;
  year: string;
  link_hash: string | null;
}

interface PraxisEntry {
  section: string;
  title: string;
  date: string | null;
  corrective_action: string | null;
  legal_reference: string | null;
  keywords: string | null;
  appeal: string | null;
  final_judgment: string | null;
  summary: string | null;
  link_hash: string | null;
}

interface IngestState {
  ingested_decision_refs: string[];
  ingested_guideline_refs: string[];
  last_run: string;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19).replace("T", " ");
  console.log(`[${ts}] ${msg}`);
}

function warn(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19).replace("T", " ");
  console.warn(`[${ts}] WARN: ${msg}`);
}

function err(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19).replace("T", " ");
  console.error(`[${ts}] ERROR: ${msg}`);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<Response> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const resp = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent":
          "AnsvarIMYCrawler/1.0 (+https://ansvar.eu; data-protection-research)",
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.5",
      },
    });
    return resp;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchHtml(url: string): Promise<string | null> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await rateLimitedFetch(url);
      if (!resp.ok) {
        warn(`HTTP ${resp.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`);
        if (resp.status === 404) return null;
        if (attempt < MAX_RETRIES) await sleep(RETRY_BACKOFF_MS * attempt);
        continue;
      }
      return await resp.text();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      warn(`Fetch error for ${url}: ${msg} (attempt ${attempt}/${MAX_RETRIES})`);
      if (attempt < MAX_RETRIES) await sleep(RETRY_BACKOFF_MS * attempt);
    }
  }
  err(`Failed to fetch ${url} after ${MAX_RETRIES} attempts`);
  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function resolveUrl(path: string): string {
  if (path.startsWith("http")) return path;
  return `${BASE_URL}${path.startsWith("/") ? "" : "/"}${path}`;
}

// ---------------------------------------------------------------------------
// Resume state
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (existsSync(STATE_PATH)) {
    try {
      return JSON.parse(readFileSync(STATE_PATH, "utf-8")) as IngestState;
    } catch {
      warn("Corrupt state file, starting fresh");
    }
  }
  return { ingested_decision_refs: [], ingested_guideline_refs: [], last_run: "" };
}

function saveState(state: IngestState): void {
  state.last_run = new Date().toISOString();
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------------

/**
 * Normalize various date formats found on imy.se to YYYY-MM-DD.
 * Handles:  "2023-06-26", "23-06-26", "26 January 2026", "2023-11-28", etc.
 */
function normalizeDate(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const trimmed = raw.trim();

  // Already YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;

  // YY-MM-DD (short year, used in praxisbeslut)
  const shortMatch = trimmed.match(/^(\d{2})-(\d{2})-(\d{2})$/);
  if (shortMatch) {
    const yr = parseInt(shortMatch[1]!, 10);
    const century = yr >= 50 ? "19" : "20";
    return `${century}${shortMatch[1]}-${shortMatch[2]}-${shortMatch[3]}`;
  }

  // Swedish month names
  const svMonths: Record<string, string> = {
    januari: "01", februari: "02", mars: "03", april: "04",
    maj: "05", juni: "06", juli: "07", augusti: "08",
    september: "09", oktober: "10", november: "11", december: "12",
  };

  // "26 januari 2026" or "26 January 2026"
  const longMatch = trimmed.match(/(\d{1,2})\s+(\w+)\s+(\d{4})/);
  if (longMatch) {
    const day = longMatch[1]!.padStart(2, "0");
    const monthStr = longMatch[2]!.toLowerCase();
    const month = svMonths[monthStr];
    if (month) return `${longMatch[3]}-${month}-${day}`;
  }

  // Try Date.parse as last resort
  const parsed = Date.parse(trimmed);
  if (!isNaN(parsed)) {
    const d = new Date(parsed);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Fine amount parsing
// ---------------------------------------------------------------------------

/**
 * Parse fine amounts from Swedish text.
 * Handles "6 000 000 SEK", "6,000,000 SEK", "6 000 000 kronor",
 * "35 000 EUR", combined amounts like "300 000 + 1 600 000 SEK".
 * Returns the total amount in SEK (EUR converted at approximate rate).
 */
function parseFineAmount(text: string | null | undefined): number | null {
  if (!text) return null;
  const cleaned = text.trim();
  if (!cleaned) return null;

  // Find all amount patterns in the text
  const amountPattern = /(\d[\d\s,.]*\d)\s*(?:SEK|kronor|kr|EUR|euro)/gi;
  let total = 0;
  let found = false;
  const isEur = /EUR|euro/i.test(cleaned);

  let match: RegExpExecArray | null;
  while ((match = amountPattern.exec(cleaned)) !== null) {
    const numStr = match[1]!.replace(/[\s.]/g, "").replace(",", ".");
    const val = parseFloat(numStr);
    if (!isNaN(val) && val > 0) {
      total += val;
      found = true;
    }
  }

  // If no unit-suffixed match, try bare numbers with common separators
  if (!found) {
    const barePattern = /(\d[\d\s]*\d)/g;
    while ((match = barePattern.exec(cleaned)) !== null) {
      const numStr = match[1]!.replace(/\s/g, "");
      const val = parseFloat(numStr);
      if (!isNaN(val) && val > 0) {
        total += val;
        found = true;
        break; // only take the first bare number
      }
    }
  }

  if (!found) return null;

  // Convert EUR to SEK at approximate rate (for Nusvar AB case)
  if (isEur) {
    total = Math.round(total * 11.5); // approximate EUR→SEK
  }

  return total;
}

// ---------------------------------------------------------------------------
// GDPR article extraction
// ---------------------------------------------------------------------------

/**
 * Extract GDPR article references from text.
 * Looks for patterns like "artikel 32", "art. 6.1 f", "articles 12.1, 12.3",
 * "artiklarna 5.1 a, 5.1 c".
 */
function extractGdprArticles(text: string): string[] {
  const articles = new Set<string>();

  // Pattern: "artikel(n|arna)? NN" or "art. NN" or "article(s)? NN"
  // followed by optional sub-references like ".1", ".1 a", "(1)(a)"
  const patterns = [
    /(?:artik(?:el|eln|larna)|art\.?)\s+([\d.,\s]+(?:\.\d+)?(?:\s*[a-z])?(?:\s*(?:och|and|,|samt)\s*[\d.]+(?:\s*[a-z])?)*)/gi,
    /(?:articles?)\s+([\d.,\s]+(?:\.\d+)?(?:\s*[a-z])?(?:\s*(?:and|,|samt|och)\s*[\d.]+(?:\s*[a-z])?)*)/gi,
  ];

  for (const pattern of patterns) {
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      const refs = match[1]!;
      // Split on "och", "and", ",", "samt"
      const parts = refs.split(/\s*(?:och|and|samt|,)\s*/);
      for (const part of parts) {
        const cleaned = part.trim().replace(/\s+/g, "");
        if (cleaned && /^\d/.test(cleaned)) {
          // Normalize: strip trailing letters for the base article number
          const baseMatch = cleaned.match(/^(\d+(?:\.\d+)?)/);
          if (baseMatch) {
            articles.add(baseMatch[1]!);
          }
        }
      }
    }
  }

  // Also match "GDPR" followed by article references
  const gdprPattern = /(?:GDPR|dataskyddsförordningen).*?(?:artikel|art\.?)\s*(\d+)/gi;
  let gdprMatch: RegExpExecArray | null;
  while ((gdprMatch = gdprPattern.exec(text)) !== null) {
    articles.add(gdprMatch[1]!);
  }

  return [...articles].sort((a, b) => {
    const na = parseFloat(a);
    const nb = parseFloat(b);
    return na - nb;
  });
}

// ---------------------------------------------------------------------------
// Topic classification
// ---------------------------------------------------------------------------

/** Map Swedish keywords to topic IDs. */
const KEYWORD_TO_TOPIC: Record<string, string> = {
  kamerabevakning: "camera_surveillance",
  kamera: "camera_surveillance",
  "ansiktsigenkänning": "camera_surveillance",
  konsekvensbedömning: "dpia",
  "konsekvensbedomning": "dpia",
  "impact assessment": "dpia",
  marknadsföring: "marketing",
  "marknadsforing": "marketing",
  profilering: "marketing",
  profiling: "marketing",
  "direktmarknadsföring": "marketing",
  samtycke: "consent",
  consent: "consent",
  informationssäkerhet: "information_security",
  "informationssakerhet": "information_security",
  "tekniska och organisatoriska": "information_security",
  "säkerhetsåtgärder": "information_security",
  "security measures": "information_security",
  dataskyddsombud: "dpo",
  "data protection officer": "dpo",
  "registrerades rättigheter": "data_subject_rights",
  "dina rättigheter": "data_subject_rights",
  "data subject rights": "data_subject_rights",
  radering: "data_subject_rights",
  rättelse: "data_subject_rights",
  tillgång: "data_subject_rights",
  "rättslig grund": "legal_basis",
  "rattslig grund": "legal_basis",
  "legal basis": "legal_basis",
  "känsliga personuppgifter": "sensitive_data",
  hälso: "health_data",
  sjukvård: "health_data",
  "health data": "health_data",
  "personuppgiftsincident": "breach_notification",
  incident: "breach_notification",
  "data breach": "breach_notification",
  överföring: "international_transfers",
  "tredjeland": "international_transfers",
  "third country": "international_transfers",
  meta: "international_transfers",
  barn: "children",
  skola: "children",
  "children": "children",
  "school": "children",
  förening: "associations",
  arbetsplats: "employment",
  arbetsliv: "employment",
  "anställd": "employment",
  employee: "employment",
  ai: "ai",
  "artificiell intelligens": "ai",
  "maskininlärning": "ai",
  forskning: "research",
  research: "research",
};

function classifyTopics(
  text: string,
  categories: string[],
  keywords: string | null,
): string[] {
  const topicSet = new Set<string>();
  const searchText = [text, ...categories, keywords ?? ""].join(" ").toLowerCase();

  for (const [keyword, topicId] of Object.entries(KEYWORD_TO_TOPIC)) {
    if (searchText.includes(keyword.toLowerCase())) {
      topicSet.add(topicId);
    }
  }

  return [...topicSet];
}

// ---------------------------------------------------------------------------
// Decision type classification
// ---------------------------------------------------------------------------

function classifyDecisionType(
  corrective_action: string | null,
  fine_amount: number | null,
  text: string,
): string {
  const combined = [corrective_action ?? "", text].join(" ").toLowerCase();

  if (fine_amount && fine_amount > 0) return "sanction";
  if (combined.includes("sanktionsavgift") || combined.includes("administrative fine")) {
    return "sanction";
  }
  if (combined.includes("förbud") || combined.includes("prohibition")) {
    return "prohibition";
  }
  if (combined.includes("föreläggande") || combined.includes("injunction")) {
    return "injunction";
  }
  if (combined.includes("reprimand") || combined.includes("tillrättavisning")) {
    return "reprimand";
  }
  if (combined.includes("avskrivning") || combined.includes("discontinued") || combined.includes("avslutad")) {
    return "discontinued";
  }

  return "decision";
}

// ---------------------------------------------------------------------------
// Scraping: Tillsyner listing pages
// ---------------------------------------------------------------------------

async function scrapeTillsynerListing(): Promise<TillsynListItem[]> {
  log("Scraping tillsyner listing pages...");
  const items: TillsynListItem[] = [];
  let page = 0;

  while (page < MAX_PAGES) {
    const url = `${BASE_URL}/tillsyner/?page=${page}`;
    log(`  Fetching listing page ${page + 1}: ${url}`);
    const html = await fetchHtml(url);
    if (!html) {
      if (page === 0) {
        err("Could not fetch first listing page, aborting tillsyner scrape");
        return items;
      }
      break;
    }

    const $ = cheerio.load(html);
    const pageItems: TillsynListItem[] = [];

    // Each supervision case is a linked block in the listing.
    // The listing uses article/card-like elements with headings and metadata.
    // We look for links to /tillsyner/{slug}/ paths.
    $("a[href*='/tillsyner/']").each((_i, el) => {
      const href = $(el).attr("href");
      if (!href || href === "/tillsyner/" || href.includes("?page=")) return;

      // Avoid duplicate links on same page
      const fullUrl = resolveUrl(href);
      if (items.some((x) => x.url === fullUrl) || pageItems.some((x) => x.url === fullUrl)) {
        return;
      }

      // Extract data from the link and its parent container
      const container = $(el).closest("li, article, div.card, div");
      const title = $(el).text().trim() ||
        container.find("h2, h3, h4").first().text().trim();

      if (!title) return;

      // Look for status text
      let status: string | null = null;
      const statusEl = container.find("[class*='status'], [class*='tag'], span, small");
      statusEl.each((_j, sEl) => {
        const sText = $(sEl).text().trim().toLowerCase();
        if (["beslut", "pågår", "inledd", "överklagan", "avslutad"].some((s) => sText.includes(s))) {
          status = $(sEl).text().trim();
        }
      });

      // Look for date
      let date: string | null = null;
      const dateMatch = container.text().match(/(\d{4}-\d{2}-\d{2})/);
      if (dateMatch) {
        date = dateMatch[1]!;
      }

      // Categories from tag-like elements
      const categories: string[] = [];
      container.find("li, [class*='tag'], [class*='etikett'], [class*='kategori']").each((_j, cEl) => {
        const cText = $(cEl).text().trim();
        if (cText && cText.length < 50 && cText !== title) {
          categories.push(cText);
        }
      });

      // Description text
      const descEl = container.find("p").first();
      const description = descEl.text().trim() || null;

      pageItems.push({
        title,
        url: fullUrl,
        status,
        date,
        categories,
        description,
      });
    });

    if (pageItems.length === 0) {
      log(`  No items found on page ${page + 1}, stopping pagination`);
      break;
    }

    items.push(...pageItems);
    log(`  Found ${pageItems.length} items on page ${page + 1} (total: ${items.length})`);

    // Check if we might have reached the last page
    if (pageItems.length < PAGE_SIZE) {
      log("  Fewer items than page size, likely last page");
      break;
    }

    page++;
  }

  log(`Collected ${items.length} tillsyner listing entries`);
  return items;
}

// ---------------------------------------------------------------------------
// Scraping: Individual tillsyn decision page
// ---------------------------------------------------------------------------

interface TillsynDetail {
  reference: string | null;
  title: string;
  date: string | null;
  entity_name: string;
  summary: string;
  full_text: string;
  categories: string[];
  pdf_url: string | null;
  fine_amount: number | null;
  gdpr_articles: string[];
  status: string;
}

async function scrapeTillsynDetail(url: string, listItem: TillsynListItem): Promise<TillsynDetail | null> {
  const html = await fetchHtml(url);
  if (!html) return null;

  const $ = cheerio.load(html);

  // Title — from the main heading
  const title = $("h1").first().text().trim() || listItem.title;

  // Entity name — usually the h1 on individual pages, or the listing title
  const entity_name = title;

  // Reference number (diarienummer) — look for IMY-YYYY-NNNN pattern
  let reference: string | null = null;
  const bodyText = $("body").text();
  const refMatch = bodyText.match(/IMY-\d{4}-\d+/);
  if (refMatch) {
    reference = refMatch[0];
  }

  // If no IMY reference found, generate one from the URL slug
  if (!reference) {
    const slugMatch = url.match(/\/tillsyner\/([^/]+)\/?$/);
    if (slugMatch) {
      reference = `IMY-TILLSYN-${slugMatch[1]}`;
    }
  }

  // Decision date — from structured data or text
  let date = listItem.date;
  if (!date) {
    // Look for date patterns in the page
    const datePatterns = [
      /(?:Beslutsdatum|Datum för beslut|Beslut|Datum)[:\s]*(\d{4}-\d{2}-\d{2})/i,
      /(\d{1,2})\s+(januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\s+(\d{4})/i,
    ];
    for (const pattern of datePatterns) {
      const match = bodyText.match(pattern);
      if (match) {
        date = normalizeDate(match[0].replace(/^[^0-9]*/, ""));
        if (date) break;
      }
    }
  }

  // Main content area — the article body
  const mainContent = $("main, [role='main'], .main-content, article").first();
  const contentParagraphs: string[] = [];
  (mainContent.length > 0 ? mainContent : $("body")).find("p").each((_i, el) => {
    const text = $(el).text().trim();
    if (text && text.length > 20) {
      contentParagraphs.push(text);
    }
  });

  const full_text = contentParagraphs.join("\n\n");
  if (!full_text || full_text.length < 50) {
    warn(`Insufficient content on ${url}, skipping`);
    return null;
  }

  // Summary — first substantive paragraph or the listing description
  const summary = listItem.description || contentParagraphs[0] || null;

  // PDF link
  let pdf_url: string | null = null;
  $("a[href*='.pdf']").each((_i, el) => {
    const href = $(el).attr("href");
    if (href && (href.includes("beslut") || href.includes("tillsyn") || href.includes("globalassets"))) {
      pdf_url = resolveUrl(href);
      return false; // break
    }
  });

  // Fine amount — search for SEK amounts in text
  let fine_amount: number | null = null;
  const finePatterns = [
    /(?:sanktionsavgift|böter|fine|avgift)[^.]*?(\d[\d\s]+)\s*(?:kronor|SEK|kr)/i,
    /(\d[\d\s]+)\s*(?:kronor|SEK|kr).*?(?:sanktionsavgift|böter|fine)/i,
    /(\d[\d\s]+)\s*(?:kronor|SEK|kr)/i,
  ];
  for (const pattern of finePatterns) {
    const match = bodyText.match(pattern);
    if (match) {
      fine_amount = parseFineAmount(match[0]);
      if (fine_amount) break;
    }
  }

  // GDPR articles referenced in the page text
  const gdpr_articles = extractGdprArticles(bodyText);

  // Categories from listing or page tags
  const categories = [...listItem.categories];
  $("[class*='tag'], [class*='etikett'], [class*='kategori']").each((_i, el) => {
    const tag = $(el).text().trim();
    if (tag && tag.length < 50 && !categories.includes(tag)) {
      categories.push(tag);
    }
  });

  // Status
  let status = "final";
  const statusLower = (listItem.status ?? "").toLowerCase();
  if (statusLower.includes("pågår") || statusLower.includes("inledd")) {
    status = "ongoing";
  } else if (statusLower.includes("överklagan")) {
    status = "appealed";
  } else if (statusLower.includes("beslut") || statusLower.includes("avslutad")) {
    status = "final";
  }

  return {
    reference,
    title,
    date,
    entity_name,
    summary,
    full_text,
    categories,
    pdf_url,
    fine_amount,
    gdpr_articles,
    status,
  };
}

// ---------------------------------------------------------------------------
// Scraping: Praxisbeslut (practice decisions with structured metadata)
// ---------------------------------------------------------------------------

async function scrapePraxisbeslut(): Promise<PraxisEntry[]> {
  log("Scraping praxisbeslut (practice decisions)...");
  const url = `${BASE_URL}/om-oss/beslut-publikationer-och-remisser/praxisbeslut/`;
  const html = await fetchHtml(url);
  if (!html) {
    err("Could not fetch praxisbeslut page");
    return [];
  }

  const $ = cheerio.load(html);
  const entries: PraxisEntry[] = [];

  // The page is organized by topic sections (h2 or h3 headings),
  // with decision entries containing structured metadata in lists.
  let currentSection = "Okategoriserad";

  // Process headings and their following content
  $("h2, h3").each((_i, headingEl) => {
    const heading = $(headingEl).text().trim();
    if (!heading) return;

    // Skip navigation-like headings
    if (heading.length < 3 || heading.includes("Meny") || heading.includes("Sök")) return;

    currentSection = heading;

    // Find content blocks between this heading and the next heading
    let nextEl = $(headingEl).next();
    let entryText = "";
    let entryLink: string | null = null;
    let entrySummary: string | null = null;

    while (nextEl.length > 0 && !nextEl.is("h2, h3")) {
      const tagName = nextEl.prop("tagName")?.toLowerCase();

      if (tagName === "hr") {
        // Separator between entries — flush current entry if we have data
        if (entryText) {
          const entry = parsePraxisEntry(entryText, entrySummary, entryLink, currentSection);
          if (entry) entries.push(entry);
        }
        entryText = "";
        entryLink = null;
        entrySummary = null;
      } else if (tagName === "p") {
        const pText = nextEl.text().trim();
        // Check for decision link
        const linkEl = nextEl.find("a[href*='/link/'], a[href*='.aspx']");
        if (linkEl.length > 0) {
          entryLink = linkEl.attr("href") ?? null;
        } else if (!entrySummary && pText.length > 30) {
          entrySummary = pText;
        }
        entryText += pText + "\n";
      } else if (tagName === "ul") {
        nextEl.find("li").each((_j, li) => {
          entryText += $(li).text().trim() + "\n";
        });
      }

      nextEl = nextEl.next();
    }

    // Flush last entry in this section
    if (entryText) {
      const entry = parsePraxisEntry(entryText, entrySummary, entryLink, currentSection);
      if (entry) entries.push(entry);
    }
  });

  log(`Collected ${entries.length} praxisbeslut entries`);
  return entries;
}

function parsePraxisEntry(
  text: string,
  summary: string | null,
  linkHref: string | null,
  section: string,
): PraxisEntry | null {
  if (!text || text.length < 20) return null;

  const lines = text.split("\n").map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) return null;

  const title = lines[0] ?? section;

  // Extract structured fields from the list items
  let date: string | null = null;
  let corrective_action: string | null = null;
  let legal_reference: string | null = null;
  let keywords: string | null = null;
  let appeal: string | null = null;
  let final_judgment: string | null = null;

  for (const line of lines) {
    const lower = line.toLowerCase();

    if (lower.includes("datum för beslut") || lower.includes("datum:")) {
      const dateStr = line.replace(/^[^:]*:\s*/, "").trim();
      date = normalizeDate(dateStr);
    } else if (lower.includes("korrigerande åtgärd") || lower.includes("korrigerande atgard")) {
      corrective_action = line.replace(/^[^:]*:\s*/, "").trim();
    } else if (lower.includes("lagrum")) {
      legal_reference = line.replace(/^[^:]*:\s*/, "").trim();
    } else if (lower.includes("nyckelord")) {
      keywords = line.replace(/^[^:]*:\s*/, "").trim();
    } else if (lower.includes("överklagan") || lower.includes("overklag")) {
      appeal = line.replace(/^[^:]*:\s*/, "").trim();
    } else if (lower.includes("laga kraft") || lower.includes("lagakraft")) {
      final_judgment = line.replace(/^[^:]*:\s*/, "").trim();
    }
  }

  // Extract link hash from href
  let link_hash: string | null = null;
  if (linkHref) {
    const hashMatch = linkHref.match(/\/link\/([^.]+)\.aspx/);
    if (hashMatch) link_hash = hashMatch[1] ?? null;
  }

  return {
    section,
    title,
    date,
    corrective_action,
    legal_reference,
    keywords,
    appeal,
    final_judgment,
    summary,
    link_hash,
  };
}

// ---------------------------------------------------------------------------
// Scraping: Sanktionsavgifter (fine amounts list)
// ---------------------------------------------------------------------------

async function scrapeSanktionsavgifter(): Promise<SanctionEntry[]> {
  log("Scraping sanktionsavgifter (fines list)...");
  const url = `${BASE_URL}/om-oss/beslut-publikationer-och-remisser/beslut-om-sanktionsavgift/`;
  const html = await fetchHtml(url);
  if (!html) {
    err("Could not fetch sanktionsavgifter page");
    return [];
  }

  const $ = cheerio.load(html);
  const entries: SanctionEntry[] = [];
  let currentYear = "";

  // The page is organized by year (h2 headings),
  // with entity: amount entries as paragraphs containing links.
  $("h2, h3, p").each((_i, el) => {
    const tagName = $(el).prop("tagName")?.toLowerCase();
    const text = $(el).text().trim();

    if (tagName === "h2" || tagName === "h3") {
      const yearMatch = text.match(/\b(20\d{2})\b/);
      if (yearMatch) {
        currentYear = yearMatch[1]!;
      }
      return;
    }

    if (tagName !== "p") return;

    // Look for entity: amount pattern
    // Examples: "Sportadmin: 6 000 000 kronor", "Google: 50 000 000 kronor"
    const entityMatch = text.match(/^(.+?):\s*(.+)/);
    if (!entityMatch) return;

    const entity = entityMatch[1]!.trim();
    const amountText = entityMatch[2]!.trim();
    const amount = parseFineAmount(amountText);

    if (!amount || !entity) return;

    // Check for PDF link
    let link_hash: string | null = null;
    const linkEl = $(el).find("a[href*='/link/'], a[href*='.aspx']");
    if (linkEl.length > 0) {
      const href = linkEl.attr("href") ?? "";
      const hashMatch = href.match(/\/link\/([^.]+)\.aspx/);
      if (hashMatch) link_hash = hashMatch[1] ?? null;
    }

    entries.push({
      entity_name: entity,
      fine_amount: amount,
      year: currentYear || "unknown",
      link_hash,
    });
  });

  log(`Collected ${entries.length} sanktionsavgifter entries`);
  return entries;
}

// ---------------------------------------------------------------------------
// Scraping: Guidance pages (vägledningar)
// ---------------------------------------------------------------------------

interface GuidanceLink {
  title: string;
  url: string;
  description: string | null;
  category: string;
}

async function scrapeGuidanceIndex(): Promise<GuidanceLink[]> {
  log("Scraping guidance index pages...");

  const guidanceIndexUrls = [
    { url: `${BASE_URL}/verksamhet/dataskydd/det-har-galler-enligt-gdpr/`, category: "GDPR" },
    { url: `${BASE_URL}/verksamhet/utbildning-och-stod/vagledning-for-verksamheter/`, category: "Vägledning" },
    { url: `${BASE_URL}/verksamhet/dataskydd/innovationsportalen/`, category: "Innovation" },
    { url: `${BASE_URL}/verksamhet/dataskydd/det-har-galler-enligt-gdpr/informationssakerhet/`, category: "Informationssäkerhet" },
    { url: `${BASE_URL}/verksamhet/dataskydd/det-har-galler-enligt-gdpr/personuppgiftsincidenter/`, category: "Incidenthantering" },
    { url: `${BASE_URL}/verksamhet/dataskydd/det-har-galler-enligt-gdpr/konsekvensbedomning/`, category: "Konsekvensbedömning" },
    { url: `${BASE_URL}/verksamhet/dataskydd/det-har-galler-enligt-gdpr/overforing-till-tredje-land/`, category: "Internationella överföringar" },
  ];

  const links: GuidanceLink[] = [];
  const seenUrls = new Set<string>();

  for (const index of guidanceIndexUrls) {
    const html = await fetchHtml(index.url);
    if (!html) continue;

    const $ = cheerio.load(html);

    // Collect links to subpages within the dataskydd/verksamhet domain
    $("a[href*='/verksamhet/'], a[href*='/om-oss/']").each((_i, el) => {
      const href = $(el).attr("href");
      if (!href) return;

      const fullUrl = resolveUrl(href);

      // Skip index pages, anchors, and already-seen URLs
      if (seenUrls.has(fullUrl)) return;
      if (href.includes("#")) return;
      if (href === index.url.replace(BASE_URL, "")) return;

      const title = $(el).text().trim();
      if (!title || title.length < 5 || title.length > 200) return;

      // Skip navigational links
      if (["Hem", "Meny", "Tillbaka", "Sök", "Kontakt", "Om IMY"].includes(title)) return;

      // Only include guidance-like pages
      const isGuidancePath = href.includes("/vagledning") ||
        href.includes("/det-har-galler") ||
        href.includes("/innovationsportalen") ||
        href.includes("/dataskydd-pa-") ||
        href.includes("/informationssakerhet") ||
        href.includes("/personuppgiftsincident") ||
        href.includes("/konsekvensbedomning") ||
        href.includes("/overforing-till") ||
        href.includes("/gdpr") ||
        href.includes("/eu-riktlinjer") ||
        href.includes("/rattsliga-stallningstaganden");

      if (!isGuidancePath) return;

      seenUrls.add(fullUrl);

      // Extract description from parent or sibling elements
      const parentLi = $(el).closest("li");
      const desc = parentLi.find("p, span.description").first().text().trim() || null;

      links.push({
        title,
        url: fullUrl,
        description: desc,
        category: index.category,
      });
    });
  }

  log(`Collected ${links.length} guidance page links`);
  return links;
}

async function scrapeGuidancePage(link: GuidanceLink): Promise<GuidelineRow | null> {
  const html = await fetchHtml(link.url);
  if (!html) return null;

  const $ = cheerio.load(html);

  const title = $("h1").first().text().trim() || link.title;

  // Extract main content
  const mainContent = $("main, [role='main'], .main-content, article").first();
  const contentRoot = mainContent.length > 0 ? mainContent : $("body");

  const paragraphs: string[] = [];
  contentRoot.find("p, li").each((_i, el) => {
    const text = $(el).text().trim();
    if (text && text.length > 15) {
      paragraphs.push(text);
    }
  });

  // Also capture heading structure for context
  const sections: string[] = [];
  contentRoot.find("h2, h3").each((_i, el) => {
    const text = $(el).text().trim();
    if (text && text.length > 3 && text.length < 200) {
      sections.push(text);
    }
  });

  const full_text = paragraphs.join("\n\n");
  if (!full_text || full_text.length < 100) {
    // Page may be primarily links or have insufficient content
    return null;
  }

  const summary = link.description || paragraphs[0] || null;

  // Generate a reference ID from the URL
  const pathParts = link.url.replace(BASE_URL, "").replace(/^\/|\/$/g, "").split("/");
  const lastPart = pathParts[pathParts.length - 1] ?? "unknown";
  const reference = `IMY-GUIDE-${lastPart}`.toUpperCase().slice(0, 80);

  // Detect date from page text
  let date: string | null = null;
  const bodyText = $("body").text();
  const datePatterns = [
    /(?:Senast uppdaterad|Publicerad|Uppdaterad)[:\s]*(\d{4}-\d{2}-\d{2})/i,
    /(?:Senast uppdaterad|Publicerad)[:\s]*(\d{1,2}\s+\w+\s+\d{4})/i,
  ];
  for (const pattern of datePatterns) {
    const match = bodyText.match(pattern);
    if (match) {
      date = normalizeDate(match[1]!);
      if (date) break;
    }
  }

  // Classify type
  let type = "guide";
  const urlLower = link.url.toLowerCase();
  if (urlLower.includes("eu-riktlinjer") || urlLower.includes("riktlinje")) type = "guideline";
  if (urlLower.includes("stallningstaganden")) type = "legal_position";
  if (urlLower.includes("vagledning")) type = "guidance";
  if (urlLower.includes("innovationsportalen")) type = "innovation_guidance";
  if (urlLower.includes("informationssakerhet")) type = "security_guidance";

  // Detect language
  const language = bodyText.includes("This page") || bodyText.includes("In English")
    ? "en"
    : "sv";

  // Topic classification from section headings and content
  const allText = [title, ...sections, ...paragraphs.slice(0, 5)].join(" ");
  const topics = classifyTopics(allText, [link.category], null);

  return {
    reference,
    title,
    date,
    type,
    summary,
    full_text,
    topics: topics.length > 0 ? JSON.stringify(topics) : null,
    language,
  };
}

// ---------------------------------------------------------------------------
// Merge: Combine data from multiple sources
// ---------------------------------------------------------------------------

/**
 * Merge sanktionsavgifter amounts into decisions. The fines list provides
 * authoritative fine amounts; individual decision pages may not always
 * parse them correctly.
 */
function mergeSanctionData(
  decisions: Map<string, DecisionRow>,
  sanctions: SanctionEntry[],
): void {
  for (const sanction of sanctions) {
    // Try to find a matching decision by entity name
    const entityLower = sanction.entity_name.toLowerCase();

    for (const [_ref, decision] of decisions) {
      if (!decision.entity_name) continue;
      const decEntityLower = decision.entity_name.toLowerCase();

      if (
        decEntityLower.includes(entityLower) ||
        entityLower.includes(decEntityLower)
      ) {
        // Update fine amount if the sanction list has a value
        if (sanction.fine_amount > 0) {
          decision.fine_amount = sanction.fine_amount;
          decision.type = "sanction";
        }
      }
    }
  }
}

/**
 * Merge praxisbeslut metadata into decisions. Praxisbeslut provides
 * structured legal references, keywords, and corrective actions.
 */
function mergePraxisData(
  decisions: Map<string, DecisionRow>,
  praxis: PraxisEntry[],
): void {
  for (const p of praxis) {
    // Try to match by title substring
    const pTitleLower = p.title.toLowerCase();

    let matched = false;
    for (const [_ref, decision] of decisions) {
      const decTitleLower = decision.title.toLowerCase();

      if (
        decTitleLower.includes(pTitleLower) ||
        pTitleLower.includes(decTitleLower) ||
        (decision.entity_name && pTitleLower.includes(decision.entity_name.toLowerCase()))
      ) {
        // Enrich with praxis metadata
        if (p.legal_reference) {
          const articles = extractGdprArticles(p.legal_reference);
          if (articles.length > 0) {
            const existing = decision.gdpr_articles
              ? (JSON.parse(decision.gdpr_articles) as string[])
              : [];
            const merged = [...new Set([...existing, ...articles])].sort(
              (a, b) => parseFloat(a) - parseFloat(b),
            );
            decision.gdpr_articles = JSON.stringify(merged);
          }
        }

        if (p.keywords) {
          const topics = classifyTopics(
            decision.full_text,
            [],
            p.keywords,
          );
          if (topics.length > 0) {
            const existing = decision.topics
              ? (JSON.parse(decision.topics) as string[])
              : [];
            const merged = [...new Set([...existing, ...topics])];
            decision.topics = JSON.stringify(merged);
          }
        }

        if (p.corrective_action) {
          const fine = parseFineAmount(p.corrective_action);
          if (fine && fine > 0 && (!decision.fine_amount || fine > decision.fine_amount)) {
            decision.fine_amount = fine;
          }
          decision.type = classifyDecisionType(
            p.corrective_action,
            decision.fine_amount,
            decision.full_text,
          );
        }

        if (p.date && !decision.date) {
          decision.date = normalizeDate(p.date);
        }

        // Append praxis summary to the full text if it adds information
        if (p.summary && p.summary.length > 50 && !decision.full_text.includes(p.summary.slice(0, 40))) {
          decision.full_text += `\n\n---\n\nPraxisbeslut: ${p.summary}`;
        }

        matched = true;
        break;
      }
    }

    // If no match found, create a standalone decision from the praxis entry
    if (!matched && p.summary && p.summary.length > 50) {
      const reference = p.link_hash
        ? `IMY-PRAXIS-${p.link_hash.slice(0, 12)}`
        : `IMY-PRAXIS-${p.title.slice(0, 30).replace(/\s+/g, "-").toLowerCase()}`;

      const gdpr_articles = p.legal_reference
        ? extractGdprArticles(p.legal_reference)
        : [];

      const topics = classifyTopics(
        p.summary + " " + (p.keywords ?? ""),
        [p.section],
        p.keywords,
      );

      const fine_amount = p.corrective_action
        ? parseFineAmount(p.corrective_action)
        : null;

      decisions.set(reference, {
        reference,
        title: p.title,
        date: normalizeDate(p.date),
        type: classifyDecisionType(p.corrective_action, fine_amount, p.summary),
        entity_name: null,
        fine_amount,
        summary: p.summary,
        full_text: p.summary,
        topics: topics.length > 0 ? JSON.stringify(topics) : null,
        gdpr_articles: gdpr_articles.length > 0 ? JSON.stringify(gdpr_articles) : null,
        status: "final",
      });
    }
  }
}

// ---------------------------------------------------------------------------
// Topics seed data
// ---------------------------------------------------------------------------

const TOPICS: TopicRow[] = [
  {
    id: "camera_surveillance",
    name_local: "Kamerabevakning",
    name_en: "Camera surveillance",
    description: "Tillsyn och vägledning om kamerabevakning, ansiktsigenkänning och videoövervakning enligt kamerabevakningslagen och GDPR.",
  },
  {
    id: "dpia",
    name_local: "Konsekvensbedömning (DPIA)",
    name_en: "Data Protection Impact Assessment",
    description: "Krav på och vägledning om konsekvensbedömningar avseende dataskydd enligt artikel 35 GDPR.",
  },
  {
    id: "consent",
    name_local: "Samtycke",
    name_en: "Consent",
    description: "Krav på giltigt samtycke för behandling av personuppgifter enligt artikel 6.1 a och 7 GDPR.",
  },
  {
    id: "marketing",
    name_local: "Marknadsföring",
    name_en: "Marketing and profiling",
    description: "Regler om direktmarknadsföring, profilering och digital annonsering enligt GDPR.",
  },
  {
    id: "information_security",
    name_local: "Informationssäkerhet",
    name_en: "Information security",
    description: "Tekniska och organisatoriska säkerhetsåtgärder enligt artikel 32 GDPR.",
  },
  {
    id: "dpo",
    name_local: "Dataskyddsombud",
    name_en: "Data Protection Officer",
    description: "Krav på och rollen för dataskyddsombud enligt artiklarna 37-39 GDPR.",
  },
  {
    id: "data_subject_rights",
    name_local: "Registrerades rättigheter",
    name_en: "Data subject rights",
    description: "Rätten till tillgång, rättelse, radering, portabilitet och invändning enligt artiklarna 12-23 GDPR.",
  },
  {
    id: "legal_basis",
    name_local: "Rättslig grund",
    name_en: "Legal basis",
    description: "Val av rättslig grund för behandling av personuppgifter enligt artikel 6 GDPR.",
  },
  {
    id: "sensitive_data",
    name_local: "Känsliga personuppgifter",
    name_en: "Sensitive personal data",
    description: "Behandling av särskilda kategorier av uppgifter (hälsa, biometri, politiska åsikter) enligt artikel 9 GDPR.",
  },
  {
    id: "health_data",
    name_local: "Hälso- och sjukvårdsdata",
    name_en: "Health data",
    description: "Behandling av personuppgifter inom hälso- och sjukvården, inklusive patientdatalagen.",
  },
  {
    id: "breach_notification",
    name_local: "Personuppgiftsincidenter",
    name_en: "Data breach notification",
    description: "Anmälan och hantering av personuppgiftsincidenter enligt artiklarna 33-34 GDPR.",
  },
  {
    id: "international_transfers",
    name_local: "Internationella överföringar",
    name_en: "International data transfers",
    description: "Överföring av personuppgifter till tredje land enligt artiklarna 44-49 GDPR, inklusive Schrems II.",
  },
  {
    id: "children",
    name_local: "Barn och skola",
    name_en: "Children and schools",
    description: "Behandling av barns personuppgifter och dataskydd i skol- och förskoleverksamhet.",
  },
  {
    id: "associations",
    name_local: "Föreningar",
    name_en: "Associations",
    description: "Dataskydd för ideella föreningar och frivilligorganisationer.",
  },
  {
    id: "employment",
    name_local: "Arbetsliv",
    name_en: "Employment",
    description: "Dataskydd på arbetsplatsen: anställdas integritet, övervakning, GPS-spårning och biometri.",
  },
  {
    id: "ai",
    name_local: "Artificiell intelligens",
    name_en: "Artificial intelligence",
    description: "Vägledning om dataskydd vid utveckling och användning av AI och maskininlärning.",
  },
  {
    id: "research",
    name_local: "Forskning",
    name_en: "Research",
    description: "Behandling av personuppgifter för vetenskapliga forskningsändamål.",
  },
];

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDatabase(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    log(`Created data directory: ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  log(`Database initialized at ${DB_PATH}`);
  return db;
}

function insertTopics(db: Database.Database): void {
  const stmt = db.prepare(
    "INSERT OR IGNORE INTO topics (id, name_local, name_en, description) VALUES (?, ?, ?, ?)",
  );

  const insertAll = db.transaction(() => {
    for (const t of TOPICS) {
      stmt.run(t.id, t.name_local, t.name_en, t.description);
    }
  });

  insertAll();
  log(`Inserted/updated ${TOPICS.length} topics`);
}

function insertDecisions(
  db: Database.Database,
  decisions: Map<string, DecisionRow>,
  existingRefs: Set<string>,
): number {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO decisions
      (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let count = 0;
  const insertAll = db.transaction(() => {
    for (const [ref, d] of decisions) {
      if (FLAG_RESUME && existingRefs.has(ref)) {
        continue;
      }
      stmt.run(
        d.reference,
        d.title,
        d.date,
        d.type,
        d.entity_name,
        d.fine_amount,
        d.summary,
        d.full_text,
        d.topics,
        d.gdpr_articles,
        d.status,
      );
      count++;
    }
  });

  insertAll();
  return count;
}

function insertGuidelines(
  db: Database.Database,
  guidelines: GuidelineRow[],
  existingRefs: Set<string>,
): number {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO guidelines
      (reference, title, date, type, summary, full_text, topics, language)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let count = 0;
  const insertAll = db.transaction(() => {
    for (const g of guidelines) {
      if (FLAG_RESUME && g.reference && existingRefs.has(g.reference)) {
        continue;
      }
      stmt.run(
        g.reference,
        g.title,
        g.date,
        g.type,
        g.summary,
        g.full_text,
        g.topics,
        g.language,
      );
      count++;
    }
  });

  insertAll();
  return count;
}

function getExistingRefs(db: Database.Database): { decisionRefs: Set<string>; guidelineRefs: Set<string> } {
  const decisionRefs = new Set<string>();
  const guidelineRefs = new Set<string>();

  const dRows = db.prepare("SELECT reference FROM decisions").all() as { reference: string }[];
  for (const r of dRows) decisionRefs.add(r.reference);

  const gRows = db.prepare("SELECT reference FROM guidelines WHERE reference IS NOT NULL").all() as { reference: string }[];
  for (const r of gRows) guidelineRefs.add(r.reference);

  return { decisionRefs, guidelineRefs };
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("=== IMY Ingestion Crawler ===");
  log(`Flags: resume=${FLAG_RESUME}, dry-run=${FLAG_DRY_RUN}, force=${FLAG_FORCE}`);
  log(`Database: ${DB_PATH}`);
  log("");

  // Phase 0: Initialize database (unless dry-run)
  let db: Database.Database | null = null;
  let existingDecisionRefs = new Set<string>();
  let existingGuidelineRefs = new Set<string>();

  if (!FLAG_DRY_RUN) {
    db = initDatabase();
    insertTopics(db);

    if (FLAG_RESUME) {
      const existing = getExistingRefs(db);
      existingDecisionRefs = existing.decisionRefs;
      existingGuidelineRefs = existing.guidelineRefs;
      log(`Resume mode: ${existingDecisionRefs.size} decisions and ${existingGuidelineRefs.size} guidelines already in DB`);
    }
  }

  const decisions = new Map<string, DecisionRow>();

  // --- Phase 1: Scrape tillsyner listing pages ---------------------------

  log("\n--- Phase 1: Tillsyner listing ---");
  const tillsynerItems = await scrapeTillsynerListing();

  // --- Phase 2: Scrape individual tillsyn detail pages --------------------

  log("\n--- Phase 2: Individual tillsyn pages ---");
  let detailCount = 0;
  let skipCount = 0;

  for (const item of tillsynerItems) {
    // Generate a preliminary reference for resume check
    const slugMatch = item.url.match(/\/tillsyner\/([^/]+)\/?$/);
    const slug = slugMatch ? slugMatch[1]! : item.title.toLowerCase().replace(/\s+/g, "-");
    const prelimRef = `IMY-TILLSYN-${slug}`;

    if (FLAG_RESUME && existingDecisionRefs.has(prelimRef)) {
      skipCount++;
      continue;
    }

    log(`  Scraping: ${item.title} (${item.url})`);
    const detail = await scrapeTillsynDetail(item.url, item);

    if (!detail) {
      warn(`  Could not extract detail from ${item.url}`);
      continue;
    }

    const ref = detail.reference ?? prelimRef;

    // Classify topics from all available text
    const topics = classifyTopics(
      detail.full_text + " " + detail.summary,
      detail.categories,
      null,
    );

    const decisionType = classifyDecisionType(null, detail.fine_amount, detail.full_text);

    decisions.set(ref, {
      reference: ref,
      title: detail.title,
      date: normalizeDate(detail.date),
      type: decisionType,
      entity_name: detail.entity_name,
      fine_amount: detail.fine_amount,
      summary: detail.summary,
      full_text: detail.full_text,
      topics: topics.length > 0 ? JSON.stringify(topics) : null,
      gdpr_articles: detail.gdpr_articles.length > 0 ? JSON.stringify(detail.gdpr_articles) : null,
      status: detail.status,
    });

    detailCount++;
  }

  log(`Scraped ${detailCount} decision detail pages (${skipCount} skipped in resume mode)`);

  // --- Phase 3: Scrape praxisbeslut (structured metadata) -----------------

  log("\n--- Phase 3: Praxisbeslut ---");
  const praxisEntries = await scrapePraxisbeslut();
  mergePraxisData(decisions, praxisEntries);
  log(`Merged ${praxisEntries.length} praxisbeslut entries`);

  // --- Phase 4: Scrape sanktionsavgifter (fine amounts) -------------------

  log("\n--- Phase 4: Sanktionsavgifter ---");
  const sanctions = await scrapeSanktionsavgifter();
  mergeSanctionData(decisions, sanctions);
  log(`Merged ${sanctions.length} sanktionsavgifter entries`);

  // --- Phase 5: Scrape guidance pages ------------------------------------

  log("\n--- Phase 5: Guidance pages ---");
  const guidanceLinks = await scrapeGuidanceIndex();
  const guidelines: GuidelineRow[] = [];
  let guideSkipCount = 0;

  for (const link of guidanceLinks) {
    // Generate reference for resume check
    const pathParts = link.url.replace(BASE_URL, "").replace(/^\/|\/$/g, "").split("/");
    const lastPart = pathParts[pathParts.length - 1] ?? "unknown";
    const guideRef = `IMY-GUIDE-${lastPart}`.toUpperCase().slice(0, 80);

    if (FLAG_RESUME && existingGuidelineRefs.has(guideRef)) {
      guideSkipCount++;
      continue;
    }

    log(`  Scraping guidance: ${link.title}`);
    const guideline = await scrapeGuidancePage(link);

    if (guideline) {
      guidelines.push(guideline);
    }
  }

  log(`Scraped ${guidelines.length} guidance pages (${guideSkipCount} skipped in resume mode)`);

  // --- Phase 6: Write to database ----------------------------------------

  log("\n--- Phase 6: Database write ---");
  log(`Total decisions: ${decisions.size}`);
  log(`Total guidelines: ${guidelines.length}`);

  if (FLAG_DRY_RUN) {
    log("DRY RUN — no data written to database");
    log("\nSample decisions:");
    let i = 0;
    for (const [ref, d] of decisions) {
      if (i >= 5) break;
      log(`  ${ref}: ${d.title} (${d.date ?? "no date"}) type=${d.type} fine=${d.fine_amount ?? "none"}`);
      i++;
    }
    log("\nSample guidelines:");
    for (const g of guidelines.slice(0, 5)) {
      log(`  ${g.reference}: ${g.title} (${g.type})`);
    }
    return;
  }

  if (!db) {
    err("Database not initialized");
    process.exit(1);
  }

  const decisionInsertCount = insertDecisions(db, decisions, existingDecisionRefs);
  const guidelineInsertCount = insertGuidelines(db, guidelines, existingGuidelineRefs);

  // --- Phase 7: Summary ---------------------------------------------------

  log("\n--- Summary ---");
  const decisionTotal = (
    db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
  ).cnt;
  const guidelineTotal = (
    db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
  ).cnt;
  const topicTotal = (
    db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
  ).cnt;
  const ftsDecisions = (
    db.prepare("SELECT count(*) as cnt FROM decisions_fts").get() as { cnt: number }
  ).cnt;
  const ftsGuidelines = (
    db.prepare("SELECT count(*) as cnt FROM guidelines_fts").get() as { cnt: number }
  ).cnt;

  log(`  Topics:           ${topicTotal}`);
  log(`  Decisions:        ${decisionTotal} (${decisionInsertCount} new, FTS: ${ftsDecisions})`);
  log(`  Guidelines:       ${guidelineTotal} (${guidelineInsertCount} new, FTS: ${ftsGuidelines})`);

  // Save resume state
  const state: IngestState = {
    ingested_decision_refs: [...decisions.keys()],
    ingested_guideline_refs: guidelines.map((g) => g.reference).filter(Boolean) as string[],
    last_run: new Date().toISOString(),
  };
  saveState(state);
  log(`\nResume state saved to ${STATE_PATH}`);

  db.close();
  log(`\nDone. Database ready at ${DB_PATH}`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((e: unknown) => {
  err(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  if (e instanceof Error && e.stack) {
    console.error(e.stack);
  }
  process.exit(1);
});
