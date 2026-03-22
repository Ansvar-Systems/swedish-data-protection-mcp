/**
 * Seed the IMY database with sample decisions and guidelines for testing.
 *
 * Includes real IMY decisions (Google, Spotify, Klarna) and representative
 * guidance documents so MCP tools can be tested without running a full
 * data ingestion pipeline.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["IMY_DB_PATH"] ?? "data/imy.db";
const force = process.argv.includes("--force");

// --- Bootstrap database ------------------------------------------------------

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Topics ------------------------------------------------------------------

interface TopicRow {
  id: string;
  name_local: string;
  name_en: string;
  description: string;
}

const topics: TopicRow[] = [
  {
    id: "samtycke",
    name_local: "Samtycke",
    name_en: "Consent",
    description: "Insamling, giltighet och återkallelse av samtycke till behandling av personuppgifter (art. 7 GDPR).",
  },
  {
    id: "cookies",
    name_local: "Cookies och spårare",
    name_en: "Cookies and trackers",
    description: "Placering och läsning av cookies och spårare på användarens enhet (art. 82 lag om elektronisk kommunikation).",
  },
  {
    id: "tredjelandsoverfoering",
    name_local: "Tredjelandsöverföring",
    name_en: "International transfers",
    description: "Överföring av personuppgifter till tredjeland eller internationella organisationer (art. 44–49 GDPR).",
  },
  {
    id: "konsekvensbedömning",
    name_local: "Konsekvensbedömning (DPIA)",
    name_en: "Data Protection Impact Assessment (DPIA)",
    description: "Bedömning av risker för registrerades rättigheter och friheter vid högriskbehandling (art. 35 GDPR).",
  },
  {
    id: "personuppgiftsincident",
    name_local: "Personuppgiftsincident",
    name_en: "Data breach notification",
    description: "Anmälan av personuppgiftsincidenter till IMY och berörda registrerade (art. 33–34 GDPR).",
  },
  {
    id: "inbyggt_dataskydd",
    name_local: "Inbyggt dataskydd",
    name_en: "Privacy by design",
    description: "Integrering av dataskydd redan vid utformning och som standard (art. 25 GDPR).",
  },
  {
    id: "arbetsgivare",
    name_local: "Arbetsgivare och anställda",
    name_en: "Employee monitoring",
    description: "Behandling av personuppgifter i arbetsrelationer och övervakning av anställda.",
  },
  {
    id: "hälsodata",
    name_local: "Hälsodata",
    name_en: "Health data",
    description: "Behandling av hälsodata — känsliga uppgifter med förstärkta skyddsgarantier (art. 9 GDPR).",
  },
  {
    id: "profilering",
    name_local: "Profilering",
    name_en: "Profiling",
    description: "Automatiserad behandling för att utvärdera personliga aspekter, inklusive kreditbedömning (art. 22 GDPR).",
  },
  {
    id: "kamerabevakning",
    name_local: "Kamerabevakning",
    name_en: "Camera surveillance",
    description: "Kamerabevakning på arbetsplatser, offentliga platser och bostadsområden (kamerabevakningslagen).",
  },
];

const insertTopic = db.prepare(
  "INSERT OR IGNORE INTO topics (id, name_local, name_en, description) VALUES (?, ?, ?, ?)",
);

for (const t of topics) {
  insertTopic.run(t.id, t.name_local, t.name_en, t.description);
}

console.log(`Inserted ${topics.length} topics`);

// --- Decisions ---------------------------------------------------------------

interface DecisionRow {
  reference: string;
  title: string;
  date: string;
  type: string;
  entity_name: string;
  fine_amount: number | null;
  summary: string;
  full_text: string;
  topics: string;
  gdpr_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  // IMY-2022-6524 — Google (SEK 75M)
  {
    reference: "IMY-2022-6524",
    title: "Tillsynsbeslut mot Google LLC — SEK 75 000 000",
    date: "2022-06-02",
    type: "sanction",
    entity_name: "Google LLC",
    fine_amount: 75_000_000,
    summary:
      "IMY ålade Google en sanktionsavgift på 75 miljoner kronor för att Google Analytics överförde personuppgifter om svenska webbplatsbesökare till USA utan tillräckliga skyddsgarantier. Överföringarna stred mot GDPR:s regler om tredjelandsöverföringar efter Schrems II-domen.",
    full_text:
      "Integritetsskyddsmyndigheten (IMY) har den 2 juni 2022 beslutat att påföra Google LLC en administrativ sanktionsavgift på 75 000 000 kr. Bakgrunden är att fyra svenska företag och myndigheter använde Google Analytics för webbanalys, vilket resulterade i att personuppgifter (IP-adresser, unika identifierare, webbläsarinformation) om besökare på deras webbplatser överfördes till USA. EU-domstolens dom i mål C-311/18 (Schrems II) ogiltigförklarade Privacy Shield-ramverket och fastslog att standardavtalsklausuler inte automatiskt säkerställer en adekvat skyddsnivå. IMY konstaterade att de tekniska kompletterande åtgärder som Google vidtagit — pseudonymisering och kryptering — inte var tillräckliga eftersom Google som leverantör av molntjänster kan tvingas lämna ut data till amerikanska myndigheter enligt Foreign Intelligence Surveillance Act (FISA). De aktuella webbplatsoperatörerna ålades att upphöra med användningen av Google Analytics om de inte kan säkerställa att personuppgifter inte överförs till USA i strid med GDPR artiklarna 44–46. Google LLC, som personuppgiftsbiträde, ålades sanktionsavgiften för sin del i att möjliggöra dessa regelstridiga överföringar.",
    topics: JSON.stringify(["tredjelandsoverfoering", "cookies"]),
    gdpr_articles: JSON.stringify(["44", "46", "28"]),
    status: "final",
  },
  // IMY-2021-5679 — Spotify employee surveillance
  {
    reference: "IMY-2021-5679",
    title: "Tillsynsbeslut mot Spotify AB — anställdas personuppgifter",
    date: "2021-06-16",
    type: "sanction",
    entity_name: "Spotify AB",
    fine_amount: 58_000_000,
    summary:
      "IMY ålade Spotify en sanktionsavgift på 58 miljoner kronor för bristande transparens i hur företaget behandlar anställdas och tidigare anställdas personuppgifter. Spotify hade inte tillräckligt tydligt informerat om ändamålen med behandlingen och hur länge uppgifterna sparades.",
    full_text:
      "Integritetsskyddsmyndigheten (IMY) genomförde tillsyn av Spotify AB:s behandling av personuppgifter om anställda. Tillsynen visade att Spotify brast i sin informationsskyldighet gentemot anställda. Spotify informerade inte tillräckligt tydligt om: (1) vilka personuppgifter som behandlades och för vilka ändamål; (2) hur länge uppgifterna sparades; (3) till vilka mottagare uppgifterna lämnas ut, inklusive utlämnande till amerikanska myndigheter. Integritetsskyddsmyndigheten konstaterade att Spotifys integritetspolicy för anställda inte uppfyllde kraven i artikel 13 och 14 GDPR om transparent information. Spotify använder globala HR-system som innebär att anställdas uppgifter hanteras i USA, vilket ställer krav på adekvata skyddsgarantier. IMY konstaterade vidare att Spotify inte hade någon tydlig rättslig grund för vissa behandlingar av känsliga uppgifter om anställda, till exempel sjukfrånvaro och rehabiliteringsåtgärder. Sanktionsavgiften fastställdes till 58 000 000 kr med beaktande av överträdelsernas allvar, antal berörda personer och Spotifys ekonomiska ställning.",
    topics: JSON.stringify(["arbetsgivare", "hälsodata", "tredjelandsoverfoering"]),
    gdpr_articles: JSON.stringify(["5", "6", "9", "13", "14"]),
    status: "final",
  },
  // DI-2020-11332 — Klarna credit checks
  {
    reference: "DI-2020-11332",
    title: "Tillsynsbeslut mot Klarna Bank AB — kreditupplysningsprofilering",
    date: "2021-03-22",
    type: "tillsynsbeslut",
    entity_name: "Klarna Bank AB",
    fine_amount: 7_500_000,
    summary:
      "IMY utfärdade ett tillsynsbeslut mot Klarna för bristande information till konsumenter om hur kreditbedömningsprofileringen fungerar och vilka automatiserade beslut som fattas vid köp med delbetalning.",
    full_text:
      "Integritetsskyddsmyndigheten genomförde tillsyn av Klarna Bank AB:s behandling av personuppgifter vid kreditbedömning. Tillsynen inriktades på Klarnas profilering av konsumenter vid köp med delbetalning och fakturaköp. IMY konstaterade följande brister: (1) Otillräcklig information om profilering — Klarnas integritetspolicy innehöll inte tillräcklig information om logiken bakom de automatiserade besluten, till exempel vilka faktorer som avgör om kredit beviljas, och konsekvenserna av den automatiserade behandlingen; (2) Otydlig information om rätten att invända — konsumenter informerades inte tillräckligt tydligt om sin rätt att invända mot profilering och att begära manuell prövning av automatiserade beslut; (3) Otillräcklig dokumentation — Klarna saknade tillräcklig dokumentation av algoritmerna och modellerna som används vid kreditbedömning, vilket försvårade IMY:s tillsynsarbete. IMY förelade Klarna att åtgärda bristerna och fastställde en sanktionsavgift på 7 500 000 kr.",
    topics: JSON.stringify(["profilering", "samtycke"]),
    gdpr_articles: JSON.stringify(["13", "14", "22", "35"]),
    status: "final",
  },
  // IMY-2021-3 — Capio healthcare breach
  {
    reference: "IMY-2021-3",
    title: "Tillsynsbeslut mot Capio AB — personuppgiftsincident hälsodata",
    date: "2021-09-14",
    type: "tillsynsbeslut",
    entity_name: "Capio AB",
    fine_amount: 3_000_000,
    summary:
      "IMY påförde Capio en sanktionsavgift på 3 miljoner kronor för en personuppgiftsincident där patientjournaler och hälsodata om tusentals patienter blev åtkomliga för obehöriga på grund av en felkonfigurerad databas.",
    full_text:
      "Integritetsskyddsmyndigheten har beslutat att påföra Capio AB en sanktionsavgift på 3 000 000 kr. En personuppgiftsincident uppstod när en databas innehållande patientjournaler, diagnoser, ordinationer och övrig hälsoinformation om uppskattningsvis 15 000 patienter under en period var åtkomlig utan autentisering via internet. Capio underrättade IMY om incidenten men anmälan inkom 18 dagar efter att incidenten upptäckts, vilket överstiger den lagstadgade fristen på 72 timmar i artikel 33 GDPR. IMY konstaterade vidare att Capio brustit i sina säkerhetsrutiner avseende: (1) åtkomstkontroll och behörighetshantering för databaser innehållande känsliga hälsouppgifter; (2) penetrationstestning och löpande säkerhetsöversyn; (3) rutiner för incidenthantering och anmälan. Hälsodata utgör känsliga personuppgifter enligt artikel 9 GDPR och kräver förstärkta tekniska och organisatoriska skyddsåtgärder. Att känsliga hälsouppgifter om tusentals patienter exponerats bedömdes som en allvarlig överträdelse.",
    topics: JSON.stringify(["hälsodata", "personuppgiftsincident", "inbyggt_dataskydd"]),
    gdpr_articles: JSON.stringify(["9", "32", "33", "34"]),
    status: "final",
  },
  // IMY-2022-2 — Municipality school monitoring
  {
    reference: "IMY-2022-2",
    title: "Tillsynsbeslut — kamerabevakning i skola utan stöd i lag",
    date: "2022-04-05",
    type: "tillsynsbeslut",
    entity_name: "Göteborgs stad",
    fine_amount: 1_000_000,
    summary:
      "IMY utfärdade tillsynsbeslut mot Göteborgs stad för att en skola bedrivit kamerabevakning av elever i matsalen och korridorer utan tillräckligt stöd i kamerabevakningslagen och utan att genomföra en konsekvensbedömning.",
    full_text:
      "Integritetsskyddsmyndigheten genomförde tillsyn av kamerabevakning vid en grundskola i Göteborgs stad. Tillsynen initierades efter klagomål från elever och föräldrar. IMY konstaterade följande: (1) Bristande rättslig grund — kamerabevakningslagen (2018:1200) kräver att det finns ett berättigat syfte som väger tyngre än den enskildes intresse av integritet; skolans syfte att förebygga skadegörelse och trygghetsskapande åtgärder bedömdes inte uppfylla lagens krav utan en mer påtaglig och konkret hotbild; (2) Avsaknad av konsekvensbedömning — skolan hade inte genomfört en konsekvensbedömning avseende dataskydd (DPIA) trots att kamerabevakning av barn i en skolmiljö utgör ett högriskbehandling; (3) Bristande information — elever och föräldrar informerades inte tillräckligt om kamerabevakningens ändamål, varaktighet och vilka som hade tillgång till bildmaterialet. IMY beslutade om förbud mot kamerabevakningens fortsatta bedrivande och påförde en sanktionsavgift på 1 000 000 kr.",
    topics: JSON.stringify(["kamerabevakning", "konsekvensbedömning"]),
    gdpr_articles: JSON.stringify(["6", "13", "35"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(
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
  }
});

insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Guidelines --------------------------------------------------------------

interface GuidelineRow {
  reference: string | null;
  title: string;
  date: string;
  type: string;
  summary: string;
  full_text: string;
  topics: string;
  language: string;
}

const guidelines: GuidelineRow[] = [
  {
    reference: "IMY-VÄGLEDNING-KAMERA-2022",
    title: "Vägledning om kamerabevakning",
    date: "2022-01-01",
    type: "vagledning",
    summary:
      "IMY:s vägledning om kamerabevakning i arbetslivet, offentliga platser och bostadsområden. Förklarar när tillstånd krävs, vad som gäller för lagring av bildmaterial och hur registrerade ska informeras.",
    full_text:
      "Kamerabevakning av platser dit allmänheten har tillträde regleras i kamerabevakningslagen (2018:1200). IMY har tillsyn över att lagen följs. Vägledningen behandlar: (1) När tillstånd krävs — kamerabevakning av platser dit allmänheten har tillträde kräver tillstånd från IMY, med undantag för vissa platser som banker, butiker och myndighetsbyggnader som kan bevaka utan tillstånd om vissa villkor uppfylls; (2) Berättigat syfte och proportionalitet — syftet med bevakningen ska vara berättigat (t.ex. brottsförebyggande, säkerhet) och bevakningen ska vara proportionerlig i förhållande till integritetsintrånget; kamerabevakning i känsliga miljöer som skolor, vårdinrättningar och arbetsplatser kräver särskilt starka skäl; (3) Information till registrerade — synlig skyltning om kamerabevakning är obligatorisk; informationen ska ange vem som är personuppgiftsansvarig och kontaktuppgifter till dataskyddsombud; (4) Lagring av material — bildmaterial ska lagras under kortast möjliga tid, normalt 72 timmar; längre lagringstider kräver särskild motivering; (5) Tillgång till material — begränsad krets av personer ska ha tillgång till bildmaterialet; rutiner för hantering av förfrågningar från brottsbekämpande myndigheter ska finnas; (6) Konsekvensbedömning (DPIA) — kamerabevakning som kan medföra hög risk för integritetsintrång, till exempel bevakning av stora folksamlingar eller på känsliga platser, kräver en konsekvensbedömning.",
    topics: JSON.stringify(["kamerabevakning", "konsekvensbedömning"]),
    language: "sv",
  },
  {
    reference: "IMY-VÄGLEDNING-COOKIES-2022",
    title: "Vägledning om cookies och andra spårare",
    date: "2022-06-01",
    type: "vagledning",
    summary:
      "IMY:s vägledning om krav på samtycke för cookies och andra spårningstekniker. Förklarar vilka cookies som kräver samtycke, hur samtycke ska inhämtas och hur cookie-banners ska utformas.",
    full_text:
      "Placering av cookies och liknande spårare på en användares enhet regleras av lagen om elektronisk kommunikation (2022:482). IMY:s vägledning klargör: (1) Samtycke krävs — cookies som inte är strikt nödvändiga för tjänstens funktion kräver informerat och frivilligt samtycke; detta gäller analytiska cookies, marknadsföringscookies och tredjepartscookies; (2) Undantag för nödvändiga cookies — sessionscookies, inloggningscookies och varukorgar kräver inte samtycke; (3) Krav på cookie-banner — bannern ska innehålla tydliga val att acceptera och avböja alla cookies; avvisningsalternativet ska vara lika lättillgängligt som acceptansalternativet; (4) Förhandsmarkerade val är inte tillåtna — samtycke kräver en aktiv handling; (5) Granulär kontroll — användare ska kunna välja vilka kategorier av cookies de accepterar; (6) Dokumentation av samtycke — organisationen ska kunna visa att giltigt samtycke lämnats; (7) Tredjepartstjänster — användning av externa tjänster som Google Analytics, Facebook Pixel och liknande innebär dataöverföring som måste ha rättslig grund och, vid överföring utanför EU/EES, adekvata skyddsgarantier.",
    topics: JSON.stringify(["cookies", "samtycke"]),
    language: "sv",
  },
  {
    reference: "IMY-VÄGLEDNING-DPIA-2021",
    title: "Vägledning om konsekvensbedömning avseende dataskydd (DPIA)",
    date: "2021-10-01",
    type: "vagledning",
    summary:
      "IMY:s vägledning om när en DPIA är obligatorisk, hur den ska genomföras och dokumenteras. Innehåller IMY:s förteckning över behandlingar som alltid kräver DPIA.",
    full_text:
      "Artikel 35 i GDPR kräver att personuppgiftsansvariga genomför en konsekvensbedömning avseende dataskydd (DPIA) när en behandling sannolikt innebär hög risk för fysiska personers rättigheter och friheter. IMY:s förteckning över behandlingar som alltid kräver DPIA inkluderar: systematisk och omfattande profilering av individer; behandling i stor skala av känsliga uppgifter; systematisk övervakning av offentliga platser; behandling av uppgifter om barn i stor skala. Genomförande av DPIA: (1) Beskrivning av behandlingen — ändamål, rättslig grund, kategorier av uppgifter, mottagare, internationella överföringar, lagringstider; (2) Nödvändighets- och proportionalitetsbedömning — är behandlingen nödvändig? Kan ändamålen uppnås med mindre integritetskänsliga metoder? (3) Riskbedömning — identifiering av risker för registrerades rättigheter (obehörig åtkomst, felaktig behandling, förlust av uppgifter); bedömning av sannolikhet och allvarlighetsgrad; (4) Åtgärder — tekniska och organisatoriska åtgärder för att minimera riskerna. DPIA ska dokumenteras och uppdateras vid väsentliga förändringar. IMY ska konsulteras om kvarstående höga risker inte kan elimineras.",
    topics: JSON.stringify(["konsekvensbedömning", "inbyggt_dataskydd"]),
    language: "sv",
  },
];

const insertGuideline = db.prepare(`
  INSERT INTO guidelines (reference, title, date, type, summary, full_text, topics, language)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertGuidelinesAll = db.transaction(() => {
  for (const g of guidelines) {
    insertGuideline.run(
      g.reference,
      g.title,
      g.date,
      g.type,
      g.summary,
      g.full_text,
      g.topics,
      g.language,
    );
  }
});

insertGuidelinesAll();
console.log(`Inserted ${guidelines.length} guidelines`);

// --- Summary -----------------------------------------------------------------

const decisionCount = (
  db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
).cnt;
const guidelineCount = (
  db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
).cnt;
const topicCount = (
  db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
).cnt;
const decisionFtsCount = (
  db.prepare("SELECT count(*) as cnt FROM decisions_fts").get() as { cnt: number }
).cnt;
const guidelineFtsCount = (
  db.prepare("SELECT count(*) as cnt FROM guidelines_fts").get() as { cnt: number }
).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Topics:         ${topicCount}`);
console.log(`  Decisions:      ${decisionCount} (FTS entries: ${decisionFtsCount})`);
console.log(`  Guidelines:     ${guidelineCount} (FTS entries: ${guidelineFtsCount})`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
