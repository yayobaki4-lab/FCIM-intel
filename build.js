/* FCIM Daily Intelligence — daily builder v3

- Runs in GitHub Actions on a schedule.
- Pipeline:
- 1. Apify pulls candidate profiles (LinkedIn search, Short mode for cost).
- 1. Quality gate scores each profile, drops sub-threshold.
- 1. Problem-diagnosis agent maps each kept profile to a specific
- ```
   FCIM-solvable problem and the matching service.
  ```
- 1. Hunter.io finds verified email per kept profile (graceful fallback to multi-pattern guess).
- 1. Renders index.html grouped by FCIM service, then by region.
- 
- Daily cost target:
- Apify Short mode: 2 queries/weekday × $0.10 = ~$4.40/month  (fits free tier)
- Hunter.io: ~10–25 verified-email lookups/day on Starter plan
  */
  const fs = require(‘node:fs’);

// =========================================================================
// CONFIG
// =========================================================================

const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) {
console.error(‘FATAL: APIFY_TOKEN env var missing. Set it as a GitHub repo secret.’);
process.exit(1);
}
const HUNTER_API_KEY = process.env.HUNTER_API_KEY || null;
if (!HUNTER_API_KEY) {
console.warn(‘NOTE: HUNTER_API_KEY missing — emails will fall back to format-pattern guesses.’);
}

const APIFY_ACTOR = ‘harvestapi~linkedin-profile-search’;
const PROFILES_PER_QUERY = 12;
const QUALITY_THRESHOLD = 5;
const MAX_HUNTER_CALLS_PER_RUN = 25;

// =========================================================================
// SEARCH BUCKETS — 7 buckets, weekday rotation runs 2 per day
// Mapped to Yehya’s stated priorities:
//   Family offices: Caucasus, Russia/CIS, Africa, MENA (NOT mega-GCC)
//   Commodity hedging targets
//   Up-and-coming firms with structuring needs
//   HNW with international holdings
//   EAMs / referral partners
// =========================================================================

const QUERY_BUCKETS = [
{
label: ‘Armenian / Caucasus Dubai’,
body: {
searchQuery: ‘Armenian Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: ‘Caucasus / CIS’,
serviceHint: ‘Foundation + Private Fund’
},
{
label: ‘Russian Dubai’,
body: {
searchQuery: ‘Russian Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: ‘Russia / CIS’,
serviceHint: ‘Foundation + Private Fund’
},
{
label: ‘Nigerian / African Dubai’,
body: {
searchQuery: ‘Nigerian Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: ‘Africa’,
serviceHint: ‘Foundation + Private Fund’
},
{
label: ‘Lebanese Dubai’,
body: {
searchQuery: ‘Lebanese Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: ‘MENA / Levant’,
serviceHint: ‘Foundation + Private Fund’
},
{
label: ‘Egyptian Dubai’,
body: {
searchQuery: ‘Egyptian Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: ‘Egypt’,
serviceHint: ‘Foundation + Private Fund’
},
{
label: ‘Commodity trader Dubai’,
body: {
searchQuery: ‘commodity trader Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: null,
serviceHint: ‘Commodity Derivatives’
},
{
label: ‘Family office Dubai’,
body: {
searchQuery: ‘family office Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: null,
serviceHint: ‘Foundation + Private Fund’
},
{
label: ‘Wealth manager Dubai’,
body: {
searchQuery: ‘wealth manager Dubai’,
profileScraperMode: ‘Short’,
maxItems: 25
},
region: null,
serviceHint: ‘Discretionary Portfolio Management’
}
];

function pickTodaysQueries() {
const now = new Date();
const start = new Date(Date.UTC(now.getUTCFullYear(), 0, 0));
const dayOfYear = Math.floor((now.getTime() - start.getTime()) / 86400000);
const i = dayOfYear % QUERY_BUCKETS.length;
const j = (i + 1) % QUERY_BUCKETS.length;
return [QUERY_BUCKETS[i], QUERY_BUCKETS[j]];
}

// =========================================================================
// FCIM SERVICES — verbatim from the corporate profile + strategic blueprint PDFs
// =========================================================================

const SERVICES = [
{
name: ‘Discretionary Portfolio Management’,
desc: ‘Five CMA-approved models from capital preservation through aggressive. USD 1M minimum.’,
solution: ‘Discretionary mandate on a CMA-approved model portfolio matched to the client\u2019s risk profile and time horizon.’
},
{
name: ‘Foundation + Private Fund’,
desc: ‘UAE Foundation owning a CMA Private Fund. Three-level control. UBO privacy. 10-day approval.’,
solution: ‘UAE Foundation owning a CMA Private Fund. Only FCIM and the regulator know the UBO. Three-level control: directly at the asset, via the fund board, via the foundation council. Already executed for $15-20M acquisition vehicles and $100M+ real estate restructures.’
},
{
name: ‘CMA Private Fund (standalone)’,
desc: ‘Regulated UAE private fund. No restriction on asset type. Fast-track 10 working day approval.’,
solution: ‘Standalone CMA Private Fund. No restrictions on asset class — public/private equity, credit, real estate, single-asset 100% concentration. FCIM acts as both manager and administrator.’
},
{
name: ‘Commodity Derivatives’,
desc: ‘SCA-licensed direct CME / ICE / LME / EEX / SGX access without a clearing account.’,
solution: ‘SCA-licensed commodity derivatives platform. Direct access to CME, ICE, LME, EEX, SGX without the client needing to set up their own clearing account. Futures, options, swaps, cross-commodity spreads. Risk and margin reporting built in.’
},
{
name: ‘Fund Administration’,
desc: ‘One of only five UAE-authorised fund administrators. In-house or third-party funds.’,
solution: ‘Fund administration for in-house or third-party funds, UAE or foreign-domiciled. NAV calculation, investor servicing, regulatory reporting, multi-tier verification.’
},
{
name: ‘IB & Advisory’,
desc: ‘Dmitri Tchekalkine-led desk. ECM, DCM, M&A, listings on UAE exchanges.’,
solution: ‘Investment banking and advisory. IPO / bond / sukuk issuance manager, UAE exchange listing advisory, M&A in the USD 50-150M band. Led by Dmitri Tchekalkine (30+ years emerging markets, ex-JPMorgan/BNP/HSBC).’
},
{
name: ‘Family Office Advisory’,
desc: ‘Governance, succession, estate planning, concierge, VC/PE direct deals.’,
solution: ‘Full family office build: governance frameworks, multi-generational succession planning, estate structuring, concierge, and direct VC/PE deal access aligned to family interests.’
},
{
name: ‘EAM / FI Platform’,
desc: ‘Confidential Client Money accounts at FAB and ENBD. Secondary custodianship for EAMs and FIs.’,
solution: ‘Platform for external asset managers and financial institutions. Open Confidential Client Money accounts at First Abu Dhabi Bank and Emirates NBD. FCIM acts as secondary custodian; brokers execute under their names representing FCIM accounts.’
}
];

// =========================================================================
// PROBLEMS — diagnostic patterns. Each prospect matched to strongest fit.
// =========================================================================

const PROBLEMS = [
{
id: ‘international-asset-holding’,
label: ‘Holding international assets under sovereign / banking pressure’,
signals: [
/\b(international|cross[- ]border|european|offshore)\s+(asset|real estate|holding|property|portfolio)/i,
/\b(sovereign|sanction|banking|payment)\s+(risk|pressure|challenge|difficulty)/i,
/\b(re-?structure|re-?domicile|consolidate)\s+(holdings|assets|portfolio)/i
],
fcimService: ‘Foundation + Private Fund’,
angle: ‘International asset-holding and banking-access pressure — the same pattern FCIM solved for the $100M+ Family A real-estate restructure case.’
},
{
id: ‘ubo-privacy’,
label: ‘UBO privacy / anonymity in acquisitions’,
signals: [
/\b(confidential|anonymity|private)\s+(acquisition|investment|deal|transaction)/i,
/\b(UBO|beneficial owner|disclosure)\b/i,
/\b(undisclosed|private|sensitive)\s+(stake|investment|holding)/i
],
fcimService: ‘Foundation + Private Fund’,
angle: ‘UBO privacy in acquisitions — same structure FCIM used for the Principal A football-club roll-up ($15-20M per acquisition, 3 clubs done, UBO never disclosed to counterparties or banks).’
},
{
id: ‘multi-venture-structuring’,
label: ‘Founder with multiple ventures needing structured holdco’,
signals: [
/\b(multiple|portfolio of|several)\s+(ventures|companies|businesses|investments)/i,
/\b(serial|repeat)\s+(entrepreneur|founder|investor)/i,
/\b(investment\s+holding|holding\s+company|family\s+holding)\b/i,
/\b(group\s+chairman|group\s+CEO|founder\s+&\s+chairman)\b/i
],
fcimService: ‘Foundation + Private Fund’,
angle: ‘Multiple operating ventures held under personal name or scattered SPVs — Foundation + Private Fund provides one umbrella with three-level control and clean UBO privacy.’
},
{
id: ‘family-succession’,
label: ‘Family succession / generational wealth transition’,
signals: [
/\b(next generation|second generation|third generation|2G|3G)\b/i,
/\b(succession|legacy|inheritance|estate)\s+(planning|transition|strategy)/i,
/\b(family\s+(business|enterprise|legacy|trust|council))\b/i
],
fcimService: ‘Family Office Advisory’,
angle: ‘Family succession and generational transition — full family-office build covering governance, succession, estate planning, and concierge.’
},
{
id: ‘commodity-hedging’,
label: ‘Physical commodity exposure without hedging infrastructure’,
signals: [
/\b(physical\s+(commodity|trader|trading))\b/i,
/\b(grain|fertili[sz]er|freight|metals|energy|oilseed|sugar|cocoa|coffee|cotton)\s+(trad|market|hedg)/i,
/\b(hedging|risk\s+management)\b.*\b(commodity|commodities|metals|energy|grain)/i,
/\b(import|export)\s+(business|operations|trader)\b/i
],
fcimService: ‘Commodity Derivatives’,
angle: ‘Physical commodity exposure without an exchange-cleared hedging desk — FCIM\u2019s SCA-licensed platform gives direct CME/ICE/LME access without the client setting up their own clearing account.’
},
{
id: ‘eam-platform-need’,
label: ‘EAM / boutique wealth manager looking for client-money platform’,
signals: [
/\b(external\s+asset\s+manager|EAM)\b/i,
/\b(independent\s+(wealth|financial)\s+(manager|advisor))\b/i,
/\b(boutique|managing\s+partner).*\b(wealth|advisory|asset\s+management)/i,
/\b(family\s+wealth\s+advisor|multi-?family\s+office\s+founder)\b/i
],
fcimService: ‘EAM / FI Platform’,
angle: ‘EAM looking for a regulated platform — FCIM provides Confidential Client Money accounts at FAB and ENBD plus secondary custodianship, freeing the EAM from holding client funds directly.’
},
{
id: ‘fund-launch-or-admin’,
label: ‘Fund launching or needing admin upgrade’,
signals: [
/\b(launching|launched|new)\s+(fund|vehicle)/i,
/\b(general\s+partner|GP\s+at|fund\s+manager)\b/i,
/\b(fund\s+(admin|administrator|administration|services))\b/i,
/\b(NAV|fund\s+accounting|fund\s+operations)\b/i
],
fcimService: ‘Fund Administration’,
angle: ‘Fund launch or admin pain — FCIM is one of only five UAE-authorised fund administrators, can serve as manager and administrator both for fast-tracked CMA approval (10 working days).’
},
{
id: ‘pre-ipo-or-ma’,
label: ‘Pre-IPO or M&A advisory candidate’,
signals: [
/\b(pre-?IPO|going\s+public|listing\s+plans)\b/i,
/\b(M&A|mergers|acquisitions)\s+(advisor|target|strategy)/i,
/\b(capital\s+raise|growth\s+equity|series\s+[CDE])\b/i,
/\b(corporate\s+finance|capital\s+markets)\s+(director|head|managing)/i
],
fcimService: ‘IB & Advisory’,
angle: ‘Capital-markets activity ahead — FCIM\u2019s IB desk (Dmitri Tchekalkine, 30+ yrs, ex-JPM/BNP/HSBC) covers ECM/DCM/M&A in the $50-150M band with UAE exchange listing capability.’
},
{
id: ‘discretionary-mandate’,
label: ‘HNW with liquid capital seeking discretionary mandate’,
signals: [
/\b(post-?exit|exited|sold\s+(my|the)\s+(company|business))\b/i,
/\b(personal\s+investment\s+company|PIC)\b/i,
/\b(family\s+wealth|personal\s+wealth|liquid\s+capital)\b/i,
/\b(HNW|UHNW|high\s+net\s+worth)\b/i
],
fcimService: ‘Discretionary Portfolio Management’,
angle: ‘Liquid personal capital seeking a managed mandate — five CMA-approved model portfolios spanning capital preservation through aggressive, $1M minimum entry.’
},
{
id: ‘sensitive-jurisdiction-banking’,
label: ‘Banking access difficulty due to sensitive nationality / jurisdiction’,
signals: [
/\b(Russian|Belarusian|Iranian|Syrian)\b/i,
/\b(sanction|de-?risk|banking\s+access)\b/i,
/\b(re-?domicile|relocation|UAE\s+residency)\b/i
],
fcimService: ‘Foundation + Private Fund’,
angle: ‘Banking access friction due to nationality or jurisdiction — UAE-regulated Foundation + Private Fund structure passes KYC where personal-name holdings increasingly cannot.’
}
];

// =========================================================================
// REGIONS — team routing
// =========================================================================

const REGIONS = [
{ name: ‘MENA / Levant’,    lead: ‘Amr Fergany’,
warmPath: ‘Amr Fergany via the Credit Suisse DIFC alumni network and the Levantine professional community in DIFC and Business Bay.’ },
{ name: ‘Egypt’,            lead: ‘Ibrahim Hemeida’,
warmPath: ‘Ibrahim Hemeida via the Egyptian Business Council Dubai and his Egyptian banking-sector relationships.’ },
{ name: ‘Russia / CIS’,     lead: ‘Dmitri Tchekalkine’,
warmPath: ‘Dmitri Tchekalkine via 30+ years of EM banking relationships at Chemical Bank, JPMorgan, BNP and HSBC.’ },
{ name: ‘Caucasus / CIS’,   lead: ‘Dmitri Tchekalkine’,
warmPath: ‘Dmitri Tchekalkine via his Caucasus and CIS banking relationships built across his JPMorgan and HSBC tenures.’ },
{ name: ‘India’,            lead: ‘Saran Sankar’,
warmPath: ‘Saran Sankar via the UBS Investment Bank alumni network and Indian Business & Professional Council Dubai.’ },
{ name: ‘Africa’,           lead: ‘Ibrahim Hemeida’,
warmPath: ‘Ibrahim Hemeida via Dubai-based African diaspora professional networks and his MENA emerging-markets relationships.’ },
{ name: ‘UK / Western’,     lead: ‘Steven Downey’,
warmPath: ‘Steven Downey via the London Business School alumni community and CFA Society UAE.’ }
];

// =========================================================================
// COMPLIANCE — hard blocks (Yehya rules)
// =========================================================================

const COMPLIANCE_BLOCK = [
/\bgary\s+dugan\b/i,
/\b(al\s+maktoum|bin\s+rashid\s+al\s+maktoum|mohammed\s+bin\s+rashid|hamdan\s+bin\s+mohammed)\b/i,
/\b(al\s+nahyan|bin\s+zayed\s+al\s+nahyan|mohamed\s+bin\s+zayed)\b/i,
/\b(prigozhin|usmanov|deripaska|abramovich|vekselberg|rotenberg|fridman)\b/i,
/\barqaam\s+capital\b/i,
/\b(mashreq|emirates\s+nbd|enbd)\b/i,
/\bindex\s+&\s+cie\b/i,
/\bskybound\s+wealth\b/i
];
const COMPLIANCE_WARN = /\b(PEP|politically\s+exposed|state[- ]owned|sovereign\s+wealth|ministry\s+of)\b/i;

// =========================================================================
// APIFY CALL
// =========================================================================

async function runApifyActor(bucket) {
const url = `https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?token=${APIFY_TOKEN}`;
try {
const res = await fetch(url, {
method: ‘POST’,
headers: { ‘Content-Type’: ‘application/json’ },
body: JSON.stringify(bucket.body)
});
if (!res.ok) {
const text = await res.text();
console.warn(`Apify "${bucket.label}": HTTP ${res.status} — ${text.slice(0, 200)}`);
return [];
}
const data = await res.json();
if (!Array.isArray(data)) {
console.warn(`Apify "${bucket.label}": unexpected shape`);
return [];
}
return data.map(p => ({
…p,
_queryRegion: bucket.region,
_queryLabel: bucket.label,
_queryServiceHint: bucket.serviceHint
}));
} catch (e) {
console.warn(`Apify "${bucket.label}" failed: ${e.message}`);
return [];
}
}

// =========================================================================
// PROFILE NORMALISATION
// =========================================================================

function normaliseProfile(raw) {
const fullName = (raw.firstName || raw.lastName)
? `${raw.firstName || ''} ${raw.lastName || ''}`.trim()
: (raw.fullName || raw.name || ‘Name unavailable’);
const company = (Array.isArray(raw.currentPosition) && raw.currentPosition[0])
? (raw.currentPosition[0].companyName || ‘’)
: (raw.currentCompany || raw.company || ‘’);
const locText = (raw.location && typeof raw.location === ‘object’)
? (raw.location.linkedinText || (raw.location.parsed && raw.location.parsed.text) || ‘Dubai’)
: (raw.location || ‘Dubai’);
const slug = raw.publicIdentifier && !/^AC[oOwW]A/.test(raw.publicIdentifier)
? raw.publicIdentifier
: null;
const linkedinUrl = slug
? `https://www.linkedin.com/in/${slug}`
: (raw.linkedinUrl || raw.profileUrl || raw.url || ‘’);
return {
firstName: raw.firstName || (fullName.split(’ ‘)[0] || ‘’),
lastName: raw.lastName || (fullName.split(’ ‘).slice(1).join(’ ’) || ‘’),
name: fullName,
title: raw.headline || raw.title || raw.position || ‘’,
company: company,
location: locText,
linkedinUrl: linkedinUrl,
publicIdentifier: slug || null,
email: raw.email || raw.emailAddress || null,
emailScore: null,
emailVerified: !!(raw.email || raw.emailAddress),
about: raw.about || raw.summary || ‘’,
_queryRegion: raw._queryRegion,
_queryLabel: raw._queryLabel,
_queryServiceHint: raw._queryServiceHint
};
}

// =========================================================================
// QUALITY GATE
// =========================================================================

function scoreProfile(p) {
const text = `${p.title} ${p.company} ${p.about}`.toLowerCase();
let score = 0;
const reasons = [];

if (/\b(family\s+office|single\s+family\s+office|multi[- ]family\s+office)\b/i.test(text)) {
score += 6; reasons.push(’+6 family office’);
}
if (/\b(private\s+banker|wealth\s+manager|wealth\s+advisor|private\s+banking)\b/i.test(text)) {
score += 5; reasons.push(’+5 wealth/PB title’);
}
if (/\b(managing\s+partner|managing\s+director|founding\s+partner|general\s+partner)\b/i.test(text)) {
score += 4; reasons.push(’+4 senior partner’);
}
if (/\b(chief\s+investment\s+officer|CIO|head\s+of\s+investment)\b/i.test(text)) {
score += 5; reasons.push(’+5 CIO’);
}
if (/\b(fund\s+manager|portfolio\s+manager|hedge\s+fund|private\s+equity|venture\s+capital)\b/i.test(text)) {
score += 4; reasons.push(’+4 fund/PM’);
}
if (/\b(external\s+asset\s+manager|EAM|independent\s+wealth)\b/i.test(text)) {
score += 5; reasons.push(’+5 EAM’);
}
if (/\b(AED|USD|EUR)\s*\d+\s*(million|billion|mn|bn)\b/i.test(text) ||
/\b$\s*\d+\s*(million|billion|mn|bn|m|b)\b/i.test(text)) {
score += 3; reasons.push(’+3 monetary scale’);
}
if (/\b(assets\s+under\s+management|AUM|managed.*portfolio)/i.test(text)) {
score += 3; reasons.push(’+3 AUM’);
}
if (/\b(multiple\s+(ventures|businesses|companies)|portfolio\s+of\s+companies|investment\s+holding|group\s+chairman)\b/i.test(text)) {
score += 4; reasons.push(’+4 multi-venture’);
}
if (/\b(physical\s+commod|commodity\s+trad|grain|fertili[sz]er|freight|metals\s+trad|energy\s+trad)/i.test(text)) {
score += 4; reasons.push(’+4 commodity’);
}

// Negative signals — drop the Alfred David type
if (/\b(d2c|direct[- ]to[- ]consumer|e-?commerce|amazon\s+seller|shopify)/i.test(text)) {
score -= 8; reasons.push(’-8 D2C/e-commerce’);
}
if (/\b(retail|hospitality|restaurant|cafe|f&b)\b/i.test(text) &&
!/\b(wealth|investment|finance|capital|fund)\b/i.test(text)) {
score -= 6; reasons.push(’-6 retail w/o finance’);
}
if (/\b(student|intern|junior)\b/i.test(text)) {
score -= 5; reasons.push(’-5 junior’);
}
if (/\b(marketing|growth|content|social\s+media|HR|recruit)\b/i.test(text) &&
!/\b(wealth|investment|finance|capital|fund|family\s+office)\b/i.test(text)) {
score -= 4; reasons.push(’-4 non-finance function’);
}
if (/\bfounder\b/i.test(text) && !/(family\s+office|investment|fund|capital|wealth|holding)/i.test(text)) {
score -= 3; reasons.push(’-3 generic founder’);
}

return { score, reasons };
}

// =========================================================================
// PROBLEM-DIAGNOSIS AGENT
// =========================================================================

function diagnoseProblem(p) {
const text = `${p.title} ${p.company} ${p.about}`.toLowerCase();
const matches = [];
for (const prob of PROBLEMS) {
let hits = 0;
for (const sig of prob.signals) {
if (sig.test(text)) hits++;
}
if (hits > 0) matches.push({ prob, hits });
}
matches.sort((a, b) => b.hits - a.hits);

if (matches.length === 0) {
const fallbackService = p._queryServiceHint || ‘Discretionary Portfolio Management’;
return {
problem: ‘Profile lookup — no specific FCIM problem auto-detected. Manual review.’,
fcimService: fallbackService,
angle: `Inferred from search bucket "${p._queryLabel || 'general'}". Worth manual scan to confirm fit.`,
confidence: ‘low’
};
}
const top = matches[0];
return {
problem: top.prob.label,
fcimService: top.prob.fcimService,
angle: top.prob.angle,
confidence: top.hits >= 2 ? ‘high’ : ‘medium’,
secondary: matches.slice(1, 3).map(m => m.prob.label)
};
}

// =========================================================================
// HUNTER.IO EMAIL FINDER
// =========================================================================

async function findEmailViaHunter(p) {
if (!HUNTER_API_KEY) return null;
if (!p.firstName || !p.lastName || !p.company) return null;
const params = new URLSearchParams({
company: p.company,
first_name: p.firstName,
last_name: p.lastName,
api_key: HUNTER_API_KEY
});
try {
const res = await fetch(`https://api.hunter.io/v2/email-finder?${params.toString()}`);
if (!res.ok) {
const text = await res.text();
console.warn(`Hunter ${p.name}: HTTP ${res.status} — ${text.slice(0, 200)}`);
return null;
}
const data = await res.json();
const d = data && data.data;
if (d && d.email) {
return {
email: d.email,
score: d.score || null,
verificationStatus: (d.verification && d.verification.status) || null,
domain: d.domain || null
};
}
return null;
} catch (e) {
console.warn(`Hunter ${p.name} failed: ${e.message}`);
return null;
}
}

function guessEmailPatterns(p) {
if (!p.firstName || !p.company) return [];
const cleaned = p.company
.toLowerCase()
.replace(/\b(llc|ltd|limited|gmbh|sa|sarl|inc|fzc|fze|dmcc|llp|plc|holdings?|group|capital|partners?|investments?|management)\b/gi, ‘’)
.replace(/[^a-z0-9]+/g, ‘’)
.trim();
if (!cleaned) return [];
const domains = [`${cleaned}.com`, `${cleaned}.ae`, `${cleaned}.io`];
const f = p.firstName.toLowerCase().replace(/[^a-z]/g, ‘’);
const l = (p.lastName || ‘’).toLowerCase().replace(/[^a-z]/g, ‘’);
const out = [];
for (const d of domains) {
if (f) out.push(`${f}@${d}`);
if (f && l) out.push(`${f}.${l}@${d}`);
if (f && l) out.push(`${f[0]}${l}@${d}`);
}
return out.slice(0, 3);
}

function hunterVerifyLink(p) {
if (!p.firstName) return null;
const params = new URLSearchParams({
first_name: p.firstName,
last_name: p.lastName || ‘’,
company: p.company || ‘’
});
return `https://hunter.io/email-finder?${params.toString()}`;
}

// =========================================================================
// COMPLIANCE & DEDUPE
// =========================================================================

function runCompliance(p) {
const text = `${p.name} ${p.company} ${p.about}`;
for (const re of COMPLIANCE_BLOCK) {
if (re.test(text)) return { allowed: false };
}
return { allowed: true, pep: COMPLIANCE_WARN.test(text) };
}

function fingerprint(p) {
return `${(p.name || '').toLowerCase().trim()}|${(p.company || '').toLowerCase().trim()}`;
}

// =========================================================================
// HTML HELPERS
// =========================================================================

function decodeEntities(s) {
return String(s == null ? ‘’ : s)
.replace(/&/g, ‘&’)
.replace(/</g, ‘<’)
.replace(/>/g, ‘>’)
.replace(/"/g, ‘”’)
.replace(/'/g, “’”)
.replace(/'/g, “’”)
.replace(/ /g, ’ ’);
}

function escapeHtml(s) {
return decodeEntities(s)
.replace(/&/g, ‘&’).replace(/</g, ‘<’).replace(/>/g, ‘>’)
.replace(/”/g, ‘"’).replace(/’/g, ‘'’);
}

// =========================================================================
// REGION ROUTING
// =========================================================================

function routeRegion(p) {
if (p._queryRegion) return p._queryRegion;
const text = `${p.name} ${p.title} ${p.company} ${p.about} ${p.location}`.toLowerCase();
if (/\b(armen|azerb|georgi[a]|tbilisi|yerevan|baku)\b/i.test(text)) return ‘Caucasus / CIS’;
if (/\b(russia|russian|moscow|st.?\s*petersburg|kazakh|belarus|uzbek|ukrain)\b/i.test(text)) return ‘Russia / CIS’;
if (/\b(egypt|cairo|alexandria)\b/i.test(text)) return ‘Egypt’;
if (/\b(lebanon|lebanese|beirut|jordan|syria|levant|moroc|tunis|algeri)\b/i.test(text)) return ‘MENA / Levant’;
if (/\b(india|indian|mumbai|delhi|bangalore|chennai|gurgaon)\b/i.test(text)) return ‘India’;
if (/\b(nigeri|kenya|south\s+africa|ghana|senegal|ethiopia|tanzania|uganda|ivory\s+coast|cote\s+d)\b/i.test(text)) return ‘Africa’;
if (/\b(london|england|britain|british|uk\b|scotland|german|swiss|switzerland)\b/i.test(text)) return ‘UK / Western’;
return null;
}

// =========================================================================
// DRAFT PROMPT
// =========================================================================

function buildDraftPrompt(p) {
const dx = p.diagnosis || {};
const emailLine = p.emailVerified
? `Verified email: ${p.email}`
: (p.emailGuesses && p.emailGuesses.length
? `Email (best guess, unverified): ${p.emailGuesses[0]}. Other patterns: ${p.emailGuesses.slice(1).join(', ')}. Verify on Hunter.io before sending.`
: ‘Email: not found — use LinkedIn InMail or company site.’);
return `Draft Yehya Abdelbaki’s FCIM outreach to a live prospect. The diagnostic agent has identified the FCIM angle for you — use it.

PROSPECT
Name: ${p.name}
Title: ${p.title}
Company: ${p.company}
Location: ${p.location}
LinkedIn: ${p.linkedinUrl}
${emailLine}

DIAGNOSED PROBLEM
${dx.problem || ‘(manual review)’}

FCIM SOLUTION (this is the core angle — anchor the email here)
${dx.angle || ‘Tailor based on profile.’}
Matched service: ${dx.fcimService || ‘(pick best fit)’}

REGION & WARM PATH
Region: ${p.region || ‘(not detected)’}
Suggested lead: ${p.regionLead || ‘(route by context)’}
Warm path: ${p.regionWarmPath || ‘(route by context)’}

WRITING RULES

- Under 200 words.
- Open with a specific, non-glazing reference to their actual situation.
- Lead with the diagnosed problem framed as observation, not assumption.
- Position FCIM solution clearly but not pitchy. Reference real specifics ($6B AUM, CMA-licensed, real case studies — $15-20M acquisition vehicles, $100M+ real estate restructure) only when they add credibility.
- Reference the warm-path FCIM colleague naturally only if it strengthens credibility.
- Do not glaze. Do not be over-eager. Do not pitch every service.
- NEVER mention Gary Dugan.
- Subject line: specific, attention-grabbing, not promotional, not cringe.
- End with full Yehya signature block: Yehya Abdelbaki / Relationship Manager / Fundament Capital Investment Management / yehya.abdelbaki@fundamentcapital.ae | +971 4 834 8385`;
  }

// =========================================================================
// CARD RENDERING
// =========================================================================

function prospectCardHtml(p, isFeatured) {
const dx = p.diagnosis || {};
const service = SERVICES.find(s => s.name === dx.fcimService);
const solutionText = service ? service.solution : ‘Service match indeterminate — review profile.’;

const complianceBlock = p.pep
? `<div class="compliance pep"><span class="label">Elevated DD</span>PEP / state-linked exposure detected. Enhanced due diligence required before outreach.</div>`
: ‘’;

let emailLine;
if (p.emailVerified && p.email) {
const score = p.emailScore ? ` <em>(Hunter score: ${p.emailScore})</em>` : ‘’;
emailLine = `<strong>Verified:</strong> <a href="mailto:${escapeHtml(p.email)}">${escapeHtml(p.email)}</a>${score}`;
} else if (p.emailGuesses && p.emailGuesses.length) {
const link = p.hunterVerifyLink ? ` <a href="${escapeHtml(p.hunterVerifyLink)}" target="_blank" rel="noopener noreferrer">verify on Hunter \u203a</a>` : ‘’;
emailLine = `<em>Best guess (unverified):</em> ${escapeHtml(p.emailGuesses[0])}${link}<br><em>Other patterns:</em> ${escapeHtml(p.emailGuesses.slice(1).join(', '))}`;
} else {
emailLine = `<em>not found \u2014 use LinkedIn InMail</em>`;
}

const conf = dx.confidence
? `<span style="font-size:10px; letter-spacing:0.15em; text-transform:uppercase; padding:2px 8px; border-radius:999px; background:${dx.confidence === 'high' ? '#C9A544' : dx.confidence === 'medium' ? '#F6EFD8' : '#EAE4DA'}; color:#1C1A16; margin-left:8px;">${dx.confidence}</span>`
: ‘’;

const problemBlock = dx.problem
? `<div class="section" style="background:#F6EFD8; padding:12px 14px; border-left:3px solid #C9A544; border-radius:4px;"> <div class="label">Diagnosed problem ${conf}</div> <p style="margin-top:6px;"><strong>${escapeHtml(dx.problem)}</strong></p> <p style="margin-top:8px; font-size:13px; color:#3C3730;">${escapeHtml(dx.angle || '')}</p> </div>`
: ‘’;

const prompt = buildDraftPrompt(p);

return `<article class="prospect ${isFeatured ? 'featured' : ''}"> <div class="service-tag">${escapeHtml(dx.fcimService || 'Manual review')}</div> <div class="head-row"> <div class="head-main"> <h3>${escapeHtml(p.name)}</h3> <div class="sub"> ${escapeHtml(p.title)}${p.company ?`<span class="dot">\u00b7</span>${escapeHtml(p.company)}`: ''}${p.region ?`<span class="dot">\u00b7</span>${escapeHtml(p.region)}`: ''} </div> </div> ${p.regionLead ?`<div class="lead-block"><span class="lead-label">Lead</span>${escapeHtml(p.regionLead)}</div>`: ''} </div> ${problemBlock} <div class="section"> <div class="label">Contact</div> <p> <strong>LinkedIn:</strong> ${p.linkedinUrl ?`<a href="${escapeHtml(p.linkedinUrl)}" target="_blank" rel="noopener noreferrer">profile \u203a</a>`: '<em>not available</em>'}<br> <strong>Email:</strong> ${emailLine} </p> </div> ${p.about ?`<div class="section"><div class="label">Background</div><p>${escapeHtml(p.about.slice(0, 300))}${p.about.length > 300 ? ‘\u2026’ : ‘’}</p></div>`: ''} <div class="section"> <div class="label">FCIM solution</div> <p>${escapeHtml(solutionText)}</p> </div> <div class="section"> <div class="label">Warm path</div> <p>${escapeHtml(p.regionWarmPath || 'Region indeterminate. Route to the FCIM colleague best matched to the subject\u2019s nationality / sector.')}</p> </div> ${complianceBlock} <div class="first-step"> <div class="label">First step</div> <p>Approach ${escapeHtml(p.name)} via ${p.regionLead ? escapeHtml(p.regionLead) + '\u2019s network' : 'the appropriate FCIM colleague'}. Tap below to copy the diagnostic-agent draft prompt — paste it in your Claude chat and Yehya\u2019s email comes back, anchored on the diagnosed FCIM angle.</p> <button class="draft-btn" data-prompt="${escapeHtml(prompt)}">Copy draft prompt</button> </div> </article>`;
}

function renderServiceSection(svc, items) {
if (items.length === 0) {
return ` <section class="service-section empty"> <div class="service-header"> <div class="left"> <h2>${escapeHtml(svc.name)}</h2> <p>${escapeHtml(svc.desc)}</p> </div> <div class="right-meta">No prospects today</div> </div> </section>`;
}

const byRegion = {};
for (const p of items) {
const key = p.region || ‘__unrouted’;
if (!byRegion[key]) byRegion[key] = [];
byRegion[key].push(p);
}
const regionOrder = REGIONS.map(r => r.name).filter(n => byRegion[n]);
if (byRegion.__unrouted) regionOrder.push(’__unrouted’);

const REGION_BLOCK = ‘margin: 28px 0 8px; padding: 14px 18px; background: var(–cream-yellow, #F6EFD8); border-left: 3px solid var(–mustard, #C9A544); border-radius: 4px;’;
const REGION_ROW = ‘display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;’;
const REGION_NAME = ‘font-family: 'Fraunces', ui-serif, Georgia, serif; font-style: italic; font-weight: 500; font-size: 19px; margin: 0; color: var(–ink, #1C1A16);’;
const REGION_META = ‘font-size: 11px; letter-spacing: 0.14em; text-transform: uppercase; color: var(–mustard-deep, #6F561A); font-weight: 600;’;

const blocks = regionOrder.map(rname => {
const ps = byRegion[rname];
const r = REGIONS.find(rr => rr.name === rname);
const name = (rname === ‘__unrouted’) ? ‘Unrouted’ : rname;
const lead = (rname === ‘__unrouted’) ? ‘Route by context’ : (r && r.lead ? r.lead : ‘Lead TBD’);
return ` <div class="region-block" style="${REGION_BLOCK}"> <div style="${REGION_ROW}"> <h3 style="${REGION_NAME}">${escapeHtml(name)}</h3> <div style="${REGION_META}">${escapeHtml(lead)} \u00b7 ${ps.length} prospect${ps.length === 1 ? '' : 's'}</div> </div> </div> <div class="items">${ps.map(p => prospectCardHtml(p, false)).join('')}</div>`;
}).join(’’);

return ` <section class="service-section"> <div class="service-header"> <div class="left"> <h2>${escapeHtml(svc.name)}</h2> <p>${escapeHtml(svc.desc)}</p> </div> <div class="right-meta">${items.length} prospect${items.length === 1 ? '' : 's'} \u00b7 ${regionOrder.length} region${regionOrder.length === 1 ? '' : 's'}</div> </div> ${blocks} </section>`;
}

function renderRegionChips(regionCounts) {
return REGIONS.map(r => {
const n = regionCounts[r.name] || 0;
const empty = n === 0;
const sep = ‘<span aria-hidden="true" style="opacity:0.4; margin:0 4px;">\u00b7</span>’;
return `<button class="region-chip ${empty ? 'empty' : ''}" ${empty ? 'disabled' :`data-region=”${escapeHtml(r.name)}”`}> <span>${escapeHtml(r.name)}</span>${sep}<span class="count">${n}</span>${r.lead ? `${sep}<span class="lead">${escapeHtml(r.lead.split(’ ‘)[0])}</span>` : ''} </button>`;
}).join(’’);
}

// =========================================================================
// MAIN
// =========================================================================

async function main() {
console.log(‘FCIM Daily Build v3 — starting’);
const buckets = pickTodaysQueries();
console.log(`Today's buckets: ${buckets.map(b => b.label).join(' | ')}`);

const results = await Promise.all(buckets.map(runApifyActor));
const raw = results.flat();
console.log(`Raw profiles: ${raw.length}`);
if (raw.length > 0) {
console.log(`Sample profile keys: ${Object.keys(raw[0]).slice(0, 25).join(', ')}`);
}

let profiles = raw.map(normaliseProfile);

let blocked = 0;
profiles = profiles.filter(p => {
const c = runCompliance(p);
if (!c.allowed) { blocked++; return false; }
p.pep = !!c.pep;
return true;
});

const seen = new Set();
profiles = profiles.filter(p => {
const fp = fingerprint(p);
if (!fp || fp === ‘|’ || seen.has(fp)) return false;
seen.add(fp);
return true;
});
console.log(`After compliance+dedupe: ${profiles.length} (blocked: ${blocked})`);

const beforeGate = profiles.length;
profiles = profiles
.map(p => {
const s = scoreProfile(p);
p._score = s.score;
p._scoreReasons = s.reasons;
return p;
})
.filter(p => p._score >= QUALITY_THRESHOLD)
.sort((a, b) => b._score - a._score);
console.log(`After quality gate (>= ${QUALITY_THRESHOLD}): ${profiles.length} (dropped ${beforeGate - profiles.length} low-score)`);

for (const p of profiles) {
p.region = routeRegion(p);
const meta = REGIONS.find(r => r.name === p.region);
p.regionLead = meta ? meta.lead : null;
p.regionWarmPath = meta ? meta.warmPath : null;
p.diagnosis = diagnoseProblem(p);
}

let hunterCalls = 0;
for (const p of profiles) {
if (hunterCalls >= MAX_HUNTER_CALLS_PER_RUN) break;
if (p.emailVerified) continue;
if (!HUNTER_API_KEY) continue;
const result = await findEmailViaHunter(p);
hunterCalls++;
if (result && result.email) {
p.email = result.email;
p.emailScore = result.score;
p.emailVerified = result.verificationStatus === ‘valid’ || (result.score && result.score >= 70);
}
}
console.log(`Hunter calls used: ${hunterCalls} (cap ${MAX_HUNTER_CALLS_PER_RUN})`);

for (const p of profiles) {
if (!p.emailVerified) {
p.emailGuesses = guessEmailPatterns(p);
p.hunterVerifyLink = hunterVerifyLink(p);
}
}

const featured = profiles.find(p => p.diagnosis && p.diagnosis.confidence === ‘high’ && !p.pep) ||
profiles[0] || null;
const remaining = featured ? profiles.filter(p => fingerprint(p) !== fingerprint(featured)) : profiles;

let servicesHtml = ‘’;
let emptyServicesHtml = ‘’;
for (const svc of SERVICES) {
const items = remaining.filter(p => p.diagnosis && p.diagnosis.fcimService === svc.name);
const html = renderServiceSection(svc, items);
if (items.length === 0) emptyServicesHtml += html;
else servicesHtml += html;
}

const regionCounts = {};
REGIONS.forEach(r => regionCounts[r.name] = 0);
profiles.forEach(p => { if (p.region && regionCounts[p.region] !== undefined) regionCounts[p.region]++; });

const dateStamp = new Intl.DateTimeFormat(‘en-GB’, { timeZone: ‘Asia/Dubai’, weekday: ‘long’, day: ‘numeric’, month: ‘long’, year: ‘numeric’ }).format(new Date());
const verifiedCount = profiles.filter(p => p.emailVerified).length;
const councilLine = featured
? `Council convened — ${profiles.length} qualified prospects, ${verifiedCount} with verified email, ${blocked} blocked on compliance, ${Object.values(regionCounts).filter(n => n > 0).length} regions active. Buckets pulled today: ${buckets.map(b => b.label).join(' / ')}.`
: `Council couldn\u2019t qualify any prospects today. ${beforeGate} pulled, ${beforeGate - profiles.length} dropped by quality gate, ${blocked} blocked on compliance.`;

const template = fs.readFileSync(‘index.template.html’, ‘utf-8’);
const html = template
.replace(/{{DATE}}/g, escapeHtml(dateStamp))
.replace(/{{BUILT_AT}}/g, escapeHtml(new Date().toISOString()))
.replace(/{{COUNCIL_LINE}}/g, escapeHtml(councilLine))
.replace(/{{REGION_CHIPS}}/g, renderRegionChips(regionCounts))
.replace(/{{FEATURED}}/g, featured ? prospectCardHtml(featured, true) : ‘’)
.replace(/{{CONTENT}}/g, servicesHtml + emptyServicesHtml)
.replace(/{{FEATURED_WRAPPER_STYLE}}/g, featured ? ‘’ : ‘display:none’);

fs.writeFileSync(‘index.html’, html);
console.log(`Built index.html — ${profiles.length} qualified prospects, ${verifiedCount} verified emails, featured: ${featured ? featured.name : 'none'}`);
}

main().catch(err => { console.error(err); process.exit(1); });
