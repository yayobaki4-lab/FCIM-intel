/* FCIM Daily Intelligence - daily builder v3 */
const fs = require('node:fs');

const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) {
  console.error('FATAL: APIFY_TOKEN env var missing.');
  process.exit(1);
}
const HUNTER_API_KEY = process.env.HUNTER_API_KEY || null;
if (!HUNTER_API_KEY) {
  console.warn('NOTE: HUNTER_API_KEY missing - emails fallback to format guesses.');
}
const APIFY_ACTOR_PRIMARY = 'harvestapi~linkedin-profile-search';
const APIFY_ACTOR_FALLBACK = 'harvestapi~linkedin-profile-search-by-services';
const QUALITY_THRESHOLD = 2;
const MAX_HUNTER_CALLS_PER_RUN = 25;

const QUERY_BUCKETS = [
  // Each bucket sends multi-word, finance-specific phrases via queries[] array.
  // Long phrases reduce the actor matching irrelevant profiles (like tile exporters).
  {
    label: 'Family office',
    body: { queries: ['family office Dubai', 'single family office UAE', 'multi family office Dubai'] },
    region: null, serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Wealth manager',
    body: { queries: ['wealth manager Dubai', 'senior wealth manager UAE', 'head of wealth management Dubai'] },
    region: null, serviceHint: 'Discretionary Portfolio Management'
  },
  {
    label: 'Private banker',
    body: { queries: ['private banker Dubai', 'senior private banker UAE', 'head of private banking Dubai'] },
    region: null, serviceHint: 'Discretionary Portfolio Management'
  },
  {
    label: 'Chief Investment Officer',
    body: { queries: ['Chief Investment Officer Dubai', 'CIO investment management UAE', 'head of investments Dubai'] },
    region: null, serviceHint: 'Discretionary Portfolio Management'
  },
  {
    label: 'Fund manager',
    body: { queries: ['fund manager Dubai', 'portfolio manager UAE', 'hedge fund manager Dubai'] },
    region: null, serviceHint: 'CMA Private Fund (standalone)'
  },
  {
    label: 'External asset manager',
    body: { queries: ['external asset manager Dubai', 'EAM Dubai', 'independent wealth advisor UAE'] },
    region: null, serviceHint: 'EAM / FI Platform'
  },
  {
    label: 'Investment director',
    body: { queries: ['investment director Dubai', 'managing director investments UAE', 'head of investments Dubai'] },
    region: null, serviceHint: 'Discretionary Portfolio Management'
  },
  {
    label: 'Managing partner',
    body: { queries: ['managing partner family office Dubai', 'founding partner wealth management UAE'] },
    region: null, serviceHint: 'Foundation + Private Fund'
  }
];

function pickTodaysQueries() {
  // Run all buckets every day. With 8 buckets * ~25 results, that's ~200 candidate profiles
  // before scoring — enough to survive the quality gate. Apify cost: 8 * $0.10 = $0.80/run.
  return QUERY_BUCKETS.slice();
}

const SERVICES = [
  { name: 'Discretionary Portfolio Management', desc: 'Five CMA-approved models from capital preservation through aggressive. USD 1M minimum mandate.', solution: 'Discretionary mandate on a CMA-approved model portfolio matched to the client risk profile and return objectives.' },
  { name: 'Foundation + Private Fund', desc: 'UAE Foundation owning a CMA Private Fund. Three-level control. UBO privacy. 10-day formation.', solution: 'UAE Foundation owning a CMA Private Fund. Only FCIM and the regulator know the UBO.' },
  { name: 'CMA Private Fund (standalone)', desc: 'Regulated UAE private fund. No restriction on asset type. Fast-track 10 working day setup.', solution: 'Standalone CMA Private Fund. No restrictions on asset class - public/private equity, real estate, commodities.' },
  { name: 'Commodity Derivatives', desc: 'SCA-licensed direct CME / ICE / LME / EEX / SGX access without a clearing account.', solution: 'SCA-licensed commodity derivatives platform. Direct CME, ICE, LME, EEX, SGX access without clearing account.' },
  { name: 'Fund Administration', desc: 'One of only five UAE-authorised fund administrators. In-house or third-party funds.', solution: 'Fund administration for in-house or third-party funds, UAE or foreign-domiciled.' },
  { name: 'IB & Advisory', desc: 'Dmitri Tchekalkine-led desk. ECM, DCM, M&A, listings on UAE exchanges.', solution: 'Investment banking and advisory. IPO / bond / sukuk issuance, UAE exchange listings, M&A.' },
  { name: 'Family Office Advisory', desc: 'Governance, succession, estate planning, concierge, VC/PE direct deals.', solution: 'Full family office build: governance, multi-generational succession, estate planning, concierge, VC/PE access.' },
  { name: 'EAM / FI Platform', desc: 'Confidential Client Money accounts at FAB and ENBD. Secondary custodianship for EAMs and FIs.', solution: 'Platform for EAMs and FIs. Confidential Client Money accounts at FAB and ENBD with FCIM as secondary custodian.' }
];

const PROBLEMS = [
  { id: 'international-asset-holding', label: 'Holding international assets under sovereign / banking pressure',
    signals: [/\b(international|cross[- ]border|european|offshore)\s+(asset|real estate|holding|property)/i, /\b(sovereign|sanction|banking|payment)\s+(risk|pressure|challenge|difficulty)/i, /\b(re-?structure|re-?domicile|consolidate)\s+(holdings|assets|portfolio)/i],
    fcimService: 'Foundation + Private Fund', angle: 'International asset-holding and banking-access pressure - same pattern FCIM solves with the Foundation + Private Fund stack.' },
  { id: 'ubo-privacy', label: 'UBO privacy / anonymity in acquisitions',
    signals: [/\b(confidential|anonymity|private)\s+(acquisition|investment|deal|transaction)/i, /\b(UBO|beneficial owner|disclosure)\b/i, /\b(undisclosed|private|sensitive)\s+(stake|investment|holding)/i],
    fcimService: 'Foundation + Private Fund', angle: 'UBO privacy in acquisitions - same structure FCIM uses to keep beneficial ownership disclosed only to FCIM and the regulator.' },
  { id: 'multi-venture-structuring', label: 'Founder with multiple ventures needing structured holdco',
    signals: [/\b(multiple|portfolio of|several)\s+(ventures|companies|businesses|investments)/i, /\b(serial|repeat)\s+(entrepreneur|founder|investor)/i, /\b(investment\s+holding|holding\s+company|family\s+holding)\b/i, /\b(group\s+chairman|group\s+CEO|founder\s+&\s+chairman)\b/i],
    fcimService: 'Foundation + Private Fund', angle: 'Multiple ventures held under personal name or scattered SPVs - Foundation + Private Fund consolidates under one regulated wrapper.' },
  { id: 'family-succession', label: 'Family succession / generational wealth transition',
    signals: [/\b(next generation|second generation|third generation|2G|3G)\b/i, /\b(succession|legacy|inheritance|estate)\s+(planning|transition|strategy)/i, /\b(family\s+(business|enterprise|legacy|trust|council))\b/i],
    fcimService: 'Family Office Advisory', angle: 'Family succession and generational transition - full family-office build covering governance, succession, estate, concierge.' },
  { id: 'commodity-hedging', label: 'Physical commodity exposure without hedging infrastructure',
    signals: [/\b(physical\s+(commodity|trader|trading))\b/i, /\b(grain|fertili[sz]er|freight|metals|energy|oilseed|sugar|cocoa|coffee|cotton)\s+(trad|import|export)/i, /\b(hedging|risk\s+management)\b.*\b(commodity|commodities|metals|energy|grain)/i, /\b(import|export)\s+(business|operations|trader)\b/i],
    fcimService: 'Commodity Derivatives', angle: 'Physical commodity exposure without exchange-cleared hedging - FCIM SCA-licensed platform gives direct CME/ICE/LME/EEX/SGX access without clearing account.' },
  { id: 'eam-platform-need', label: 'EAM / boutique wealth manager looking for client-money platform',
    signals: [/\b(external\s+asset\s+manager|EAM)\b/i, /\b(independent\s+(wealth|financial)\s+(manager|advisor))\b/i, /\b(boutique|managing\s+partner).*\b(wealth|advisory|asset\s+management)/i, /\b(family\s+wealth\s+advisor|multi-?family\s+office\s+founder)\b/i],
    fcimService: 'EAM / FI Platform', angle: 'EAM looking for a regulated platform - FCIM provides Confidential Client Money accounts at FAB and ENBD with FCIM as secondary custodian.' },
  { id: 'fund-launch-or-admin', label: 'Fund launching or needing admin upgrade',
    signals: [/\b(launching|launched|new)\s+(fund|vehicle)/i, /\b(general\s+partner|GP\s+at|fund\s+manager)\b/i, /\b(fund\s+(admin|administrator|administration|services))\b/i, /\b(NAV|fund\s+accounting|fund\s+operations)\b/i],
    fcimService: 'Fund Administration', angle: 'Fund launch or admin pain - FCIM is one of only five UAE-authorised fund administrators.' },
  { id: 'pre-ipo-or-ma', label: 'Pre-IPO or M&A advisory candidate',
    signals: [/\b(pre-?IPO|going\s+public|listing\s+plans)\b/i, /\b(M&A|mergers|acquisitions)\s+(advisor|target|strategy)/i, /\b(capital\s+raise|growth\s+equity|series\s+[CDE])\b/i, /\b(corporate\s+finance|capital\s+markets)\s+(director|head|managing)/i],
    fcimService: 'IB & Advisory', angle: 'Capital-markets activity ahead - FCIM IB desk (Dmitri Tchekalkine, 30+ years EM banking) covers ECM, DCM, M&A, UAE listings.' },
  { id: 'discretionary-mandate', label: 'HNW with liquid capital seeking discretionary mandate',
    signals: [/\b(post-?exit|exited|sold\s+(my|the)\s+(company|business))\b/i, /\b(personal\s+investment\s+company|PIC)\b/i, /\b(family\s+wealth|personal\s+wealth|liquid\s+capital)\b/i, /\b(HNW|UHNW|high\s+net\s+worth)\b/i],
    fcimService: 'Discretionary Portfolio Management', angle: 'Liquid capital seeking managed mandate - five CMA-approved model portfolios, USD 1M minimum.' },
  { id: 'sensitive-jurisdiction-banking', label: 'Banking access difficulty due to sensitive nationality / jurisdiction',
    signals: [/\b(Russian|Belarusian|Iranian|Syrian)\b/i, /\b(sanction|de-?risk|banking\s+access)\b/i, /\b(re-?domicile|relocation|UAE\s+residency)\b/i],
    fcimService: 'Foundation + Private Fund', angle: 'Banking friction due to nationality/jurisdiction - UAE-regulated Foundation + Private Fund stack restores banking access while preserving privacy.' }
];

const REGIONS = [
  { name: 'MENA / Levant', lead: 'Amr Fergany', warmPath: 'Amr Fergany via Credit Suisse DIFC alumni and Levantine professional networks in Dubai.' },
  { name: 'Egypt', lead: 'Ibrahim Hemeida', warmPath: 'Ibrahim Hemeida via the Egyptian Business Council Dubai and his Egyptian banking and UBP relationships.' },
  { name: 'Russia / CIS', lead: 'Dmitri Tchekalkine', warmPath: 'Dmitri Tchekalkine via 30+ years of EM banking relationships at Chemical Bank, Bear Stearns, and CIS-focused institutions.' },
  { name: 'Caucasus / CIS', lead: 'Dmitri Tchekalkine', warmPath: 'Dmitri Tchekalkine via Caucasus and CIS banking relationships built across decades of EM coverage.' },
  { name: 'India', lead: 'Saran Sankar', warmPath: 'Saran Sankar via UBS Investment Bank alumni and Indian Business & Professional Council Dubai.' },
  { name: 'Africa', lead: 'Ibrahim Hemeida', warmPath: 'Ibrahim Hemeida via Dubai-based African diaspora professional networks and pan-African banking relationships.' },
  { name: 'UK / Western', lead: 'Steven Downey', warmPath: 'Steven Downey via London Business School alumni and CFA Society UAE.' }
];

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
const COMPLIANCE_WARN = /\b(PEP|politically\s+exposed|state[- ]owned|sovereign\s+wealth|minister|ambassador)\b/i;

async function checkApifyAccount() {
  // Diagnostic: hit /v2/users/me to verify token is valid and check account state.
  try {
    const res = await fetch(`https://api.apify.com/v2/users/me?token=${APIFY_TOKEN}`);
    if (!res.ok) {
      console.warn(`Apify account check FAILED: HTTP ${res.status}. Token may be invalid/expired.`);
      const t = await res.text();
      console.warn(`Response: ${t.slice(0, 400)}`);
      return false;
    }
    const data = await res.json();
    const u = data.data || {};
    console.log(`Apify account: username=${u.username || '?'} plan=${u.plan || '?'} email=${u.email ? 'set' : 'unset'}`);
    return true;
  } catch (e) {
    console.warn(`Apify account check threw: ${e.message}`);
    return false;
  }
}

async function callApifyActor(actorId, bucket, bodyOverride) {
  const url = `https://api.apify.com/v2/acts/${actorId}/run-sync-get-dataset-items?token=${APIFY_TOKEN}`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 280000);
  const body = bodyOverride || bucket.body;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: ctrl.signal
    });
    clearTimeout(timer);
    if (!res.ok) {
      const text = await res.text();
      console.warn(`Apify[${actorId}] "${bucket.label}": HTTP ${res.status} - ${text.slice(0, 400)}`);
      return { ok: false, profiles: [], status: res.status, body: text.slice(0, 400) };
    }
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); }
    catch (e) {
      console.warn(`Apify[${actorId}] "${bucket.label}": JSON parse failed. Body: ${text.slice(0, 300)}`);
      return { ok: false, profiles: [], status: 200, body: text.slice(0, 300) };
    }
    if (!Array.isArray(data)) {
      console.warn(`Apify[${actorId}] "${bucket.label}": non-array response: ${JSON.stringify(data).slice(0, 300)}`);
      return { ok: false, profiles: [], status: 200, body: JSON.stringify(data).slice(0, 300) };
    }
    return { ok: true, profiles: data, status: 200 };
  } catch (e) {
    clearTimeout(timer);
    console.warn(`Apify[${actorId}] "${bucket.label}" threw: ${e.message}`);
    return { ok: false, profiles: [], status: 0, body: e.message };
  }
}

async function runApifyActor(bucket) {
  // Use linkedin-profile-search-by-services — this is the actor your account has access to.
  // It takes a queries[] array of natural-language search strings (NOT keywords).
  const body = {
    profileScraperMode: 'Full',
    queries: bucket.body.queries,
    maxItems: 25
  };
  const result = await callApifyActor(APIFY_ACTOR_FALLBACK, bucket, body);
  if (result.profiles.length > 0) {
    console.log(`Apify "${bucket.label}": ${result.profiles.length} profiles`);
    return result.profiles.map(p => ({ ...p, _queryRegion: bucket.region, _queryLabel: bucket.label, _queryServiceHint: bucket.serviceHint }));
  }
  console.warn(`Apify "${bucket.label}": empty. status=${result.status} body=${result.body || 'none'}`);
  return [];
}

function normaliseProfile(raw) {
  // Handle both actor schemas: linkedin-profile-search returns firstName/lastName/headline/currentPosition[0].companyName,
  // linkedin-profile-search-by-services returns name/position/summary/services (no currentPosition array, no separate company field).
  const fullName = (raw.firstName || raw.lastName)
    ? `${raw.firstName || ''} ${raw.lastName || ''}`.trim()
    : (raw.fullName || raw.name || 'Name unavailable');

  // Company: try multiple shapes
  let company = '';
  if (Array.isArray(raw.currentPosition) && raw.currentPosition[0]) {
    company = raw.currentPosition[0].companyName || raw.currentPosition[0].company || '';
  }
  if (!company) company = raw.currentCompany || raw.company || '';
  // Fallback actor often embeds "@ Company" inside position/headline string — try to extract.
  if (!company) {
    const titleStr = raw.position || raw.headline || raw.title || '';
    const atMatch = titleStr.match(/\s+(?:at|@)\s+(.+?)(?:[|·•]|$)/i);
    if (atMatch) company = atMatch[1].trim();
  }

  // Title
  const title = raw.headline || raw.title || raw.position || '';

  // About / scoring text — concat summary, about, services array, topSkills so scoring has signal.
  const aboutParts = [];
  if (raw.about) aboutParts.push(raw.about);
  if (raw.summary) aboutParts.push(raw.summary);
  if (Array.isArray(raw.services)) aboutParts.push(raw.services.map(s => typeof s === 'string' ? s : (s.name || s.title || '')).join(' '));
  if (raw.topSkills) aboutParts.push(raw.topSkills);
  const about = aboutParts.filter(Boolean).join(' \n ');

  // Location
  const locText = (raw.location && typeof raw.location === 'object')
    ? (raw.location.linkedinText || (raw.location.parsed && raw.location.parsed.text) || 'Dubai')
    : (raw.location || 'Dubai');

  // Slug / URL
  const slug = raw.publicIdentifier && !/^AC[oOwW]A/.test(raw.publicIdentifier) ? raw.publicIdentifier : null;
  const linkedinUrl = slug
    ? `https://www.linkedin.com/in/${slug}`
    : (raw.linkedinUrl || raw.linkedinProfileUrl || raw.profileUrl || raw.url || '');

  // Email — fallback actor may put emails in an array
  let email = raw.email || raw.emailAddress || null;
  if (!email && Array.isArray(raw.emails) && raw.emails.length > 0) {
    email = typeof raw.emails[0] === 'string' ? raw.emails[0] : (raw.emails[0].email || raw.emails[0].address || null);
  }

  return {
    firstName: raw.firstName || (fullName.split(' ')[0] || ''),
    lastName: raw.lastName || (fullName.split(' ').slice(1).join(' ') || ''),
    name: fullName, title, company, location: locText,
    linkedinUrl, publicIdentifier: slug || null,
    email, emailScore: null,
    emailVerified: !!email,
    about,
    _queryRegion: raw._queryRegion, _queryLabel: raw._queryLabel, _queryServiceHint: raw._queryServiceHint
  };
}

function scoreProfile(p) {
  const text = `${p.title} ${p.company} ${p.about}`.toLowerCase();
  let score = 0; const reasons = [];
  if (/\b(family\s+office|single\s+family\s+office|multi[- ]family\s+office)\b/i.test(text)) { score += 6; reasons.push('+6 family office'); }
  if (/\b(private\s+banker|wealth\s+manager|wealth\s+advisor|private\s+banking)\b/i.test(text)) { score += 5; reasons.push('+5 wealth/PB title'); }
  if (/\b(managing\s+partner|managing\s+director|founding\s+partner|general\s+partner)\b/i.test(text)) { score += 4; reasons.push('+4 senior partner'); }
  // Strict: only match full investment-CIO phrase, or bare 'CIO' WITH another wealth/finance keyword nearby.
  if (/\b(chief\s+investment\s+officer|head\s+of\s+investment)\b/i.test(text)) {
    score += 5; reasons.push('+5 CIO');
  } else if (/\bCIO\b/i.test(text) && /\b(wealth|asset|fund|portfolio|investment|family\s+office|capital|private\s+bank)/i.test(text)) {
    score += 5; reasons.push('+5 CIO (with finance context)');
  }
  // Penalty: if profile has 'CIO' but ALSO IT context, it's a Chief Information Officer.
  if (/\bCIO\b/i.test(text) && /\b(information\s+technology|IT\s+infrastructure|ERP|cloud\s+migration|cybersecurity|software\s+engineering|digital\s+transformation)\b/i.test(text)) {
    score -= 6; reasons.push('-6 IT-CIO not investment');
  }
  if (/\b(fund\s+manager|portfolio\s+manager|hedge\s+fund|private\s+equity|venture\s+capital)\b/i.test(text)) { score += 4; reasons.push('+4 fund/PM'); }
  if (/\b(external\s+asset\s+manager|EAM|independent\s+wealth)\b/i.test(text)) { score += 5; reasons.push('+5 EAM'); }
  if (/\b(AED|USD|EUR)\s*\d+\s*(million|billion|mn|bn)\b/i.test(text) || /\b\$\s*\d+\s*(million|billion|mn|bn|m|b)\b/i.test(text)) { score += 3; reasons.push('+3 monetary scale'); }
  if (/\b(assets\s+under\s+management|AUM|managed.*portfolio)/i.test(text)) { score += 3; reasons.push('+3 AUM'); }
  if (/\b(multiple\s+(ventures|businesses|companies)|portfolio\s+of\s+companies|investment\s+holding)/i.test(text)) { score += 4; reasons.push('+4 multi-venture'); }
  if (/\b(physical\s+commod|commodity\s+trad|grain|fertili[sz]er|freight|metals\s+trad|energy\s+trad)/i.test(text)) { score += 4; reasons.push('+4 commodity'); }
  if (/\b(d2c|direct[- ]to[- ]consumer|e-?commerce|amazon\s+seller|shopify)/i.test(text)) { score -= 8; reasons.push('-8 D2C/e-commerce'); }
  if (/\b(retail|hospitality|restaurant|cafe|f&b)\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund)\b/i.test(text)) { score -= 6; reasons.push('-6 retail w/o finance'); }
  // Penalize obvious non-finance industries that the keyword search may surface
  if (/\b(tile|ceramic|construction|export|shipping|freight|logistics\s+(company|firm))\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office)\b/i.test(text)) { score -= 8; reasons.push('-8 unrelated industry'); }
  if (/\b(real\s+estate\s+agent|broker|property\s+consultant)\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office)\b/i.test(text)) { score -= 6; reasons.push('-6 real estate sales'); }
  if (/\b(IT|software|developer|engineer|programmer)\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office|fintech)\b/i.test(text)) { score -= 6; reasons.push('-6 IT/dev role'); }
  if (/\b(student|intern|junior)\b/i.test(text)) { score -= 5; reasons.push('-5 junior'); }
  if (/\b(marketing|growth|content|social\s+media|HR|recruit)\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office)\b/i.test(text)) { score -= 4; reasons.push('-4 non-finance function'); }
  if (/\bfounder\b/i.test(text) && !/(family\s+office|investment|fund|capital|wealth|holding)/i.test(text)) { score -= 3; reasons.push('-3 generic founder'); }
  return { score, reasons };
}

function diagnoseProblem(p) {
  const text = `${p.title} ${p.company} ${p.about}`.toLowerCase();
  const matches = [];
  for (const prob of PROBLEMS) {
    let hits = 0;
    for (const sig of prob.signals) if (sig.test(text)) hits++;
    if (hits > 0) matches.push({ prob, hits });
  }
  matches.sort((a, b) => b.hits - a.hits);
  if (matches.length === 0) {
    const fallbackService = p._queryServiceHint || 'Discretionary Portfolio Management';
    return { problem: 'Profile lookup - no specific FCIM problem auto-detected. Manual review.', fcimService: fallbackService, angle: `Inferred from search bucket "${p._queryLabel || 'general'}". Worth manual scan.`, confidence: 'low' };
  }
  const top = matches[0];
  return { problem: top.prob.label, fcimService: top.prob.fcimService, angle: top.prob.angle, confidence: top.hits >= 2 ? 'high' : 'medium', secondary: matches.slice(1, 3).map(m => m.prob.label) };
}

async function findEmailViaHunter(p) {
  if (!HUNTER_API_KEY) return null;
  if (!p.firstName || !p.lastName || !p.company) return null;
  const params = new URLSearchParams({ company: p.company, first_name: p.firstName, last_name: p.lastName, api_key: HUNTER_API_KEY });
  try {
    const res = await fetch(`https://api.hunter.io/v2/email-finder?${params.toString()}`);
    if (!res.ok) { console.warn(`Hunter ${p.name}: HTTP ${res.status}`); return null; }
    const data = await res.json();
    const d = data && data.data;
    if (d && d.email) return { email: d.email, score: d.score || null, verificationStatus: (d.verification && d.verification.status) || null, domain: d.domain || null };
    return null;
  } catch (e) { console.warn(`Hunter ${p.name} failed: ${e.message}`); return null; }
}

function guessEmailPatterns(p) {
  if (!p.firstName || !p.company) return [];
  const cleaned = p.company.toLowerCase().replace(/\b(llc|ltd|limited|gmbh|sa|sarl|inc|fzc|fze|dmcc|llp|plc|holdings?|group|capital)\b/g, '').replace(/[^a-z0-9]+/g, '').trim();
  if (!cleaned) return [];
  const domains = [`${cleaned}.com`, `${cleaned}.ae`, `${cleaned}.io`];
  const f = p.firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = (p.lastName || '').toLowerCase().replace(/[^a-z]/g, '');
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
  const params = new URLSearchParams({ first_name: p.firstName, last_name: p.lastName || '', company: p.company || '' });
  return `https://hunter.io/email-finder?${params.toString()}`;
}

function runCompliance(p) {
  const text = `${p.name} ${p.company} ${p.about}`;
  for (const re of COMPLIANCE_BLOCK) if (re.test(text)) return { allowed: false };
  return { allowed: true, pep: COMPLIANCE_WARN.test(text) };
}

function fingerprint(p) {
  // Prefer LinkedIn ID when available — it's globally unique. Falls back to name+company.
  if (p.publicIdentifier) return `li:${p.publicIdentifier.toLowerCase()}`;
  if (p.linkedinUrl) return `url:${p.linkedinUrl.toLowerCase().replace(/[/?#].*$/, '')}`;
  const n = (p.name || '').toLowerCase().trim();
  const c = (p.company || '').toLowerCase().trim();
  if (!n) return '';
  if (!c) return `n:${n}`;  // tag-prefix so name-only fingerprints don't collide with name+company ones
  return `nc:${n}|${c}`;
}

function decodeEntities(s) {
  return String(s == null ? '' : s).replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&#x27;/g, "'").replace(/&nbsp;/g, ' ');
}

function escapeHtml(s) {
  return decodeEntities(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
}

function routeRegion(p) {
  if (p._queryRegion) return p._queryRegion;
  const text = `${p.name} ${p.title} ${p.company} ${p.about} ${p.location}`.toLowerCase();
  if (/\b(armen|azerb|georgi[a]|tbilisi|yerevan|baku)\b/i.test(text)) return 'Caucasus / CIS';
  if (/\b(russia|russian|moscow|st\.?\s*petersburg|kazakh|belarus|uzbek|ukrain)\b/i.test(text)) return 'Russia / CIS';
  if (/\b(egypt|cairo|alexandria)\b/i.test(text)) return 'Egypt';
  if (/\b(lebanon|lebanese|beirut|jordan|syria|levant|moroc|tunis|algeri)\b/i.test(text)) return 'MENA / Levant';
  if (/\b(india|indian|mumbai|delhi|bangalore|chennai|gurgaon)\b/i.test(text)) return 'India';
  if (/\b(nigeri|kenya|south\s+africa|ghana|senegal|ethiopia|tanzania|uganda|ivory\s+coast|cote)\b/i.test(text)) return 'Africa';
  if (/\b(london|england|britain|british|uk\b|scotland|german|swiss|switzerland)\b/i.test(text)) return 'UK / Western';
  return null;
}

function buildDraftPrompt(p) {
  const dx = p.diagnosis || {};
  const emailLine = p.emailVerified ? `Verified email: ${p.email}` : (p.emailGuesses && p.emailGuesses.length ? `Email (best guess, unverified): ${p.emailGuesses[0]}. Other patterns: ${p.emailGuesses.slice(1).join(', ')}` : 'Email: not found - use LinkedIn InMail.');
  return `Draft Yehya Abdelbaki FCIM outreach. The diagnostic agent has matched a specific FCIM problem.

PROSPECT
Name: ${p.name}
Title: ${p.title}
Company: ${p.company}
Location: ${p.location}
LinkedIn: ${p.linkedinUrl}
${emailLine}

DIAGNOSED PROBLEM
${dx.problem || '(manual review)'}

FCIM SOLUTION (anchor the email here)
${dx.angle || 'Tailor based on profile.'}
Matched service: ${dx.fcimService || '(pick best fit)'}

REGION & WARM PATH
Region: ${p.region || '(not detected)'}
Suggested lead: ${p.regionLead || '(route by context)'}
Warm path: ${p.regionWarmPath || '(route by context)'}

WRITING RULES
- Under 200 words.
- Open with a specific, non-glazing reference to their actual situation.
- Lead with the diagnosed problem framed as observation, not assumption.
- Position FCIM solution clearly but not pitchy. Reference real specifics ($6B AUM, CMA-licensed, SCA-licensed) where relevant.
- Reference the warm-path FCIM colleague naturally only if it strengthens credibility.
- Do not glaze. Do not be over-eager. Do not pitch every service.
- NEVER mention Gary Dugan.
- Subject line: specific, attention-grabbing, not promotional, not cringe.
- End with full Yehya signature: Yehya Abdelbaki / Relationship Manager / Fundament Capital Investment Management / Office 1511, The Binary Tower, Business Bay, Dubai.`;
}

function prospectCardHtml(p, isFeatured) {
  const dx = p.diagnosis || {};
  const service = SERVICES.find(s => s.name === dx.fcimService);
  const solutionText = service ? service.solution : 'Service match indeterminate - review profile manually.';
  const complianceBlock = p.pep ? `<div class="compliance pep"><span class="label">Elevated DD</span>PEP / state-linked exposure detected. Run enhanced due diligence before outreach.</div>` : '';
  let emailLine;
  if (p.emailVerified && p.email) {
    const score = p.emailScore ? ` <em>(Hunter score: ${p.emailScore})</em>` : '';
    emailLine = `<strong>Verified:</strong> <a href="mailto:${escapeHtml(p.email)}">${escapeHtml(p.email)}</a>${score}`;
  } else if (p.emailGuesses && p.emailGuesses.length) {
    const link = p.hunterVerifyLink ? ` <a href="${escapeHtml(p.hunterVerifyLink)}" target="_blank">verify in Hunter</a>` : '';
    emailLine = `<em>Best guess (unverified):</em> ${escapeHtml(p.emailGuesses[0])}${link}<br><small>Other patterns: ${p.emailGuesses.slice(1).map(escapeHtml).join(', ')}</small>`;
  } else {
    emailLine = `<em>not found - use LinkedIn InMail</em>`;
  }
  const conf = dx.confidence ? `<span style="font-size:10px; letter-spacing:0.15em; text-transform:uppercase; padding:2px 6px; border:1px solid #C9B458; margin-left:8px;">${escapeHtml(dx.confidence)}</span>` : '';
  const problemBlock = dx.problem ? `<div class="section" style="background:#F6EFD8; padding:12px 14px; border-left:3px solid #C9B458;"><div class="label">Diagnosed problem ${conf}</div><p style="margin-top:6px;"><strong>${escapeHtml(dx.problem)}</strong></p><p style="margin-top:8px; font-size:13px; color:#3C3730;">${escapeHtml(dx.angle || '')}</p></div>` : '';
  const prompt = buildDraftPrompt(p);
  return `
    <article class="prospect ${isFeatured ? 'featured' : ''}">
      <div class="service-tag">${escapeHtml(dx.fcimService || 'Manual review')}</div>
      <div class="head-row">
        <div class="head-main">
          <h3>${escapeHtml(p.name)}</h3>
          <div class="sub">${escapeHtml(p.title)}${p.company ? `<span class="dot">\u00b7</span>${escapeHtml(p.company)}` : ''}</div>
        </div>
        ${p.regionLead ? `<div class="lead-block"><span class="lead-label">Lead</span>${escapeHtml(p.regionLead)}</div>` : ''}
      </div>
      ${problemBlock}
      <div class="section"><div class="label">Contact</div><p>
        <strong>LinkedIn:</strong> ${p.linkedinUrl ? `<a href="${escapeHtml(p.linkedinUrl)}" target="_blank">${escapeHtml(p.linkedinUrl)}</a>` : '<em>not available</em>'}<br>
        <strong>Email:</strong> ${emailLine}
      </p></div>
      ${p.about ? `<div class="section"><div class="label">Background</div><p>${escapeHtml(p.about)}</p></div>` : ''}
      <div class="section"><div class="label">FCIM solution</div><p>${escapeHtml(solutionText)}</p></div>
      <div class="section"><div class="label">Warm path</div><p>${escapeHtml(p.regionWarmPath || 'Region indeterminate. Route to best-matched FCIM colleague.')}</p></div>
      ${complianceBlock}
      <div class="first-step"><div class="label">First step</div>
        <p>Approach ${escapeHtml(p.name)} via ${p.regionLead ? escapeHtml(p.regionLead) + '\u2019s warm path' : 'best-matched FCIM colleague'}.</p>
        <button class="draft-btn" data-prompt="${escapeHtml(prompt)}">Copy draft prompt</button>
      </div>
    </article>`;
}

function renderServiceSection(svc, items) {
  if (items.length === 0) {
    return `<section class="service-section empty"><div class="service-header"><div class="left"><h2>${escapeHtml(svc.name)}</h2><p>${escapeHtml(svc.desc)}</p></div><div class="right-meta">No prospects today</div></div></section>`;
  }
  const byRegion = {};
  for (const p of items) {
    const key = p.region || '__unrouted';
    if (!byRegion[key]) byRegion[key] = [];
    byRegion[key].push(p);
  }
  const regionOrder = REGIONS.map(r => r.name).filter(n => byRegion[n]);
  if (byRegion.__unrouted) regionOrder.push('__unrouted');
  const REGION_BLOCK = 'margin: 28px 0 8px; padding: 14px 18px; background: var(--cream-yellow, #F6EFD8); border-left: 3px solid #C9B458;';
  const REGION_ROW = 'display:flex; justify-content:space-between; align-items:baseline; gap:12px;';
  const REGION_NAME = "font-family: 'Fraunces', ui-serif, Georgia, serif; font-style: italic; font-size: 22px; margin: 0;";
  const REGION_META = 'font-size: 11px; letter-spacing: 0.14em; text-transform: uppercase; color: #5a503e;';
  const blocks = regionOrder.map(rname => {
    const ps = byRegion[rname];
    const r = REGIONS.find(rr => rr.name === rname);
    const name = (rname === '__unrouted') ? 'Unrouted' : rname;
    const lead = (rname === '__unrouted') ? 'Route by context' : (r && r.lead ? r.lead : 'Lead');
    return `<div class="region-block" style="${REGION_BLOCK}"><div style="${REGION_ROW}"><h3 style="${REGION_NAME}">${escapeHtml(name)}</h3><div style="${REGION_META}">${escapeHtml(lead)} \u00b7 ${ps.length} prospect${ps.length === 1 ? '' : 's'}</div></div></div><div class="items">${ps.map(p => prospectCardHtml(p, false)).join('')}</div>`;
  }).join('');
  return `<section class="service-section"><div class="service-header"><div class="left"><h2>${escapeHtml(svc.name)}</h2><p>${escapeHtml(svc.desc)}</p></div><div class="right-meta">${items.length} prospect${items.length === 1 ? '' : 's'} \u00b7 today</div></div>${blocks}</section>`;
}

function renderRegionChips(regionCounts) {
  return REGIONS.map(r => {
    const n = regionCounts[r.name] || 0;
    const empty = n === 0;
    const sep = '<span aria-hidden="true" style="opacity:0.4; margin:0 4px;">\u00b7</span>';
    return `<button class="region-chip ${empty ? 'empty' : ''}" ${empty ? 'disabled' : `data-region="${escapeHtml(r.name)}"`}><span>${escapeHtml(r.name)}</span>${sep}<span class="count">${n}</span>${r.lead ? `${sep}<span class="lead">${escapeHtml(r.lead)}</span>` : ''}</button>`;
  }).join('');
}

async function main() {
  console.log('FCIM Daily Build v7 - starting');
  await checkApifyAccount();
  const buckets = pickTodaysQueries();
  console.log(`Today's buckets: ${buckets.map(b => b.label).join(' | ')}`);
  // Sequential to avoid Apify concurrent-run limits and 429 rate-limiting on lower tiers.
  const results = [];
  for (const b of buckets) {
    const r = await runApifyActor(b);
    results.push(r);
  }
  const raw = results.flat();
  console.log(`Raw profiles: ${raw.length}`);
  if (raw.length > 0) console.log(`Sample profile keys: ${Object.keys(raw[0]).slice(0, 25).join(', ')}`);
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
    if (!fp || seen.has(fp)) return false;
    seen.add(fp);
    return true;
  });
  console.log(`After compliance+dedupe: ${profiles.length} (blocked: ${blocked})`);
  const beforeGate = profiles.length;
  // Diagnostic: log first profile's scoring text + score so we can see why things drop.
  if (profiles.length > 0) {
    const sample = profiles[0];
    const s0 = scoreProfile(sample);
    console.log(`Sample profile: name="${sample.name}" title="${sample.title}" company="${sample.company}"`);
    console.log(`Sample about (first 200 chars): ${(sample.about || '').slice(0, 200)}`);
    console.log(`Sample score: ${s0.score} reasons: ${s0.reasons.join(', ') || '(none)'}`);
  }
  profiles = profiles.map(p => { const s = scoreProfile(p); p._score = s.score; p._scoreReasons = s.reasons; return p; }).filter(p => p._score >= QUALITY_THRESHOLD).sort((a, b) => b._score - a._score);
  console.log(`After quality gate (>= ${QUALITY_THRESHOLD}): ${profiles.length} (dropped ${beforeGate - profiles.length})`);
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
    // Skip prospects we can't usefully query — don't burn the budget on no-ops.
    if (!p.firstName || !p.lastName || !p.company) continue;
    const result = await findEmailViaHunter(p);
    hunterCalls++;
    if (result && result.email) {
      p.email = result.email;
      p.emailScore = result.score;
      p.emailVerified = result.verificationStatus === 'valid' || (result.score && result.score >= 70);
    }
  }
  console.log(`Hunter calls used: ${hunterCalls} (cap ${MAX_HUNTER_CALLS_PER_RUN})`);
  for (const p of profiles) {
    if (!p.emailVerified) {
      p.emailGuesses = guessEmailPatterns(p);
      p.hunterVerifyLink = hunterVerifyLink(p);
    }
  }
  const featured = profiles.find(p => p.diagnosis && p.diagnosis.confidence === 'high' && !p.pep) || profiles[0] || null;
  const remaining = featured ? profiles.filter(p => fingerprint(p) !== fingerprint(featured)) : profiles;
  let servicesHtml = ''; let emptyServicesHtml = '';
  for (const svc of SERVICES) {
    const items = remaining.filter(p => p.diagnosis && p.diagnosis.fcimService === svc.name);
    const html = renderServiceSection(svc, items);
    if (items.length === 0) emptyServicesHtml += html; else servicesHtml += html;
  }
  const regionCounts = {};
  REGIONS.forEach(r => regionCounts[r.name] = 0);
  profiles.forEach(p => { if (p.region && regionCounts[p.region] !== undefined) regionCounts[p.region]++; });
  const dateStamp = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Dubai', weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }).format(new Date());
  const verifiedCount = profiles.filter(p => p.emailVerified).length;
  const councilLine = featured ? `Council convened - ${profiles.length} qualified prospects, ${verifiedCount} with verified emails.` : `Council couldn't qualify any prospects today. ${beforeGate} pulled, ${beforeGate - profiles.length} dropped at the quality gate.`;
  let template;
  try {
    template = fs.readFileSync('index.template.html', 'utf-8');
  } catch (e) {
    console.error('FATAL: index.template.html not found in working directory.');
    console.error('Listing files in cwd:', fs.readdirSync('.').join(', '));
    process.exit(1);
  }
  // Function-form replacements so $&, $1, etc. in values are not interpreted as backreferences.
  const dateStr = escapeHtml(dateStamp);
  const builtAtStr = escapeHtml(new Date().toISOString());
  const councilStr = escapeHtml(councilLine);
  const chipsStr = renderRegionChips(regionCounts);
  const featuredStr = featured ? prospectCardHtml(featured, true) : '';
  const contentStr = servicesHtml + emptyServicesHtml;
  const wrapperStr = featured ? '' : 'display:none';
  const html = template
    .replace(/\{\{DATE\}\}/g, () => dateStr)
    .replace(/\{\{BUILT_AT\}\}/g, () => builtAtStr)
    .replace(/\{\{COUNCIL_LINE\}\}/g, () => councilStr)
    .replace(/\{\{REGION_CHIPS\}\}/g, () => chipsStr)
    .replace(/\{\{FEATURED\}\}/g, () => featuredStr)
    .replace(/\{\{CONTENT\}\}/g, () => contentStr)
    .replace(/\{\{FEATURED_WRAPPER_STYLE\}\}/g, () => wrapperStr);
  fs.writeFileSync('index.html', html);
  console.log(`Built index.html - ${profiles.length} qualified prospects, ${verifiedCount} with verified emails.`);
}

main().catch(err => { console.error(err); process.exit(1); });
