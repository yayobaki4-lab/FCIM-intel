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
const QUALITY_THRESHOLD = 1;
const MAX_HUNTER_CALLS_PER_RUN = 25;

const QUERY_BUCKETS = [
  // v10: aligned to FCIM's Corporate Profile 2026.
  // FCIM is "built for complexity" — three core specialties: Distressed & Special
  // Situations, Commodities & Natural Resources, Structuring (Funds & Transactions).
  // Wealth Management and IB&Advisory are ADDITIONAL capabilities, not core.
  // Therefore prospects are: people who BRING complex situations FCIM solves.
  // We DROP the major private banks (competitors, not clients) and instead target:
  //  - Restructuring/special-sit advisors at IBs (referral partners for distressed deals)
  //  - Physical commodity traders (direct clients for derivatives platform)
  //  - Fund admin & MFO operators (referral partners for structuring)
  //  - PE/VC sponsors (direct clients for fund formation)
  //  - Indian/Lebanese/Egyptian-origin family conglomerates (direct clients)
  //  - Boutique EAM/independent wealth shops (FI Platform clients)
  //  - Distressed/special-situations capital funds (direct clients)
  //  - Mid-market corporate development (IB & Advisory clients)
  {
    label: 'Restructuring & Special Situations Advisors',
    body: { companies: ['Houlihan Lokey', 'Rothschild & Co', 'Lazard', 'AlixPartners', 'FTI Consulting', 'Alvarez & Marsal', 'PJT Partners', 'Kroll'] },
    region: null, serviceHint: 'Distressed & Special Situations'
  },
  {
    label: 'Physical Commodity Traders',
    body: { companies: ['Trafigura', 'Vitol', 'Glencore', 'Mercuria', 'Gunvor', 'Cargill', 'Bunge', 'Olam International'] },
    region: null, serviceHint: 'Commodities & Natural Resources'
  },
  {
    label: 'Fund Admin & MFO Operators',
    body: { companies: ['Stonehage Fleming', 'Apex Group', 'IQ-EQ', 'Maitland Group', 'Hawksford', 'Vistra', 'TMF Group', 'Trident Trust'] },
    region: null, serviceHint: 'Structuring (Private Funds)'
  },
  {
    label: 'PE / VC Sponsors',
    body: { companies: ['Investcorp', 'Gulf Capital', 'NBK Capital Partners', 'Waha Capital', 'Mubadala Capital', 'Ardian', 'The Carlyle Group', 'TPG'] },
    region: null, serviceHint: 'Structuring (Private Funds)'
  },
  {
    label: 'Indian-Origin Family Conglomerates',
    body: { companies: ['Lulu Group International', 'Landmark Group', 'Apparel Group', 'Sharaf Group', 'Choithrams', 'GEMS Education', 'Aster DM Healthcare', 'Thumbay Group'] },
    region: null, serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Lebanese / Egyptian / Levantine Business',
    body: { companies: ['Sabbagh Holding', 'Joseph Group', 'Ezz Steel', 'Orascom Construction', 'Mansour Group', 'Hassan Allam Holding', 'BTI International', 'CCC Consolidated Contractors'] },
    region: null, serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Boutique EAM & Independent Wealth',
    body: { companies: ['Holborn Assets', 'Lighthouse Capital', 'Ocean Wall', 'Globaleye Wealth Management', 'Killik & Co', 'Quintet Private Bank', 'Stanhope Capital', 'LGT Vestra'] },
    region: null, serviceHint: 'EAM / FI Platform'
  },
  {
    label: 'Distressed Capital & Special Situations Funds',
    body: { companies: ['Davidson Kempner Capital Management', 'Värde Partners', 'Ares Management', 'Brevet Capital', 'Cerberus Capital Management', 'Sculptor Capital', 'Centerbridge Partners', 'Oaktree Capital Management'] },
    region: null, serviceHint: 'Distressed & Special Situations'
  }
];

function pickTodaysQueries() {
  // Run all buckets every day. With 8 buckets * ~25 results, that's ~200 candidate profiles
  // before scoring — enough to survive the quality gate. Apify cost: 8 * $0.10 = $0.80/run.
  return QUERY_BUCKETS.slice();
}

const SERVICES = [
  // Three CORE specialties (per Corporate Profile 2026)
  { name: 'Distressed & Special Situations', desc: 'FCIM core specialty. Disciplined underwriting and active engagement around distressed credit, recapitalizations, and workouts.', solution: 'Distressed and special situations expertise — disciplined underwriting, active engagement around recapitalizations, debt-for-equity, and discounted secondary acquisitions. We unlock value where traditional investors are constrained by scale, process, or risk appetite.' },
  { name: 'Commodities & Natural Resources', desc: 'FCIM core specialty. SCA-licensed direct access to CME, ICE, LME, EEX, SGX. Hedging, structured trade, supply-chain-linked equity strategies.', solution: 'SCA-licensed commodity derivatives platform with direct exchange access (CME, ICE, LME, EEX, SGX). Hedging across energy, metals, agri, freight, environmental products — without client maintaining own clearing relationships. Combined with physical-market insight and supply-chain-linked equity strategies.' },
  { name: 'Structuring (Private Funds)', desc: 'FCIM core specialty. CMA-approved private funds in 10 business days. Foundation + Private Fund stack for UBO privacy and founder control.', solution: 'CMA-approved private fund formation in 10 business days. Optional UAE Foundation as parent for UBO privacy (only FCIM and CMA know the UBO). 100% asset-class concentration permitted. Direct investor representation on fund board / Investment Committee.' },
  // Additional capabilities
  { name: 'Foundation + Private Fund', desc: 'UAE Foundation owning a CMA Private Fund. UBO privacy, founder control, 10-day formation.', solution: 'UAE Foundation owning a CMA Private Fund. Only FCIM and CMA know the UBO. Founder retains control via Charter and By-laws.' },
  { name: 'IB & Advisory', desc: 'Tim Almashat-led desk. ECM, DCM, M&A, valuations, regulatory advisory. $50M-$150M deals.', solution: 'Investment banking and advisory: ECM (IPOs, rights issues), DCM (bonds, sukuk), M&A end-to-end, valuations and feasibility, CMA/ADX/DFM regulatory advisory.' },
  { name: 'Family Office Advisory', desc: 'Governance, multi-generational succession, estate planning, concierge, VC/PE direct deals.', solution: 'Full family office build covering governance, succession, estate, concierge, and direct deal access. Cross-border execution with discretion.' },
  { name: 'EAM / FI Platform', desc: 'Confidential Client Money accounts at FAB. Platform for EAMs and FIs.', solution: 'EAM/FI platform with Confidential Client Money accounts at FAB. FCIM acts as secondary custodian; EAMs leverage platform for client mandates.' },
  { name: 'Discretionary Portfolio Management', desc: 'CMA-approved discretionary mandates for HNWIs, family offices, PICs, and institutions.', solution: 'Discretionary portfolio management with strategic asset allocation, manager selection, and risk-resilient execution. Customized to risk profile and time horizon.' }
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
    fcimService: 'Commodities & Natural Resources', angle: 'Physical commodity exposure without exchange-cleared hedging - FCIM SCA-licensed platform gives direct CME/ICE/LME/EEX/SGX access without clearing account.' },
  { id: 'eam-platform-need', label: 'EAM / boutique wealth manager looking for client-money platform',
    signals: [/\b(external\s+asset\s+manager|EAM)\b/i, /\b(independent\s+(wealth|financial)\s+(manager|advisor))\b/i, /\b(boutique|managing\s+partner).*\b(wealth|advisory|asset\s+management)/i, /\b(family\s+wealth\s+advisor|multi-?family\s+office\s+founder)\b/i],
    fcimService: 'EAM / FI Platform', angle: 'EAM looking for a regulated platform - FCIM provides Confidential Client Money accounts at FAB and ENBD with FCIM as secondary custodian.' },
  { id: 'fund-launch-or-admin', label: 'Fund launching or needing admin upgrade',
    signals: [/\b(launching|launched|new)\s+(fund|vehicle)/i, /\b(general\s+partner|GP\s+at|fund\s+manager)\b/i, /\b(fund\s+(admin|administrator|administration|services))\b/i, /\b(NAV|fund\s+accounting|fund\s+operations)\b/i],
    fcimService: 'Structuring (Private Funds)', angle: 'Fund launch or admin pain - FCIM is one of only five UAE-authorised fund administrators.' },
  { id: 'pre-ipo-or-ma', label: 'Pre-IPO or M&A advisory candidate',
    signals: [/\b(pre-?IPO|going\s+public|listing\s+plans)\b/i, /\b(M&A|mergers|acquisitions)\s+(advisor|target|strategy)/i, /\b(capital\s+raise|growth\s+equity|series\s+[CDE])\b/i, /\b(corporate\s+finance|capital\s+markets)\s+(director|head|managing)/i],
    fcimService: 'IB & Advisory', angle: 'Capital-markets activity ahead - FCIM IB desk (Dmitri Tchekalkine, 30+ years EM banking) covers ECM, DCM, M&A, UAE listings.' },
  { id: 'discretionary-mandate', label: 'HNW with liquid capital seeking discretionary mandate',
    signals: [/\b(post-?exit|exited|sold\s+(my|the)\s+(company|business))\b/i, /\b(personal\s+investment\s+company|PIC)\b/i, /\b(family\s+wealth|personal\s+wealth|liquid\s+capital)\b/i, /\b(HNW|UHNW|high\s+net\s+worth)\b/i],
    fcimService: 'Discretionary Portfolio Management', angle: 'Liquid capital seeking managed mandate - five CMA-approved model portfolios, USD 1M minimum.' },
  { id: 'sensitive-jurisdiction-banking', label: 'Banking access difficulty due to sensitive nationality / jurisdiction',
    signals: [/\b(Russian|Belarusian|Iranian|Syrian)\b/i, /\b(sanction|de-?risk|banking\s+access)\b/i, /\b(re-?domicile|relocation|UAE\s+residency)\b/i],
    fcimService: 'Foundation + Private Fund', angle: 'Banking friction due to nationality/jurisdiction - UAE-regulated Foundation + Private Fund stack restores banking access while preserving privacy.' },
  { id: 'distressed-special', label: 'Distressed credit / special situations sponsor',
    signals: [/\b(distressed|special\s+situations|turnaround|workout|restructuring)\b/i, /\b(debt-?for-?equity|recapitalization|bankruptcy|chapter\s+11)\b/i, /\b(secondary\s+(market|acquisition)|opportunistic\s+credit)\b/i],
    fcimService: 'Distressed & Special Situations', angle: 'Distressed and special situations expertise - FCIM specializes in disciplined underwriting, active engagement, and structuring around recapitalizations and workouts.' }
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
  /\bskybound\s+wealth\b/i,
  // v10: existing FCIM relationships / direct competitors — exclude from prospect list
  /\bjulius\s+baer\b/i,
  /\bEFG\s+(international|bank|hermes)\b/i
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
  // v9 strategy: scrape employees from KNOWN target firms (Julius Baer, UBS, etc).
  // This is far more reliable than keyword search because the firms definitely exist
  // and definitely employ wealth managers in Dubai.
  const body = {
    profileScraperMode: 'Short',
    currentCompanies: bucket.body.companies,
    locations: ['Dubai'],
    maxItems: 25
  };
  const result = await callApifyActor(APIFY_ACTOR_PRIMARY, bucket, body);
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

  // Company: try multiple shapes including v9 schema (currentPositions array, plural)
  let company = '';
  let titleFromPosition = '';
  // v9 schema: currentPositions = [{ companyName, title, ... }]
  if (Array.isArray(raw.currentPositions) && raw.currentPositions[0]) {
    company = raw.currentPositions[0].companyName || raw.currentPositions[0].company || raw.currentPositions[0].companyUrn || '';
    titleFromPosition = raw.currentPositions[0].title || raw.currentPositions[0].position || '';
  }
  // older schema: currentPosition (singular)
  if (!company && Array.isArray(raw.currentPosition) && raw.currentPosition[0]) {
    company = raw.currentPosition[0].companyName || raw.currentPosition[0].company || '';
  }
  if (!company) company = raw.currentCompany || raw.company || '';
  // Fallback: embedded "@ Company" inside position/headline string
  if (!company) {
    const titleStr = raw.position || raw.headline || raw.title || titleFromPosition || '';
    const atMatch = titleStr.match(/\s+(?:at|@)\s+(.+?)(?:[|·•]|$)/i);
    if (atMatch) company = atMatch[1].trim();
  }

  // Title — prefer headline, then v9 currentPositions[0].title, then legacy fields
  const title = raw.headline || titleFromPosition || raw.title || raw.position || '';

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

  // ============ FCIM CORE SPECIALTY MATCHES (high boost) ============
  // 1. Distressed & Special Situations
  if (/\b(distressed|special\s+situations?|turnaround|workout|restructuring|debt-?for-?equity|recapitalization)\b/i.test(text)) {
    score += 8; reasons.push('+8 distressed/special sits (CORE)');
  }
  if (/\b(special\s+situation\s+group|restructuring\s+(advisor|advisory|group)|capital\s+solutions)\b/i.test(text)) {
    score += 6; reasons.push('+6 restructuring practice');
  }
  // 2. Commodities & Natural Resources
  if (/\b(physical\s+commodity|commodity\s+(trader|trading)|energy\s+trading|metals\s+trading|grain\s+trader|fertili[sz]er\s+trader|crude\s+oil\s+trader|oil\s+trader|freight\s+trader|agri\s+(commod|trader))\b/i.test(text)) {
    score += 8; reasons.push('+8 commodity trader (CORE)');
  }
  if (/\b(hedging|hedge|risk\s+management).*\b(commodity|commodities|energy|metals|agri|freight)/i.test(text)) {
    score += 5; reasons.push('+5 hedging context');
  }
  // 3. Structuring (Private Funds)
  if (/\b(general\s+partner|GP\s+at|fund\s+sponsor|launching\s+(a\s+)?(private\s+)?fund|fund\s+formation|fund\s+structuring)\b/i.test(text)) {
    score += 7; reasons.push('+7 fund sponsor (CORE)');
  }
  if (/\b(fund\s+administration|fund\s+admin|MFO\s+operator|multi-?family\s+office\s+(director|head|principal))\b/i.test(text)) {
    score += 5; reasons.push('+5 fund admin / MFO operator');
  }

  // ============ ADDITIONAL CAPABILITY MATCHES (medium boost) ============
  if (/\b(family\s+office\s+(principal|founder|head|director))\b/i.test(text)) {
    score += 6; reasons.push('+6 family office principal');
  }
  if (/\b(group\s+chairman|chairman\s+of\s+the\s+board|family\s+business\s+chairman|managing\s+director\s+&?\s*founder)\b/i.test(text)) {
    score += 5; reasons.push('+5 chairman/founder');
  }
  if (/\b(holding\s+company|investment\s+holding|family\s+holding|group\s+holdings?)\b/i.test(text)) {
    score += 4; reasons.push('+4 holding co');
  }
  if (/\b(serial\s+entrepreneur|multiple\s+ventures|portfolio\s+of\s+companies)\b/i.test(text)) {
    score += 4; reasons.push('+4 multi-venture');
  }
  if (/\b(next\s+generation|second\s+generation|third\s+generation|2G|3G|family\s+council|family\s+enterprise)\b/i.test(text)) {
    score += 5; reasons.push('+5 succession');
  }
  if (/\b(succession|estate\s+planning|generational\s+(transition|wealth))\b/i.test(text)) {
    score += 4; reasons.push('+4 estate planning');
  }
  if (/\b(CFO|chief\s+financial\s+officer|finance\s+director|head\s+of\s+(finance|corporate\s+finance))\b/i.test(text) && /\b(corporate|industries|holdings|group|enterprise|company|midcap|mid-cap)\b/i.test(text)) {
    score += 5; reasons.push('+5 corporate CFO');
  }
  if (/\b(pre-?IPO|going\s+public|capital\s+raise|growth\s+equity|series\s+[CDE]|M&A\s+advisory|mergers\s+and\s+acquisitions)\b/i.test(text)) {
    score += 5; reasons.push('+5 capital markets activity');
  }
  if (/\b(external\s+asset\s+manager|EAM|independent\s+wealth)\b/i.test(text)) {
    score += 5; reasons.push('+5 EAM');
  }
  if (/\b(AED|USD|EUR)\s*\d+\s*(million|billion|mn|bn)\b/i.test(text) || /\b\$\s*\d+\s*(million|billion|mn|bn|m|b)\b/i.test(text)) {
    score += 3; reasons.push('+3 monetary scale');
  }
  if (/\b(assets\s+under\s+management|AUM|managed.*portfolio)/i.test(text)) {
    score += 3; reasons.push('+3 AUM');
  }

  // v10 trusted-firm boost — only for FCIM-target firms (NOT private banks)
  const targetFirms = /\b(houlihan\s+lokey|rothschild|lazard|alixpartners|FTI\s+consulting|alvarez|moelis|evercore|perella|PJT\s+partners|kroll|trafigura|vitol|glencore|mercuria|gunvor|cargill|bunge|olam|stonehage|maitland|apex\s+group|IQ-?EQ|hawksford|vistra|TMF|trident\s+trust|investcorp|gulf\s+capital|NBK\s+capital|waha|mubadala\s+capital|ardian|carlyle|TPG|davidson\s+kempner|v\u00e4rde|varde|ares\s+management|brevet|cerberus|sculptor|centerbridge|oaktree|lulu\s+group|landmark\s+group|apparel\s+group|sharaf|choithram|GEMS\s+education|aster|thumbay|sabbagh|joseph\s+group|ezz\s+steel|orascom|mansour\s+group|hassan\s+allam|BTI|CCC|holborn|lighthouse\s+capital|ocean\s+wall|globaleye|killik|quintet|stanhope|LGT\s+vestra)\b/i;
  if (targetFirms.test(text)) { score += 5; reasons.push('+5 target firm'); }

  // ============ NEGATIVE SIGNALS ============
  // Wealth manager / private banker AT a competitor private bank — these are competition, not prospects
  const competitorPrivateBanks = /\b(julius\s+baer|EFG\s+(international|bank)|UBS|lombard\s+odier|pictet|mirabaud|HSBC\s+private|BNP\s+paribas\s+wealth|standard\s+chartered\s+private|deutsche\s+bank\s+wealth|citi\s+private|barclays\s+private)\b/i;
  const wealthTitle = /\b(private\s+banker|wealth\s+manager|wealth\s+advisor|relationship\s+manager|client\s+advisor|investment\s+advisor)\b/i;
  if (competitorPrivateBanks.test(text) && wealthTitle.test(text)) {
    score -= 5; reasons.push('-5 wealth title at competitor private bank');
  }

  // CIO disambiguation: investment vs IT
  if (/\b(chief\s+investment\s+officer|head\s+of\s+investment)\b/i.test(text)) {
    score += 5; reasons.push('+5 investment CIO');
  } else if (/\bCIO\b/i.test(text) && /\b(wealth|asset|fund|portfolio|investment|family\s+office|capital|private\s+bank)/i.test(text)) {
    score += 5; reasons.push('+5 CIO (with finance context)');
  }
  if (/\bCIO\b/i.test(text) && /\b(information\s+technology|IT\s+infrastructure|ERP|cloud\s+migration|cybersecurity|software\s+engineering|digital\s+transformation)\b/i.test(text)) {
    score -= 6; reasons.push('-6 IT-CIO not investment');
  }

  // Junior / intern / student — never a prospect
  if (/\b(student|intern|junior\s+(analyst|associate)|graduate\s+(trainee|programme))\b/i.test(text)) {
    score -= 6; reasons.push('-6 junior/intern');
  }

  // Tech / IT roles — not prospects
  if (/\b(software\s+(engineer|developer)|IT\s+manager|IT\s+director|head\s+of\s+IT|backend\s+engineer|frontend\s+developer|DevOps|programmer|cloud\s+engineer|systems\s+engineer|full[- ]stack|SAP\s+(consultant|architect|analyst))\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office|fintech|asset\s+management)\b/i.test(text)) {
    score -= 6; reasons.push('-6 IT/dev role');
  }

  // HR / marketing / comms / talent acquisition — not prospects
  if (/\b(human\s+resources|HR\s+(manager|director|business\s+partner|leader)|talent\s+acquisition|head\s+of\s+HR|head\s+of\s+(marketing|communications)|chief\s+marketing|marketing\s+manager|communications\s+lead|crisis\s+management)\b/i.test(text)) {
    score -= 5; reasons.push('-5 HR/marketing/comms');
  }

  // Compliance / risk / operations — not commercial prospects
  if (/\b(compliance\s+officer|head\s+of\s+compliance|chief\s+risk\s+officer|head\s+of\s+risk|operations\s+(manager|director|analyst)|audit\s+(manager|director))\b/i.test(text) && !/\b(restructuring|distressed|special\s+situations|commodity)/i.test(text)) {
    score -= 4; reasons.push('-4 ops/compliance/risk');
  }

  // Unrelated industries (tile, ceramic, garment etc)
  if (/\b(tile|ceramic|construction\s+(materials|company)|garment|textile\s+(export|trader)|home\s+goods)\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office)\b/i.test(text)) {
    score -= 8; reasons.push('-8 unrelated industry');
  }
  if (/\b(real\s+estate\s+agent|broker|property\s+consultant)\b/i.test(text) && !/\b(wealth|investment|finance|capital|fund|family\s+office)\b/i.test(text)) {
    score -= 6; reasons.push('-6 real estate sales');
  }

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
  if (p.linkedinUrl) {
    // Extract /in/{slug} from URL. Old regex was buggy (matched first slash → collapsed all to "url:https:").
    const url = p.linkedinUrl.toLowerCase().trim();
    const m = url.match(/linkedin\.com\/(?:in|pub)\/([^/?#]+)/);
    if (m) return `li:${m[1]}`;
    // Fallback: strip protocol + query/hash, keep the path.
    const cleaned = url.replace(/^https?:\/\//, '').replace(/[?#].*$/, '').replace(/\/+$/, '');
    return `url:${cleaned}`;
  }
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
  // v10: add ID anchor + 'core' class for core specialties
  const sectionId = svc.name.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');
  const coreServices = ['Distressed & Special Situations', 'Commodities & Natural Resources', 'Structuring (Private Funds)'];
  const isCore = coreServices.includes(svc.name);
  const h2Class = isCore ? ' class="core"' : '';
  if (items.length === 0) {
    return `<section id="${sectionId}" class="service-section empty"><div class="service-header"><div class="left"><h2${h2Class}>${escapeHtml(svc.name)}</h2><p>${escapeHtml(svc.desc)}</p></div><div class="right-meta">No prospects today</div></div></section>`;
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
  return `<section id="${sectionId}" class="service-section"><div class="service-header"><div class="left"><h2${h2Class}>${escapeHtml(svc.name)}</h2><p>${escapeHtml(svc.desc)}</p></div><div class="right-meta">${items.length} prospect${items.length === 1 ? '' : 's'} \u00b7 today</div></div>${blocks}</section>`;
}

function renderRegionChips(regionCounts) {
  return REGIONS.map(r => {
    const n = regionCounts[r.name] || 0;
    const empty = n === 0;
    const sep = '<span aria-hidden="true" style="opacity:0.4; margin:0 4px;">\u00b7</span>';
    return `<button class="region-chip ${empty ? 'empty' : ''}" ${empty ? 'disabled' : `data-region="${escapeHtml(r.name)}"`}><span>${escapeHtml(r.name)}</span>${sep}<span class="count">${n}</span>${r.lead ? `${sep}<span class="lead">${escapeHtml(r.lead)}</span>` : ''}</button>`;
  }).join('');
}

const INLINE_TEMPLATE = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FCIM Daily Intelligence \u2014 {{DATE}}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,400;0,500;0,600;1,400&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{--ink:#1a1815;--cream:#FAF7EE;--cream-yellow:#F6EFD8;--gold:#C9B458;--gold-deep:#A88F35;--rule:#2a2722;--muted:#5a503e;--core:#7B2D26}
*{box-sizing:border-box}
body{margin:0;background:var(--cream);color:var(--ink);font-family:'Inter',ui-sans-serif,system-ui,sans-serif;font-size:15px;line-height:1.55}
a{color:inherit}
.wrap{max-width:1180px;margin:0 auto;padding:32px 24px 80px}
header{border-bottom:1px solid var(--rule);padding-bottom:18px;margin-bottom:24px;display:flex;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;gap:16px}
.brand{font-family:'Fraunces',ui-serif,Georgia,serif;font-size:28px;line-height:1;letter-spacing:0.02em}
.brand .sub{display:block;font-size:11px;letter-spacing:0.18em;text-transform:uppercase;color:var(--muted);margin-top:6px;font-family:'Inter',sans-serif}
.meta{font-size:12px;color:var(--muted);text-align:right;letter-spacing:0.04em}
.meta .date{font-family:'Fraunces',ui-serif,serif;font-style:italic;font-size:18px;color:var(--ink);display:block;margin-bottom:4px}
.council{margin:24px 0;font-family:'Fraunces',ui-serif,serif;font-style:italic;font-size:24px;line-height:1.3;letter-spacing:0.005em}

/* Service-sector navigation chips (replaces the old workflow tabs) */
.service-nav{margin:24px 0;padding:18px 0 14px;border-top:1px solid var(--rule);border-bottom:1px solid var(--rule)}
.service-nav-label{font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;font-weight:500}
.service-chips{display:flex;flex-wrap:wrap;gap:8px}
.service-chip{display:inline-flex;align-items:center;gap:6px;padding:8px 14px;background:var(--cream);border:1px solid var(--rule);border-radius:18px;font-size:13px;text-decoration:none;color:var(--ink);transition:background .15s,border-color .15s}
.service-chip:hover{background:var(--cream-yellow);border-color:var(--gold-deep)}
.service-chip.core{background:var(--cream-yellow);border-color:var(--gold);font-weight:600}
.service-chip.core:hover{background:#EFE3B8}
.service-chip .chip-count{font-size:11px;color:var(--muted);background:rgba(0,0,0,0.04);padding:2px 6px;border-radius:10px}
.service-chip.core .chip-count{background:rgba(169,143,53,0.18);color:var(--gold-deep)}

.regions{margin:24px 0}
.regions-label{font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
.region-chips{display:flex;flex-wrap:wrap;gap:8px}
.region-chip{display:inline-flex;align-items:center;gap:8px;padding:6px 12px;background:var(--cream);border:1px solid var(--rule);border-radius:14px;font-size:12px;cursor:pointer}
.region-chip.zero{opacity:.4}
.region-chip .name{font-weight:500}
.region-chip .count{color:var(--muted)}
.region-chip .lead{color:var(--muted);font-style:italic}
.clear-region{display:inline-block;margin-top:10px;font-size:12px;color:var(--muted);text-decoration:underline;cursor:pointer}

.featured-wrapper{margin:32px 0}
.featured-label{font-family:'Fraunces',ui-serif,serif;font-style:italic;font-size:22px;margin-bottom:12px}
.prospect.featured{border:2px solid var(--gold);background:#fffef5}

.service-section{margin:48px 0}
.service-header{display:flex;justify-content:space-between;align-items:flex-start;gap:24px;padding-bottom:14px;border-bottom:2px solid var(--rule);margin-bottom:24px;flex-wrap:wrap}
.service-header .left{flex:1;min-width:280px}
.service-header h2{font-family:'Fraunces',ui-serif,serif;font-size:30px;font-weight:500;margin:0 0 6px}
.service-header h2.core::before{content:'CORE \u00b7 ';color:var(--core);font-size:12px;letter-spacing:0.18em;font-family:'Inter',sans-serif;font-weight:600;vertical-align:middle;margin-right:4px}
.service-header p{margin:0;color:var(--muted);font-size:14px;line-height:1.5}
.service-header .right-meta{font-size:11px;letter-spacing:0.14em;text-transform:uppercase;color:var(--muted);white-space:nowrap}
.service-section.empty .service-header{border-bottom-color:#d8d2c0;opacity:0.5}

.prospect{background:#fffef9;border:1px solid var(--rule);padding:22px 24px;margin-bottom:18px}
.service-tag{font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:var(--gold-deep);font-weight:600;margin-bottom:12px}
.head-row{display:flex;justify-content:space-between;align-items:flex-start;gap:18px;flex-wrap:wrap}
.head-main{flex:1;min-width:240px}
.head-main h3{font-family:'Fraunces',ui-serif,serif;font-size:22px;line-height:1.2;margin:0 0 4px;font-weight:500}
.head-main .sub{color:var(--muted);font-size:13px}
.head-main .sub .dot{margin:0 6px;color:#bcb29a}
.lead-block{background:var(--cream-yellow);padding:6px 12px;font-size:12px;border-left:2px solid var(--gold)}
.lead-block .lead-label{display:block;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:var(--gold-deep);margin-bottom:2px}

.section{margin:14px 0}
.section .label{font-size:10px;letter-spacing:0.16em;text-transform:uppercase;color:var(--muted);margin-bottom:5px;font-weight:600}
.section p{margin:4px 0;font-size:13.5px;line-height:1.55}

.compliance{padding:10px 14px;margin:14px 0;border-left:3px solid #c14;font-size:12px;background:#fdf3f3}
.compliance .label{display:inline-block;font-size:9px;letter-spacing:0.18em;color:#c14;font-weight:700;margin-right:6px}

.first-step{background:var(--cream-yellow);padding:14px 16px;margin-top:18px;border-left:3px solid var(--gold)}
.first-step .label{font-size:10px;letter-spacing:0.16em;text-transform:uppercase;color:var(--gold-deep);font-weight:600;margin-bottom:6px}
.first-step p{margin:6px 0 12px;font-size:13.5px}
.draft-btn{padding:8px 14px;background:var(--ink);color:var(--cream);border:none;font-size:12px;letter-spacing:0.04em;cursor:pointer;font-family:inherit}
.draft-btn:hover{background:#000}
.draft-btn:active{transform:translateY(1px)}

footer{margin-top:64px;padding-top:24px;border-top:1px solid var(--rule);font-size:11px;color:var(--muted);letter-spacing:0.04em;text-align:center}

@media (max-width:680px){
  .wrap{padding:20px 16px 60px}
  header{flex-direction:column;align-items:flex-start}
  .meta{text-align:left}
  .service-header h2{font-size:24px}
  .head-main h3{font-size:18px}
  .lead-block{align-self:stretch}
  .head-row{flex-direction:column}
}
</style>
</head><body>
<div class="wrap">
<header>
  <div class="brand">FCIM Daily Intelligence<span class="sub">Fundament Capital \u00b7 Business Bay</span></div>
  <div class="meta"><span class="date">{{DATE}}</span><span>GitHub \u00b7 Daily build \u00b7 {{BUILT_AT}}</span></div>
</header>

<div class="council">{{COUNCIL_LINE}}</div>

<nav class="service-nav" aria-label="Service sectors">
  <div class="service-nav-label">FCIM service sectors \u2014 tap to jump</div>
  {{SERVICE_NAV}}
</nav>

<div class="regions">
  <div class="regions-label">Regions reporting today</div>
  {{REGION_CHIPS}}
  <span class="clear-region" id="clearRegion">Clear region filter</span>
</div>

<div class="featured-wrapper" style="{{FEATURED_WRAPPER_STYLE}}">
  <div class="featured-label">Today\u2019s strongest signal</div>
  {{FEATURED}}
</div>

<main>
{{CONTENT}}
</main>

<footer>FCIM Daily Intelligence \u00b7 internal tool for Yehya Abdelbaki \u00b7 generated {{BUILT_AT}}</footer>
</div>
<script>
// Region filter
document.querySelectorAll('.region-chip').forEach(chip => {
  chip.addEventListener('click', () => {
    const r = chip.getAttribute('data-region');
    document.querySelectorAll('.prospect').forEach(p => {
      const pr = p.getAttribute('data-region') || '';
      p.style.display = (pr === r) ? '' : 'none';
    });
    document.querySelectorAll('.region-chip').forEach(c => c.classList.remove('active'));
    chip.classList.add('active');
  });
});
document.getElementById('clearRegion').addEventListener('click', () => {
  document.querySelectorAll('.prospect').forEach(p => p.style.display = '');
  document.querySelectorAll('.region-chip').forEach(c => c.classList.remove('active'));
});
// Copy draft prompt
document.querySelectorAll('.draft-btn').forEach(btn => {
  btn.addEventListener('click', async () => {
    const t = btn.getAttribute('data-prompt');
    try { await navigator.clipboard.writeText(t); btn.textContent = 'Copied'; setTimeout(()=>btn.textContent='Copy draft prompt', 1500); }
    catch(e) { btn.textContent='Copy failed'; setTimeout(()=>btn.textContent='Copy draft prompt', 1500); }
  });
});
// Smooth-scroll service chips
document.querySelectorAll('.service-chip').forEach(c => {
  c.addEventListener('click', e => {
    const href = c.getAttribute('href');
    if (href && href.startsWith('#')) {
      const t = document.getElementById(href.slice(1));
      if (t) { e.preventDefault(); t.scrollIntoView({behavior:'smooth', block:'start'}); }
    }
  });
});
</script>
</body></html>`;

async function main() {
  console.log('FCIM Daily Build v10 - FCIM-aligned strategy + service nav - starting');
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
  // v10: template now inlined in build.js — no separate file needed
  const template = INLINE_TEMPLATE;
  // Function-form replacements so $&, $1, etc. in values are not interpreted as backreferences.
  const dateStr = escapeHtml(dateStamp);
  const builtAtStr = escapeHtml(new Date().toISOString());
  const councilStr = escapeHtml(councilLine);
  const chipsStr = renderRegionChips(regionCounts);
  const featuredStr = featured ? prospectCardHtml(featured, true) : '';
  const contentStr = servicesHtml + emptyServicesHtml;
  const wrapperStr = featured ? '' : 'display:none';
  // v10: build service-sector navigation chips (replaces old workflow tabs)
  const coreServices = ['Distressed & Special Situations', 'Commodities & Natural Resources', 'Structuring (Private Funds)'];
  const serviceCounts = {};
  for (const svc of SERVICES) {
    serviceCounts[svc.name] = (featured && featured.diagnosis && featured.diagnosis.fcimService === svc.name ? 1 : 0)
      + remaining.filter(p => p.diagnosis && p.diagnosis.fcimService === svc.name).length;
  }
  const slugify = s => s.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');
  const serviceNav = '<div class="service-chips">' + SERVICES.map(svc => {
    const isCore = coreServices.includes(svc.name);
    const count = serviceCounts[svc.name] || 0;
    return `<a href="#${slugify(svc.name)}" class="service-chip ${isCore ? 'core' : ''}">${escapeHtml(svc.name)}<span class="chip-count">${count}</span></a>`;
  }).join('') + '</div>';
  const html = template
    .replace(/\{\{DATE\}\}/g, () => dateStr)
    .replace(/\{\{BUILT_AT\}\}/g, () => builtAtStr)
    .replace(/\{\{COUNCIL_LINE\}\}/g, () => councilStr)
    .replace(/\{\{REGION_CHIPS\}\}/g, () => chipsStr)
    .replace(/\{\{SERVICE_NAV\}\}/g, () => serviceNav)
    .replace(/\{\{FEATURED\}\}/g, () => featuredStr)
    .replace(/\{\{CONTENT\}\}/g, () => contentStr)
    .replace(/\{\{FEATURED_WRAPPER_STYLE\}\}/g, () => wrapperStr);
  fs.writeFileSync('index.html', html);
  console.log(`Built index.html - ${profiles.length} qualified prospects, ${verifiedCount} with verified emails.`);
}

main().catch(err => { console.error(err); process.exit(1); });
