/* FCIM Daily Intelligence - daily builder v11.1 */
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
const MAX_HUNTER_CALLS_PER_RUN = 100;

const QUERY_BUCKETS = [
  // v11.1: TITLE + LOCATION based bucketing (was: COMPANIES list).
  //
  // v11 still hunted at named-firm scale — Gulf Capital, Waha, Mizuho, BECO, Aramex,
  // Hassan Allam, Damac. All $500m+ revenue. Yehya is a first-year RM at $5.5B FCIM
  // and these firms have entrenched tier-1 banking syndicates. Wrong-altitude counterparties.
  //
  // The actual sweet spot for FCIM RM-led outreach is sub-scale: $10m-$200m AUM
  // family offices, single-principal advisory boutiques, sole-founder fund managers,
  // owner-operated small commodity traders, recently-launched single-fund managers.
  //
  // These shops cannot be found by company name (names are obscure or generic FZ-LLCs).
  // They CAN be found by title + location: "Family Office Director Dubai",
  // "Founder & Managing Director Dubai DMCC", "Single Family Office Principal".
  //
  // v11.1 uses Apify currentJobTitles + locations params (harvestapi supports these).
  //
  // Each bucket: ~25 profiles per run × 8 buckets = 200 raw → ~140 qualified after gates.

  {
    label: 'Single Family Office Principals — Dubai',
    body: {
      currentJobTitles: ['Family Office Principal', 'Family Office Director', 'Single Family Office Founder', 'Head of Single Family Office', 'Family Office Manager', 'Family Office Chief Investment Officer'],
      locations: ['Dubai', 'United Arab Emirates'],
      profileScraperMode: 'Full',
      maxItems: 25
    },
    region: null, serviceHint: 'Family Office Advisory'
  },
  {
    label: 'Founder-Operated Investment Boutiques — DIFC/ADGM',
    body: {
      currentJobTitles: ['Managing Partner', 'Founder & Managing Partner', 'Founder & CEO', 'Founder Partner', 'Owner & Director', 'Founder Director'],
      locations: ['Dubai International Financial Centre', 'Abu Dhabi Global Market', 'Dubai', 'Abu Dhabi'],
      profileScraperMode: 'Full',
      maxItems: 25,
      // search keyword narrows to investment boutiques (vs random startups)
      search: 'investment advisory boutique'
    },
    region: null, serviceHint: 'Structuring (Private Funds)'
  },
  {
    label: 'Sub-$200m AUM Sole-Founder Fund Managers',
    body: {
      currentJobTitles: ['Fund Manager', 'Portfolio Manager', 'Founder & Fund Manager', 'CIO & Founder', 'Chief Investment Officer'],
      locations: ['Dubai', 'United Arab Emirates'],
      profileScraperMode: 'Full',
      maxItems: 25,
      search: 'private fund hedge fund boutique'
    },
    region: null, serviceHint: 'Structuring (Private Funds)'
  },
  {
    label: 'DMCC/DIFC Owner-Operated Commodity Traders',
    body: {
      currentJobTitles: ['Founder & CEO', 'Owner & Director', 'Managing Director', 'Founder', 'Director'],
      locations: ['Dubai Multi Commodities Centre', 'DMCC', 'Dubai'],
      profileScraperMode: 'Full',
      maxItems: 25,
      search: 'commodity trader physical trader DMCC oilseed grain metals fertilizer'
    },
    region: null, serviceHint: 'Commodities & Natural Resources'
  },
  {
    label: 'Independent Wealth Advisors & EAMs — Dubai',
    body: {
      currentJobTitles: ['Independent Wealth Manager', 'Independent Financial Advisor', 'External Asset Manager', 'Founder & Wealth Advisor', 'Managing Partner', 'Senior Partner'],
      locations: ['Dubai', 'United Arab Emirates'],
      profileScraperMode: 'Full',
      maxItems: 25,
      search: 'independent wealth advisor EAM boutique'
    },
    region: null, serviceHint: 'EAM / FI Platform'
  },
  {
    label: 'UAE/GCC Mid-Cap Family Business — Founders & 2G Principals',
    body: {
      currentJobTitles: ['Founder & Managing Director', 'Founder & Chairman', 'Director', 'Vice Chairman', 'Group Director'],
      locations: ['Dubai', 'Sharjah', 'Abu Dhabi', 'United Arab Emirates'],
      profileScraperMode: 'Full',
      maxItems: 25,
      search: 'family business holding founder second generation'
    },
    region: null, serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Russian/CIS Founder-Principals in Dubai',
    body: {
      currentJobTitles: ['Founder & CEO', 'Owner', 'Founder', 'Managing Director', 'Chief Investment Officer'],
      locations: ['Dubai', 'United Arab Emirates'],
      profileScraperMode: 'Full',
      maxItems: 25,
      search: 'Russian Kazakh Belarus Ukrainian relocated entrepreneur'
    },
    region: null, serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Indian Sub-Continent Founder-Principals — Dubai',
    body: {
      currentJobTitles: ['Founder & Managing Director', 'Founder', 'Owner & Director', 'Group Managing Director'],
      locations: ['Dubai', 'United Arab Emirates'],
      profileScraperMode: 'Full',
      maxItems: 25,
      search: 'Indian family business trading import export Dubai DMCC'
    },
    region: null, serviceHint: 'Foundation + Private Fund'
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
    fcimService: 'IB & Advisory', angle: 'Capital-markets activity ahead - FCIM IB desk (Tim Almashat, MD Head of IB) covers ECM, DCM, M&A, UAE listings.' },
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
  { name: 'Russia / CIS', lead: 'Dmitri Ganjour', warmPath: 'Dmitri Ganjour via ETH Zürich and EPFL networks; Russian/French/English-speaking, derivatives and quant background.' },
  { name: 'Caucasus / CIS', lead: 'Dmitri Ganjour', warmPath: 'Dmitri Ganjour via Caucasus and CIS quant/derivatives professional networks.' },
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
  /\bEFG\s+(international|bank|hermes)\b/i,

  // v11 ADDITIONS: firms structurally wrong for FCIM at first-year-RM altitude.

  // FIRM-TOO-BIG block: $50bn+ AUM funds. FCIM is a vendor not a peer. RM altitude wrong.
  /\b(blackstone|kkr|carlyle\s+group|the\s+carlyle\s+group|TPG\s+capital|TPG\s+inc|warburg\s+pincus|bain\s+capital|advent\s+international|cvc\s+capital|EQT\s+partners|apollo\s+global|brookfield\s+asset|ardian|permira|silver\s+lake|vista\s+equity|thoma\s+bravo|general\s+atlantic|hellman\s+&\s+friedman|leonard\s+green|providence\s+equity|partners\s+group|mubadala\s+capital|adq|adia|GIC|temasek|PIF|public\s+investment\s+fund|saudi\s+arabia\s+sovereign)\b/i,

  // GLOBAL DISTRESSED FUNDS: $30bn+ credit shops. Same wrong-altitude problem.
  /\b(oaktree\s+capital|ares\s+management|cerberus\s+capital|davidson\s+kempner|v\u00e4rde\s+partners|varde\s+partners|sculptor\s+capital|centerbridge\s+partners|brevet\s+capital|fortress\s+investment|elliott\s+management|baupost\s+group|king\s+street\s+capital)\b/i,

  // GLOBAL IB & RESTRUCTURING ADVISORS: tier-1 advisor MDs are not first-year-RM cold targets.
  /\b(houlihan\s+lokey|rothschild\s+&\s+co|rothschildandco|lazard\s+(ltd|freres|group)|moelis\s+&\s+company|evercore|perella\s+weinberg|PJT\s+partners|guggenheim\s+partners|alixpartners|alvarez\s+&\s+marsal|FTI\s+consulting|kroll\s+(LLC|inc|advisory))\b/i,

  // GLOBAL COMMODITY MAJORS: $50bn+ revenue traders use tier-1 banks for derivatives.
  /\b(trafigura|vitol|glencore|mercuria|gunvor|cargill\s+(inc|incorporated)|bunge\s+(limited|global)|olam\s+international|wilmar\s+international|louis\s+dreyfus|ADM\s+(archer|company)|cofco\s+international)\b/i,

  // GLOBAL FUND ADMINS — direct competitors on FCIM's service line.
  /\b(apex\s+group|apex\s+fund\s+services|TMF\s+group|hawksford|trident\s+trust|VISTRA\s+(group|ltd|services)|IQ-?EQ|maitland\s+group|stonehage\s+fleming|citco\s+fund\s+services|northern\s+trust|state\s+street\s+(corporation|fund))\b/i,

  // GLOBAL PRIVATE BANKS at competitor altitude — not first-year-RM targets.
  /\b(UBS\s+(group|wealth|global|AG)|credit\s+suisse|deutsche\s+bank\s+wealth|citi\s+private\s+bank|JP\s*morgan\s+private|goldman\s+sachs\s+(private|wealth)|morgan\s+stanley\s+(wealth|private)|HSBC\s+private\s+bank|standard\s+chartered\s+private|BNP\s+paribas\s+wealth|lombard\s+odier|pictet\s+&\s+cie|mirabaud|edmond\s+de\s+rothschild|safra\s+sarasin)\b/i
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

  // v11.1 SMALL-SHOP boost — title and firm signals that indicate sub-scale prospects.
  // Founder/principal-level titles at single-fund or single-family-office shops are
  // FCIM's actual sweet spot. v11 boosted Gulf Capital/Waha/Hassan Allam — those
  // are now NEUTRAL (no boost, no penalty) because they were too big.
  const smallShopTitles = /\b(founder\s*&?\s*managing\s+director|founder\s*&?\s*CEO|founder\s+partner|founder\s+director|owner\s*&?\s*director|owner\s*&?\s*founder|sole\s+founder|single\s+family\s+office|family\s+office\s+(principal|director|CIO|head|founder)|managing\s+partner|founding\s+partner|principal)\b/i;
  if (smallShopTitles.test(text)) {
    score += 6; reasons.push('+6 founder/principal small-shop title');
  }

  // v11.1 SMALL-FIRM SIGNALS — bio language that suggests sub-scale shop
  const smallFirmSignals = /\b(boutique|family[- ]office|single[- ]family[- ]office|SFO|sole\s+founder|launched\s+(my|our)\s+own|first\s+fund|fund\s+I|FZ-?LLC|started\s+in\s+\d{4}|established\s+in\s+\d{4}|founded\s+in\s+\d{4})\b/i;
  if (smallFirmSignals.test(text)) {
    score += 4; reasons.push('+4 small-firm bio signal');
  }

  // v11.1 LARGE-FIRM PENALTY — these surfaced in v11 results but are too big.
  // Now penalized rather than boosted. Names retained from v11 target list since
  // title-based search may still surface them.
  const tooLargeForRM = /\b(gulf\s+capital|waha\s+capital|NBK\s+capital|shorooq\s+partners|BECO\s+capital|wamda\s+capital|mizuho\s+gulf\s+capital|riyad\s+capital|saudi\s+fransi\s+capital|GFH\s+financial|thumbay\s+group|al\s+habtoor|hassan\s+allam|orascom|mansour\s+group|olayan\s+group|sabbagh|aramex|damac|aldar|deyaar|al\s+tamimi|stephenson\s+harwood|hadef\s+&\s+partners|charles\s+russell|investcorp|ARDIAN|wamda)\b/i;
  if (tooLargeForRM.test(text)) {
    score -= 4; reasons.push('-4 firm too large for first-year-RM cold (v11.1)');
  }

  // v11 PROPORTIONALITY PENALTY — global mega-firms surfaced despite COMPLIANCE_BLOCK.
  // Belt-and-suspenders: compliance kills these, but if anything slips through, score
  // pushes them below the quality gate. Only triggers if firm-name keywords appear.
  const megaFirmTextual = /\b(blackstone|KKR\b|carlyle|TPG\s+(capital|inc|holdings)|warburg\s+pincus|bain\s+capital|advent\s+international|CVC\s+capital|EQT\s+partners|apollo\s+global|brookfield|permira|silver\s+lake|vista\s+equity|thoma\s+bravo|general\s+atlantic|partners\s+group|mubadala\s+capital|ADIA|GIC\s+private|temasek|public\s+investment\s+fund|oaktree|ares\s+management|cerberus\s+capital|davidson\s+kempner|fortress\s+investment|elliott\s+management|houlihan\s+lokey|rothschild|lazard|moelis|evercore|perella|PJT\s+partners|guggenheim|alvarez\s+&\s+marsal|FTI\s+consulting|alixpartners|trafigura|vitol|glencore|mercuria|gunvor|cargill|bunge|olam|wilmar|louis\s+dreyfus|cofco|apex\s+group|TMF\s+group|hawksford|trident\s+trust|vistra|IQ-?EQ|maitland|stonehage\s+fleming|citco|UBS|credit\s+suisse|deutsche\s+bank|citi\s+private|JP\s*morgan\s+private|goldman\s+sachs|morgan\s+stanley|HSBC\s+private|BNP\s+paribas\s+wealth|lombard\s+odier|pictet|mirabaud|edmond\s+de\s+rothschild)\b/i;
  if (megaFirmTextual.test(text)) {
    score -= 10; reasons.push('-10 mega-firm wrong-altitude for FCIM');
  }

  // v11 SENIORITY MISMATCH PENALTY — Partner / SEO / Global Head / Board Member
  // at large firms is wrong-altitude for first-year-RM cold outreach. These names
  // require Amr/Ibrahim/Steven to send, not Yehya. Drop them from Yehya's list.
  const tooSeniorTitle = /\b(senior\s+managing\s+director|global\s+(head|partner)|partner\s+&\s+(MD|managing\s+director|head)|managing\s+partner|senior\s+partner|founding\s+partner|chairman\s+&\s+CEO|group\s+CEO|group\s+chairman|board\s+member|board\s+director|SEO\s+of|chief\s+executive\s+officer)\b/i;
  // ...but only penalize if NOT at a small/mid-market firm (small firms have small "MD" titles too)
  const smallFirmExempt = /\b(boutique|family\s+office|advisory|consultancy|partners\s+(LLP|UAE)|associates|FZ-?LLC|SME)\b/i;
  if (tooSeniorTitle.test(text) && !smallFirmExempt.test(text) && megaFirmTextual.test(text)) {
    score -= 6; reasons.push('-6 seniority mismatch (Yehya altitude)');
  }

  // v11.1 SWEET-SPOT TITLE at SMALL FIRM — Director / VP / Head of [Function]
  // at a small shop (not at a mega firm). Combined boost.
  const sweetSpotTitle = /\b(director|vice\s+president|VP\b|head\s+of\s+(finance|treasury|corporate\s+development|M&A|investments|portfolio|structured|risk)|principal|associate\s+director|senior\s+manager)\b/i;
  if (sweetSpotTitle.test(text) && smallShopTitles.test(text)) {
    score += 3; reasons.push('+3 sweet-spot title at small shop');
  }

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

  // v11: surface the matched FCIM evidence to the email-drafting agent so the
  // Credibility Agent has actual citable text to anchor "why us" claims.
  const match = (typeof matchEvidence === 'function') ? matchEvidence(p) : null;
  const evidenceLine = match
    ? `\nFCIM EVIDENCE (from ${match.evidence.source_pdf}, p.${match.evidence.source_page})\n"${match.evidence.text}"\n`
    : '\nFCIM EVIDENCE: no chunk auto-matched. Use the SERVICES.solution text as fallback and flag in council.\n';

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
${evidenceLine}
REGION & WARM PATH
Region: ${p.region || '(not detected)'}
Suggested lead: ${p.regionLead || '(route by context)'}
Warm path: ${p.regionWarmPath || '(route by context)'}

============================================================
COUNCIL OF AGENTS — RUN BEFORE DRAFTING (mandatory)
============================================================
Before writing the email, answer each agent honestly. If any agent
fails decisively, RECOMMEND SKIPPING THE PROSPECT instead of forcing a
draft. A "no draft" output is acceptable and preferred over a weak email.

AGENT 1 — PROPORTIONALITY AGENT (runs first)
- Is the prospect's firm matched to FCIM ($5.5B AUM, mid-market regional)?
- Rule: prospect firm should not be more than ~5x FCIM's size.
- Rule: counterpart seniority should be Director/VP/MD-at-small-firm —
  not Partner / Group CEO / SEO at a $50bn+ firm.
- Rule: Yehya is a first-year RM. If counterpart altitude requires Amr,
  Ibrahim, or Steven to send the email, FLAG and do not draft from Yehya.
- VERDICT: pass / fail / route-to-senior-colleague.

AGENT 2 — PROBLEM AGENT
- What is the prospect's specific, present-tense pain?
- Avoid generic industry pain. Cite something from THEIR bio or firm news.
- If you cannot name a specific situation, FAIL.

AGENT 3 — WHY-US AGENT
- Of the alternatives the prospect has, why FCIM specifically?
- Differentiator must be concrete: 10-day formation, SCA derivatives access,
  Foundation-as-parent, $5.5B balance sheet, etc.
- USE THE FCIM EVIDENCE ABOVE — that is the ground-truth claim drawn from
  FCIM's own corporate documentation. Do not invent claims that are not
  supported by the evidence chunk.

AGENT 4 — CREDIBILITY AGENT
- The FCIM EVIDENCE block above provides one citable, FCIM-authored claim.
  Lean on it explicitly in the email body.
- If you find yourself writing claims not supported by the evidence chunk,
  STOP and rewrite to stay within what the evidence covers.
- Named deal/client references are a bonus but not required when the
  evidence chunk is itself specific (e.g., "10 business days", "100%
  concentration permitted").

AGENT 5 — COUNTERPARTY AGENT
- What institutional friction prevents this from happening?
- Does the prospect have authority to act, or is this an upward-routed ask?
- Is there a procurement / panel / preferred-partner process that blocks
  ad-hoc onboarding?
- Frame the email so it survives that friction (give the prospect something
  forwardable, not just personal).

AGENT 6 — THREAT AGENT
- What is the worst, most cynical reading of this email from the
  prospect's side?
- "FCIM is fishing for our admin mandates" / "Random RM cold-pitching"
  / "Generic outreach" — if any cynical reading is plausibly accurate,
  rewrite to be ungameable.

AGENT 7 — NEXT-STEP AGENT
- What is the SMALLEST, lowest-friction next action that proves mutual
  interest? (Not "20-min Binary Tower meeting" by default.)
- For partnership conversations: a single binary question they can answer
  in one line.
- For client conversations: a specific, dated, named-contact next step.

============================================================
WRITING RULES (only after council passes)
============================================================
- Open with a SPECIFIC reference to their actual situation — drawn from
  the bio / firm / role. Not "I came across your profile."
- Lead with the diagnosed problem framed as observation, not assumption.
- Position FCIM solution clearly but not pitchy. Use the EVIDENCE chunk
  above as your concrete proof point. Reference real specifics ($5.5B AUM,
  CMA-licensed, SCA-licensed) where relevant.
- Reference the warm-path FCIM colleague naturally only if it strengthens
  credibility.
- Do not glaze. Do not be over-eager. Do not pitch every service.
- NEVER mention Gary Dugan.
- Subject line: specific, attention-grabbing, not promotional, not cringe.
- Email format follows the FCIM standard: "Dear [Name], / My name is
  Yehya Abdelbaki, Relationship Manager at FCIM..." opener; bullet body
  (2-4 bullets max); "I would like to invite you to come and meet us at
  The Binary Tower" closer; full signature.
- End with full Yehya signature: Yehya Abdelbaki / Relationship Manager /
  Fundament Capital Investment Management / $5.5B AUM | SCA & CMA Licensed |
  Office 1511, The Binary Tower, Business Bay, Dubai, UAE / M: +971 55 280 6653
  / yehya.abdelbaki@fundamentcapital.ae / www.fundamentcapital.ae

OUTPUT FORMAT
1. Council verdict (one paragraph): pass / skip / route-to-senior-colleague.
2. If pass: subject line + full email + recommended PDF attachment (or none).
3. If skip: one-sentence reason.
4. If route-to-senior-colleague: name the colleague (Amr/Ibrahim/Steven/
   Saran/Dmitri) and what they should send instead.`;
}

// v11: FCIM evidence base — citable PDF-sourced solution chunks loaded at build time.
// PDFs themselves remain private (not committed to repo); only the curated evidence
// JSON and citation references are rendered on the public site.
let FCIM_EVIDENCE = [];
try {
  const ev = JSON.parse(fs.readFileSync('./fcim_evidence.json', 'utf-8'));
  FCIM_EVIDENCE = ev.evidence || [];
  console.log(`Loaded ${FCIM_EVIDENCE.length} FCIM evidence chunks for prospect-card grounding.`);
} catch (e) {
  console.warn('fcim_evidence.json not found alongside build.js — falling back to hardcoded SERVICES.solution text. Evidence citations will not appear on cards.');
}

// matchEvidence: given a prospect (with diagnosed service + bio text), find the
// single highest-relevance evidence chunk. Returns null if no chunk matches the
// service or if signal scoring is too weak to cite confidently.
function matchEvidence(p) {
  if (!FCIM_EVIDENCE.length) return null;
  const dx = p.diagnosis || {};
  const targetService = dx.fcimService;
  if (!targetService) return null;

  // Step 1: filter to evidence tagged with the diagnosed service.
  const candidates = FCIM_EVIDENCE.filter(ev => ev.services.includes(targetService));
  if (!candidates.length) return null;

  // Step 2: score each candidate by signal-keyword hits in the prospect's bio + title.
  const haystack = `${p.title || ''} ${p.about || ''} ${dx.problem || ''}`.toLowerCase();
  const scored = candidates.map(ev => {
    const hits = (ev.signals || []).filter(sig => haystack.includes(sig.toLowerCase())).length;
    return { ev, hits };
  });
  scored.sort((a, b) => b.hits - a.hits);

  // Step 3: pick the top match. If zero hits, fall back to the first candidate
  // for the service (still citable, just less targeted).
  const winner = scored[0];
  return { evidence: winner.ev, hits: winner.hits };
}

function renderEvidenceBlock(p) {
  const match = matchEvidence(p);
  if (!match) return null;
  const ev = match.evidence;
  const targeted = match.hits > 0 ? '' : ' <span style="font-size:10px; letter-spacing:0.1em; text-transform:uppercase; color:#9a7a3a;">general — service-level</span>';
  return `<div class="section evidence">
    <div class="label">FCIM evidence${targeted}</div>
    <p>${escapeHtml(ev.text)}</p>
    <p class="citation"><em>Source: ${escapeHtml(ev.source_pdf)}, p.${ev.source_page}${ev.id ? ' \u00b7 ref ' + escapeHtml(ev.id) : ''}</em></p>
  </div>`;
}

function prospectCardHtml(p, isFeatured) {
  const dx = p.diagnosis || {};
  const service = SERVICES.find(s => s.name === dx.fcimService);
  const solutionText = service ? service.solution : 'Service match indeterminate - review profile manually.';
  const evidenceBlock = renderEvidenceBlock(p) || '';
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
      ${evidenceBlock}
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

.section.evidence{padding:12px 14px;margin:14px 0;background:#FAF6E8;border-left:3px solid var(--gold)}
.section.evidence .label{color:var(--gold-deep)}
.section.evidence p{margin:6px 0;font-size:13px;line-height:1.6}
.section.evidence .citation{margin-top:8px;font-size:11px;color:var(--muted);letter-spacing:0.02em}

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
  console.log('FCIM Daily Build v11.1 - title+location buckets, founder-scale prospects - starting');
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
