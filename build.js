/* FCIM Daily Intelligence — daily builder v3
 * Runs in GitHub Actions on a schedule.
 * Pipeline:
 *   1. Apify pulls candidate profiles (LinkedIn search, Short mode for cost).
 *   2. Quality gate scores each profile, drops sub-threshold.
 *   3. Problem-diagnosis agent maps each kept profile to a specific
 *      FCIM-solvable problem and the matching service.
 *   4. Hunter.io finds verified email per kept profile (graceful fallback to multi-pattern guess).
 *   5. Renders index.html grouped by FCIM service, then by region.
 */
const fs = require('node:fs');

// =========================================================================
// CONFIG
// =========================================================================

const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) {
  console.error('FATAL: APIFY_TOKEN env var missing. Set it as a GitHub repo secret.');
  process.exit(1);
}
const HUNTER_API_KEY = process.env.HUNTER_API_KEY || null;
if (!HUNTER_API_KEY) {
  console.warn('NOTE: HUNTER_API_KEY missing — emails will fall back to format-pattern guesses.');
}

const APIFY_ACTOR = 'harvestapi~linkedin-profile-search';
const PROFILES_PER_QUERY = 12;
const QUALITY_THRESHOLD = 5;
const MAX_HUNTER_CALLS_PER_RUN = 25;

// =========================================================================
// SEARCH BUCKETS
// =========================================================================

const QUERY_BUCKETS = [
  {
    label: 'Caucasus family offices Dubai',
    body: {
      searchQuery: 'Armenian Azerbaijani Georgian family',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: 'Caucasus / CIS',
    serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Russia/CIS HNW & family offices Dubai',
    body: {
      searchQuery: 'Russian Kazakh family office Dubai',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: 'Russia / CIS',
    serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'African family offices Dubai',
    body: {
      searchQuery: 'Nigerian Kenyan South African family',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: 'Africa',
    serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'MENA family offices Dubai',
    body: {
      searchQuery: 'Lebanese Egyptian family office Dubai',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: 'MENA / Levant',
    serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'Commodity traders Dubai',
    body: {
      searchQuery: 'commodity trading Dubai',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: null,
    serviceHint: 'Commodity Derivatives'
  },
  {
    label: 'Investment holdings / serial founders Dubai',
    body: {
      searchQuery: 'investment holding founder Dubai',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: null,
    serviceHint: 'Foundation + Private Fund'
  },
  {
    label: 'EAMs & boutique wealth managers Dubai',
    body: {
      searchQuery: 'external asset manager Dubai',
      locations: ['United Arab Emirates'],
      profileScraperMode: 'Short',
      maxItems: PROFILES_PER_QUERY
    },
    region: null,
    serviceHint: 'EAM / FI Platform'
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
// FCIM SERVICES
// =========================================================================

const SERVICES = [
  {
    name: 'Discretionary Portfolio Management',
    desc: 'Five CMA-approved models from capital preservation through aggressive. USD 1M minimum.',
    solution: 'Discretionary mandate on a CMA-approved model portfolio matched to the client\u2019s risk profile and time horizon.'
  },
  {
    name: 'Foundation + Private Fund',
    desc: 'UAE Foundation owning a CMA Private Fund. Three-level control. UBO privacy. 10-day approval.',
    solution: 'UAE Foundation owning a CMA Private Fund. Only FCIM and the regulator know the UBO. Three-level control: directly at the asset, via the fund board, via the foundation council. Already executed for $15-20M acquisition vehicles and $100M+ real estate restructures.'
  },
  {
    name: 'CMA Private Fund (standalone)',
    desc: 'Regulated UAE private fund. No restriction on asset type. Fast-track 10 working day approval.',
    solution: 'Standalone CMA Private Fund. No restrictions on asset class — public/private equity, credit, real estate, single-asset 100% concentration. FCIM acts as both manager and administrator.'
  },
  {
    name: 'Commodity Derivatives',
    desc: 'SCA-licensed direct CME / ICE / LME / EEX / SGX access without a clearing account.',
    solution: 'SCA-licensed commodity derivatives platform. Direct access to CME, ICE, LME, EEX, SGX without the client needing to set up their own clearing account. Futures, options, swaps, cross-commodity spreads. Risk and margin reporting built in.'
  },
  {
    name: 'Fund Administration',
    desc: 'One of only five UAE-authorised fund administrators. In-house or third-party funds.',
    solution: 'Fund administration for in-house or third-party funds, UAE or foreign-domiciled. NAV calculation, investor servicing, regulatory reporting, multi-tier verification.'
  },
  {
    name: 'IB & Advisory',
    desc: 'Dmitri Tchekalkine-led desk. ECM, DCM, M&A, listings on UAE exchanges.',
    solution: 'Investment banking and advisory. IPO / bond / sukuk issuance manager, UAE exchange listing advisory, M&A in the USD 50-150M band. Led by Dmitri Tchekalkine (30+ years emerging markets, ex-JPMorgan/BNP/HSBC).'
  },
  {
    name: 'Family Office Advisory',
    desc: 'Governance, succession, estate planning, concierge, VC/PE direct deals.',
    solution: 'Full family office build: governance frameworks, multi-generational succession planning, estate structuring, concierge, and direct VC/PE deal access aligned to family interests.'
  },
  {
    name: 'EAM / FI Platform',
    desc: 'Confidential Client Money accounts at FAB and ENBD. Secondary custodianship for EAMs and FIs.',
    solution: 'Platform for external asset managers and financial institutions. Open Confidential Client Money accounts at First Abu Dhabi Bank and Emirates NBD. FCIM acts as secondary custodian; brokers execute under their names representing FCIM accounts.'
  }
];

// =========================================================================
// PROBLEMS
// =========================================================================

const PROBLEMS = [
  {
    id: 'international-asset-holding',
    label: 'Holding international assets under sovereign / banking pressure',
    signals: [
      /\b(international|cross[- ]border|european|offshore)\s+(asset|real estate|holding|property|portfolio)/i,
      /\b(sovereign|sanction|banking|payment)\s+(risk|pressure|challenge|difficulty)/i,
      /\b(re-?structure|re-?domicile|consolidate)\s+(holdings|assets|portfolio)/i
    ],
    fcimService: 'Foundation + Private Fund',
    angle: 'International asset-holding and banking-access pressure — the same pattern FCIM solved for the $100M+ Family A real-estate restructure case.'
  },
  {
    id: 'ubo-privacy',
    label: 'UBO privacy / anonymity in acquisitions',
    signals: [
      /\b(confidential|anonymity|private)\s+(acquisition|investment|deal|transaction)/i,
      /\b(UBO|beneficial owner|disclosure)\b/i,
      /\b(undisclosed|private|sensitive)\s+(stake|investment|holding)/i
    ],
    fcimService: 'Foundation + Private Fund',
    angle: 'UBO privacy in acquisitions — same structure FCIM used for the Principal A football-club roll-up ($15-20M per acquisition, 3 clubs done, UBO never disclosed to counterparties or banks).'
  },
  {
    id: 'multi-venture-structuring',
    label: 'Founder with multiple ventures needing structured holdco',
    signals: [
      /\b(multiple|portfolio of|several)\s+(ventures|companies|businesses|investments)/i,
      /\b(serial|repeat)\s+(entrepreneur|founder|investor)/i,
      /\b(investment\s+holding|holding\s+company|family\s+holding)\b/i,
      /\b(group\s+chairman|group\s+CEO|founder\s+&\s+chairman)\b/i
    ],
    fcimService: 'Foundation + Private Fund',
    angle: 'Multiple operating ventures held under personal name or scattered SPVs — Foundation + Private Fund provides one umbrella with three-level control and clean UBO privacy.'
  },
  {
    id: 'family-succession',
    label: 'Family succession / generational wealth transition',
    signals: [
      /\b(next generation|second generation|third generation|2G|3G)\b/i,
      /\b(succession|legacy|inheritance|estate)\s+(planning|transition|strategy)/i,
      /\b(family\s+(business|enterprise|legacy|trust|council))\b/i
    ],
    fcimService: 'Family Office Advisory',
    angle: 'Family succession and generational transition — full family-office build covering governance, succession, estate planning, and concierge.'
  },
  {
    id: 'commodity-hedging',
    label: 'Physical commodity exposure without hedging infrastructure',
    signals: [
      /\b(physical\s+(commodity|trader|trading))\b/i,
      /\b(grain|fertili[sz]er|freight|metals|energy|oilseed|sugar|cocoa|coffee|cotton)\s+(trad|market|hedg)/i,
      /\b(hedging|risk\s+management)\b.*\b(commodity|commodities|metals|energy|grain)/i,
      /\b(import|export)\s+(business|operations|trader)\b/i
    ],
    fcimService: 'Commodity Derivatives',
    angle: 'Physical commodity exposure without an exchange-cleared hedging desk — FCIM\u2019s SCA-licensed platform gives direct CME/ICE/LME access without the client setting up their own clearing account.'
  },
  {
    id: 'eam-platform-need',
    label: 'EAM / boutique wealth manager looking for client-money platform',
    signals: [
      /\b(external\s+asset\s+manager|EAM)\b/i,
      /\b(independent\s+(wealth|financial)\s+(manager|advisor))\b/i,
      /\b(boutique|managing\s+partner).*\b(wealth|advisory|asset\s+management)/i,
      /\b(family\s+wealth\s+advisor|multi-?family\s+office\s+founder)\b/i
    ],
    fcimService: 'EAM / FI Platform',
    angle: 'EAM looking for a regulated platform — FCIM provides Confidential Client Money accounts at FAB and ENBD plus secondary custodianship, freeing the EAM from holding client funds directly.'
  },
  {
    id: 'fund-launch-or-admin',
    label: 'Fund launching or needing admin upgrade',
    signals: [
      /\b(launching|launched|new)\s+(fund|vehicle)/i,
      /\b(general\s+partner|GP\s+at|fund\s+manager)\b/i,
      /\b(fund\s+(admin|administrator|administration|services))\b/i,
      /\b(NAV|fund\s+accounting|fund\s+operations)\b/i
    ],
    fcimService: 'Fund Administration',
    angle: 'Fund launch or admin pain — FCIM is one of only five UAE-authorised fund administrators, can serve as manager and administrator both for fast-tracked CMA approval (10 working days).'
  },
  {
    id: 'pre-ipo-or-ma',
    label: 'Pre-IPO or M&A advisory candidate',
    signals: [
      /\b(pre-?IPO|going\s+public|listing\s+plans)\b/i,
      /\b(M&A|mergers|acquisitions)\s+(advisor|target|strategy)/i,
      /\b(capital\s+raise|growth\s+equity|series\s+[CDE])\b/i,
      /\b(corporate\s+finance|capital\s+markets)\s+(director|head|managing)/i
    ],
    fcimService: 'IB & Advisory',
    angle: 'Capital-markets activity ahead — FCIM\u2019s IB desk (Dmitri Tchekalkine, 30+ yrs, ex-JPM/BNP/HSBC) covers ECM/DCM/M&A in the $50-150M band with UAE exchange listing capability.'
  },
  {
    id: 'discretionary-mandate',
    label: 'HNW with liquid capital seeking discretionary mandate',
    signals: [
      /\b(post-?exit|exited|sold\s+(my|the)\s+(company|business))\b/i,
      /\b(personal\s+investment\s+company|PIC)\b/i,
      /\b(family\s+wealth|personal\s+wealth|liquid\s+capital)\b/i,
      /\b(HNW|UHNW|high\s+net\s+worth)\b/i
    ],
    fcimService: 'Discretionary Portfolio Management',
    angle: 'Liquid personal capital seeking a managed mandate — five CMA-approved model portfolios spanning capital preservation through aggressive, $1M minimum entry.'
  },
  {
    id: 'sensitive-jurisdiction-banking',
    label: 'Banking access difficulty due to sensitive nationality / jurisdiction',
    signals: [
      /\b(Russian|Belarusian|Iranian|Syrian)\b/i,
      /\b(sanction|de-?risk|banking\s+access)\b/i,
      /\b(re-?domicile|relocation|UAE\s+residency)\b/i
    ],
    fcimService: 'Foundation + Private Fund',
    angle: 'Banking access friction due to nationality or jurisdiction — UAE-regulated Foundation + Private Fund structure passes KYC where personal-name holdings increasingly cannot.'
  }
];

// =========================================================================
// REGIONS
// =========================================================================

const REGIONS = [
  { name: 'MENA / Levant',    l​​​​​​​​​​​​​​​​
