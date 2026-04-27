/* FCIM Daily Intelligence — daily builder v2
 * Runs in GitHub Actions on a schedule.
 * Calls Apify LinkedIn scraper to pull named individuals matching FCIM target filters,
 * classifies them into services/regions, applies compliance, writes index.html.
 */
const fs = require('node:fs');

const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) {
  console.error('FATAL: APIFY_TOKEN env var missing. Set it as a GitHub repo secret.');
  process.exit(1);
}

// Apify Actor for LinkedIn people search (no cookies, ~$3 per 1000 profiles)
const APIFY_ACTOR = 'harvestapi~linkedin-profile-search';

const SEARCH_QUERIES = [
  { label: 'Egyptian founders Dubai',     query: 'Egyptian founder Dubai',        region: 'Egypt' },
  { label: 'Lebanese private bankers',    query: 'Lebanese private banker Dubai', region: 'Lebanon / Levant' },
  { label: 'Indian family office Dubai',  query: 'Indian family office Dubai',    region: 'India' },
  { label: 'Russian CIS investors Dubai', query: 'Russian investor Dubai DMCC',   region: 'Russia / CIS' },
  { label: 'African family office Dubai', query: 'African family office Dubai',   region: 'Africa' },
  { label: 'DIFC wealth management',      query: 'DIFC wealth management',        region: null }
];

const PROFILES_PER_QUERY = 8;

const SERVICES = [
  { name: 'Portfolio Management',
    desc: 'Five CMA-approved models. USD 1M minimum. Risk-matched mandates.',
    solution: 'CMA portfolio management on a risk-matched model. Five CMA-approved portfolios spanning capital preservation through growth.',
    match: /\b(portfolio manag|wealth manager|wealth management|investment advisor|private banker|relationship manager|asset manager)\b/i },
  { name: 'CMA Private Funds',
    desc: 'Five-day approval. UBO privacy. FCIM as manager and administrator.',
    solution: 'CMA private fund with FCIM as manager and administrator. Five-day approval, UBO privacy.',
    match: /\b(fund manager|fund launch|private fund|general partner|GP\b|managing partner)\b/i },
  { name: 'Foundation + Private Fund',
    desc: 'UAE Foundation over CMA fund. Three-level control. EUR 100M+ restructure executed.',
    solution: 'UAE Foundation owning a CMA private fund. Three-level control, UBO privacy, onshore UAE governance.',
    match: /\b(family office|single family|multi-family|founder|chairman|owner|principal|entrepreneur|CEO)\b/i },
  { name: 'Commodity Derivatives',
    desc: 'SCA-licensed. Direct CME, ICE, LME, EEX, SGX. No clearing account required.',
    solution: 'SCA-licensed commodity derivatives desk, direct CME/ICE/LME/EEX/SGX access.',
    match: /\b(commodity|commodities|trader|trading|grain|metals|oil|DMCC)\b/i },
  { name: 'Fund Administration',
    desc: 'One of only five UAE-authorised fund administrators.',
    solution: 'One of only five UAE-authorised fund administrators. End-to-end operational chassis.',
    match: /\b(fund admin|fund services|NAV|fund accounting|operations|COO)\b/i },
  { name: 'IB & Advisory',
    desc: 'Tim Almashat-led. ECM, DCM, M&A in the USD 50-150M band.',
    solution: 'Tim Almashat-led IB desk. ECM, DCM, M&A in the USD 50-150M band with deep GCC family office sell-side coverage.',
    match: /\b(investment bank|M&A|mergers|advisor|corporate finance|capital markets)\b/i }
];

const REGIONS = [
  { name: 'Egypt',            lead: 'Ibrahim Hemeida',
    warmPath: 'Ibrahim Hemeida via the Egyptian Business Council Dubai and the Egyptian professional community in DIFC/Business Bay.' },
  { name: 'Lebanon / Levant', lead: 'Amr Fergany',
    warmPath: 'Amr Fergany via the Credit Suisse DIFC alumni network and Lebanese professional community in DIFC.' },
  { name: 'Russia / CIS',     lead: 'Dmitri Ganjour',
    warmPath: 'Dmitri Ganjour via Fertistream DMCC alumni and Russian business networks in DMCC/Business Bay.' },
  { name: 'India',            lead: 'Saran Sankar',
    warmPath: 'Saran Sankar via UBS Mumbai alumni and the Indian Business & Professional Council Dubai.' },
  { name: 'Africa',           lead: null,
    warmPath: 'Approach via Dubai-based African diaspora professional networks. Lead assignment TBD.' },
  { name: 'UK / Western',     lead: 'Steven Downey',
    warmPath: 'Steven Downey via the UK finance community in DIFC and CFA Society UAE.' }
];

const COMPLIANCE_BLOCK = [
  /\bgary\s+dugan\b/i,
  /\b(al\s+maktoum|bin\s+rashid\s+al\s+maktoum|mohammed\s+bin\s+rashid|hamdan\s+bin\s+mohammed)\b/i,
  /\b(al\s+nahyan|bin\s+zayed\s+al\s+nahyan|mohamed\s+bin\s+zayed)\b/i,
  /\b(prigozhin|usmanov|deripaska|abramovich|vekselberg|rotenberg|fridman)\b/i,
  /\barqaam\s+capital\b/i,
  /\b(mashreq|emirates\s+nbd|enbd)\b/i
];
const COMPLIANCE_WARN = /\b(PEP|politically\s+exposed|state[- ]owned|sovereign\s+wealth|ministry\s+of)\b/i;

async function runApifyActor(queryObj) {
  const url = `https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?token=${APIFY_TOKEN}`;
  const body = {
    searchQuery: queryObj.query,
    locations: ['United Arab Emirates'],
    profileScraperMode: 'Full + email search',
    takePages: 1,
    maxItems: PROFILES_PER_QUERY
  };
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!res.ok) {
      const text = await res.text();
      console.warn(`Apify "${queryObj.label}": HTTP ${res.status} — ${text.slice(0,200)}`);
      return [];
    }
    const data = await res.json();
    if (!Array.isArray(data)) {
      console.warn(`Apify "${queryObj.label}": unexpected shape`);
      return [];
    }
    return data.map(p => ({ ...p, _queryRegion: queryObj.region, _queryLabel: queryObj.label }));
  } catch (e) {
    console.warn(`Apify "${queryObj.label}" failed: ${e.message}`);
    return [];
  }
}

function normaliseProfile(raw) {
  const fullName = (raw.firstName || raw.lastName)
    ? `${raw.firstName || ''} ${raw.lastName || ''}`.trim()
    : (raw.fullName || raw.name || 'Name unavailable');
  const company = (Array.isArray(raw.currentPosition) && raw.currentPosition[0])
    ? (raw.currentPosition[0].companyName || '')
    : (raw.currentCompany || raw.company || '');
  const locText = (raw.location && typeof raw.location === 'object')
    ? (raw.location.linkedinText || (raw.location.parsed && raw.location.parsed.text) || 'Dubai')
    : (raw.location || 'Dubai');
  // Prefer clean URL built from publicIdentifier; fall back to whatever the API returned.
  // URN-style identifiers (start with ACoA / ACwA) make ugly URLs but still resolve.
  const slug = raw.publicIdentifier && !/^AC[oOwW]A/.test(raw.publicIdentifier)
    ? raw.publicIdentifier
    : null;
  const linkedinUrl = slug
    ? `https://www.linkedin.com/in/${slug}`
    : (raw.linkedinUrl || raw.profileUrl || raw.url || '');
  return {
    name: fullName,
    title: raw.headline || raw.title || raw.position || '',
    company: company,
    location: locText,
    linkedinUrl: linkedinUrl,
    email: raw.email || raw.emailAddress || null,
    about: raw.about || raw.summary || '',
    _queryRegion: raw._queryRegion,
    _queryLabel: raw._queryLabel
  };
}

function classifyProfile(p) {
  const text = `${p.name} ${p.title} ${p.company} ${p.about} ${p.location}`.toLowerCase();
  let primaryService = null;
  for (const svc of SERVICES) {
    if (svc.match.test(text)) { primaryService = svc.name; break; }
  }
  let region = p._queryRegion || null;
  if (!region) {
    if (/\b(egypt|cairo|alexandria)\b/i.test(text)) region = 'Egypt';
    else if (/\b(lebanon|lebanese|beirut|levant|jordan|syria)\b/i.test(text)) region = 'Lebanon / Levant';
    else if (/\b(russia|russian|moscow|cis|kazakh|ukrain|belarus)\b/i.test(text)) region = 'Russia / CIS';
    else if (/\b(india|indian|mumbai|delhi|bangalore|chennai|gurgaon)\b/i.test(text)) region = 'India';
    else if (/\b(nigeri|kenya|south\s+africa|ghana|egypt|morocco|tunisia|algeri|senegal|ethiopia|tanzania|uganda|ivory\s+coast|cote\s+d)\b/i.test(text)) region = 'Africa';
    else if (/\b(london|england|britain|british|uk\b|scotland|ireland|german|swiss|switzerland)\b/i.test(text)) region = 'UK / Western';
  }
  const regionMeta = REGIONS.find(r => r.name === region);
  return {
    primaryService,
    region,
    regionLead: regionMeta ? regionMeta.lead : null,
    regionWarmPath: regionMeta ? regionMeta.warmPath : null
  };
}

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

function decodeEntities(s) {
  return String(s == null ? '' : s)
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&nbsp;/g, ' ');
}

function escapeHtml(s) {
  return decodeEntities(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

function buildDraftPrompt(p) {
  return `Draft Yehya Abdelbaki's FCIM outreach based on this live prospect.

Name: ${p.name}
Title: ${p.title}
Company: ${p.company}
Location: ${p.location}
LinkedIn: ${p.linkedinUrl}
${p.email ? `Email (to verify): ${p.email}` : 'Email: not found — verify via LinkedIn InMail or company site'}

Matched FCIM service: ${p.primaryService || '(pick best fit)'}
Region: ${p.region || '(not detected)'}
Suggested lead: ${p.regionLead || '(route by context)'}

Write a <200 word email in Yehya's voice. Reference the prospect's role and firm. Mention the FCIM colleague naturally only where it adds credibility. End with the full Yehya signature block (Yehya Abdelbaki / Relationship Manager / Fundament Capital Investment Management / yehya.abdelbaki@fundamentcapital.ae | +971 4 834 8385).`;
}

function prospectCardHtml(p, isFeatured) {
  const service = SERVICES.find(s => s.name === p.primaryService);
  const solutionText = service ? service.solution : 'Service match indeterminate — review the profile to assess which FCIM product fits.';
  const warmPath = p.regionWarmPath || 'Region indeterminate. Route to the FCIM colleague best matched to the subject\u2019s nationality/sector.';
  const complianceBlock = p.pep
    ? `<div class="compliance pep"><span class="label">Elevated DD</span>PEP / state-linked exposure detected. Enhanced due diligence required before outreach.</div>`
    : '';
  const emailLine = p.email
    ? `${escapeHtml(p.email)} <em>(verify before sending)</em>`
    : `<em>not found \u2014 use LinkedIn InMail or company site</em>`;
  const prompt = buildDraftPrompt(p);

  return `
    <article class="prospect ${isFeatured ? 'featured' : ''}">
      <div class="service-tag">${escapeHtml(p.primaryService || 'Market context')}</div>
      <div class="head-row">
        <div class="head-main">
          <h3>${escapeHtml(p.name)}</h3>
          <div class="sub">
            ${escapeHtml(p.title)}${p.company ? `<span class="dot">\u00b7</span>${escapeHtml(p.company)}` : ''}${p.region ? `<span class="dot">\u00b7</span>${escapeHtml(p.region)}` : ''}
          </div>
        </div>
        ${p.regionLead ? `<div class="lead-block"><span class="lead-label">Lead</span>${escapeHtml(p.regionLead)}</div>` : ''}
      </div>
      <div class="section">
        <div class="label">Contact</div>
        <p>
          <strong>LinkedIn:</strong> ${p.linkedinUrl ? `<a href="${escapeHtml(p.linkedinUrl)}" target="_blank" rel="noopener noreferrer">profile \u203a</a>` : '<em>not available</em>'}<br>
          <strong>Email:</strong> ${emailLine}
        </p>
      </div>
      ${p.about ? `<div class="section"><div class="label">Background</div><p>${escapeHtml(p.about.slice(0, 300))}${p.about.length > 300 ? '\u2026' : ''}</p></div>` : ''}
      <div class="section">
        <div class="label">FCIM Solution</div>
        <p>${escapeHtml(solutionText)}</p>
      </div>
      <div class="section">
        <div class="label">Warm Path</div>
        <p>${escapeHtml(warmPath)}</p>
      </div>
      ${complianceBlock}
      <div class="first-step">
        <div class="label">First Step</div>
        <p>Approach ${escapeHtml(p.name)} via ${p.regionLead ? escapeHtml(p.regionLead) + '\u2019s network' : 'the appropriate FCIM colleague'}. Tap below to copy a draft prompt \u2014 paste it in your Claude chat and Yehya\u2019s email comes back.</p>
        <button class="draft-btn" data-prompt="${escapeHtml(prompt)}">Copy draft prompt</button>
      </div>
    </article>
  `;
}

function renderServiceSection(svc, items) {
  if (items.length === 0) {
    return `
      <section class="service-section empty">
        <div class="service-header">
          <div class="left">
            <h2>${escapeHtml(svc.name)}</h2>
            <p>${escapeHtml(svc.desc)}</p>
          </div>
          <div class="right-meta">No prospects today</div>
        </div>
      </section>`;
  }

  // Group prospects under this service by region, in REGIONS display order.
  const byRegion = {};
  for (const p of items) {
    const key = p.region || '__unrouted';
    if (!byRegion[key]) byRegion[key] = [];
    byRegion[key].push(p);
  }
  const regionOrder = REGIONS.map(r => r.name).filter(n => byRegion[n]);
  if (byRegion.__unrouted) regionOrder.push('__unrouted');

  // Inline styles used here (not in template) so a single build.js change ships the new layout.
  const REGION_BLOCK_STYLE = 'margin: 28px 0 8px; padding: 14px 18px; background: var(--cream-yellow, #F6EFD8); border-left: 3px solid var(--mustard, #C9A544); border-radius: 4px;';
  const REGION_HEADER_STYLE = 'display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;';
  const REGION_NAME_STYLE = 'font-family: \'Fraunces\', ui-serif, Georgia, serif; font-style: italic; font-weight: 500; font-size: 19px; margin: 0; color: var(--ink, #1C1A16);';
  const REGION_META_STYLE = 'font-size: 11px; letter-spacing: 0.14em; text-transform: uppercase; color: var(--mustard-deep, #6F561A); font-weight: 600;';

  const regionBlocks = regionOrder.map(rname => {
    const ps = byRegion[rname];
    const r = REGIONS.find(rr => rr.name === rname);
    const displayName = (rname === '__unrouted') ? 'Unrouted' : rname;
    const leadName = (rname === '__unrouted')
      ? 'Route by context'
      : (r && r.lead ? r.lead : 'Lead TBD');
    return `
      <div class="region-block" style="${REGION_BLOCK_STYLE}">
        <div class="region-header" style="${REGION_HEADER_STYLE}">
          <h3 style="${REGION_NAME_STYLE}">${escapeHtml(displayName)}</h3>
          <div style="${REGION_META_STYLE}">${escapeHtml(leadName)} \u00b7 ${ps.length} prospect${ps.length === 1 ? '' : 's'}</div>
        </div>
      </div>
      <div class="items">${ps.map(p => prospectCardHtml(p, false)).join('')}</div>`;
  }).join('');

  return `
    <section class="service-section">
      <div class="service-header">
        <div class="left">
          <h2>${escapeHtml(svc.name)}</h2>
          <p>${escapeHtml(svc.desc)}</p>
        </div>
        <div class="right-meta">${items.length} prospect${items.length === 1 ? '' : 's'} \u00b7 ${regionOrder.length} region${regionOrder.length === 1 ? '' : 's'}</div>
      </div>
      ${regionBlocks}
    </section>`;
}

function renderRegionChips(regionCounts) {
  return REGIONS.map(r => {
    const n = regionCounts[r.name] || 0;
    const isEmpty = n === 0;
    const sep = '<span aria-hidden="true" style="opacity:0.4; margin:0 4px;">\u00b7</span>';
    return `
      <button class="region-chip ${isEmpty ? 'empty' : ''}" ${isEmpty ? 'disabled' : `data-region="${escapeHtml(r.name)}"`}>
        <span>${escapeHtml(r.name)}</span>${sep}<span class="count">${n}</span>${r.lead ? `${sep}<span class="lead">${escapeHtml(r.lead.split(' ')[0])}</span>` : ''}
      </button>`;
  }).join('');
}

async function main() {
  console.log('FCIM Daily Build v2 — starting');

  const results = await Promise.all(SEARCH_QUERIES.map(runApifyActor));
  const raw = results.flat();
  console.log(`Raw profiles: ${raw.length}`);

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
    if (!fp || fp === '|' || seen.has(fp)) return false;
    seen.add(fp);
    return true;
  });
  console.log(`After compliance+dedupe: ${profiles.length} (blocked: ${blocked})`);

  profiles.forEach(p => Object.assign(p, classifyProfile(p)));

  const featured = profiles.find(p => !p.pep && p.primaryService && p.region) || profiles[0] || null;
  const remaining = featured ? profiles.filter(p => fingerprint(p) !== fingerprint(featured)) : profiles;

  let servicesHtml = '';
  let emptyServicesHtml = '';
  for (const svc of SERVICES) {
    const items = remaining.filter(p => p.primaryService === svc.name);
    const html = renderServiceSection(svc, items);
    if (items.length === 0) emptyServicesHtml += html;
    else servicesHtml += html;
  }

  const unclassified = remaining.filter(p => !p.primaryService);
  if (unclassified.length) {
    servicesHtml += `
      <section class="service-section">
        <div class="service-header">
          <div class="left">
            <h2>Unclassified prospects</h2>
            <p>Profiles pulled but not auto-matched to a specific FCIM service \u2014 review manually.</p>
          </div>
          <div class="right-meta">${unclassified.length} prospect${unclassified.length === 1 ? '' : 's'}</div>
        </div>
        <div class="items">${unclassified.slice(0, 10).map(p => prospectCardHtml(p, false)).join('')}</div>
      </section>`;
  }

  const regionCounts = {};
  REGIONS.forEach(r => regionCounts[r.name] = 0);
  profiles.forEach(p => { if (p.region && regionCounts[p.region] !== undefined) regionCounts[p.region]++; });

  const dateStamp = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Dubai', weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }).format(new Date());
  const councilLine = featured
    ? `Council convened — ${profiles.length} named prospects scanned via LinkedIn, ${blocked} blocked on compliance, ${Object.values(regionCounts).filter(n => n > 0).length} regions active.`
    : `Council couldn\u2019t pull fresh prospects today. ${profiles.length} returned, ${blocked} blocked on compliance. Retry the Action.`;

  const template = fs.readFileSync('index.template.html', 'utf-8');
  const html = template
    .replace(/\{\{DATE\}\}/g, escapeHtml(dateStamp))
    .replace(/\{\{BUILT_AT\}\}/g, escapeHtml(new Date().toISOString()))
    .replace(/\{\{COUNCIL_LINE\}\}/g, escapeHtml(councilLine))
    .replace(/\{\{REGION_CHIPS\}\}/g, renderRegionChips(regionCounts))
    .replace(/\{\{FEATURED\}\}/g, featured ? prospectCardHtml(featured, true) : '')
    .replace(/\{\{CONTENT\}\}/g, servicesHtml + emptyServicesHtml)
    .replace(/\{\{FEATURED_WRAPPER_STYLE\}\}/g, featured ? '' : 'display:none');

  fs.writeFileSync('index.html', html);
  console.log(`Built index.html — ${profiles.length} prospects, featured: ${featured ? featured.name : 'none'}`);
}

main().catch(err => { console.error(err); process.exit(1); });
