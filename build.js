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
    profileScraperMode: 'Short',
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
  return {
    name: fullName,
    title: raw.headline || raw.title || raw.position || '',
    company: company,
    location: locText,
    linkedinUrl: raw.linkedinUrl || raw.profileUrl || raw.url || '',
    email: raw.email || raw.emailAddress || null,
    about: raw.about || raw.summary || '',
    _queryRegion: raw._queryRegion,
    _queryLabel: raw._queryLabel
  };
}

function classifyProfile(p) {
  const text = `${p.name} ${p.title} ${p.company} ${p.about} ${p.location}`.toLowerCase();
  const services = SERVICES.filter(s => s.match.test(text));
  return services.length ? services : [SERVICES[0]];
}

function runCompliance(p) {
  const text = `${p.name} ${p.title} ${p.company} ${p.about}`;
  for (const rx of COMPLIANCE_BLOCK) {
    if (rx.test(text)) return { allowed: false, reason: 'blocked' };
  }
  const pep = COMPLIANCE_WARN.test(text);
  return { allowed: true, pep };
}

function fingerprint(p) {
  return `${(p.name||'').toLowerCase().trim()}|${(p.linkedinUrl||'').toLowerCase().trim()}`;
}

function regionFor(p) {
  if (p._queryRegion) {
    const r = REGIONS.find(x => x.name === p._queryRegion);
    if (r) return r;
  }
  return REGIONS.find(x => x.name === 'UK / Western');
}

function escapeHtml(s) {
  return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
}

function buildDraftPrompt(p, service, region) {
  const lines = [
    `Draft a cold outreach email from Yehya Abdelbaki (Relationship Manager, FCIM) to:`,
    ``,
    `Name: ${p.name}`,
    `Title: ${p.title}`,
    `Company: ${p.company}`,
    `Location: ${p.location}`,
    `LinkedIn: ${p.linkedinUrl || 'n/a'}`,
    p.email ? `Email: ${p.email}` : `Email: not yet verified`,
    ``,
    `FCIM service to lead with: ${service.name}`,
    `Service one-liner: ${service.solution}`,
    ``,
    `Warm path: ${region.warmPath}`,
    region.lead ? `Internal lead for region: ${region.lead}` : ``,
    ``,
    `Hard rules:`,
    `- Lead with FCIM platform, not senior names.`,
    `- Never mention Gary Dugan.`,
    `- Ibrahim Hemeida and Amr Fergany are never the first word of the email.`,
    `- Tone: senior, restrained, specific. No marketing language.`,
    `- Keep under 150 words.`,
    `- Sign off:`,
    `  Yehya Abdelbaki / Relationship Manager / Fundament Capital Investment Management / yehya.abdelbaki@fundamentcapital.ae | +971 4 834 8385`
  ].filter(Boolean).join('\n');
  return lines;
}

function prospectCardHtml(p) {
  const services = classifyProfile(p);
  const primary = services[0];
  const region = regionFor(p);
  const compTag = p.pep
    ? `<span class="pill pill-warn">Compliance: PEP indicator — review before contact</span>`
    : `<span class="pill pill-ok">Compliance: clear on automated screen</span>`;
  const emailLine = p.email
    ? `<div class="kv"><span class="k">Email</span><span class="v">${escapeHtml(p.email)}</span></div>`
    : `<div class="kv"><span class="k">Email</span><span class="v muted">not yet verified — request enrichment</span></div>`;
  const draftPrompt = buildDraftPrompt(p, primary, region);
  const claudeUrl = `https://claude.ai/new?q=${encodeURIComponent(draftPrompt)}`;
  return `
  <article class="prospect">
    <header>
      <h3>${escapeHtml(p.name)}</h3>
      <div class="sub">${escapeHtml(p.title)}${p.company ? ' · ' + escapeHtml(p.company) : ''}</div>
      <div class="sub muted">${escapeHtml(p.location)}</div>
    </header>
    <div class="contact">
      ${emailLine}
      ${p.linkedinUrl ? `<div class="kv"><span class="k">LinkedIn</span><span class="v"><a href="${escapeHtml(p.linkedinUrl)}" target="_blank" rel="noopener">${escapeHtml(p.linkedinUrl)}</a></span></div>` : ''}
    </div>
    ${p.about ? `<div class="about"><span class="k">Background</span><p>${escapeHtml(p.about.slice(0,400))}${p.about.length>400?'…':''}</p></div>` : ''}
    <div class="solution"><span class="k">FCIM solution</span><p>${escapeHtml(primary.solution)}</p></div>
    <div class="warmpath"><span class="k">Warm path</span><p>${escapeHtml(region.warmPath)}</p></div>
    <div class="pills">${compTag}${region.lead?`<span class="pill">Lead: ${escapeHtml(region.lead)}</span>`:''}<span class="pill">Service: ${escapeHtml(primary.name)}</span></div>
    <div class="actions"><a class="btn" href="${claudeUrl}" target="_blank" rel="noopener">Open Claude with draft prompt</a></div>
  </article>`;
}

function renderRegionChips(regionCounts) {
  return REGIONS.map(r => {
    const count = regionCounts[r.name] || 0;
    return `<span class="chip"><span class="chip-name">${escapeHtml(r.name)}</span> <span class="chip-count">${count}</span>${r.lead?`<span class="chip-lead">${escapeHtml(r.lead.split(' ')[0])}</span>`:''}</span>`;
  }).join('');
}

function buildHtml({ totalProspects, profilesByService, regionCounts }) {
  const tpl = fs.readFileSync('index.template.html', 'utf8');
  const date = new Date().toLocaleDateString('en-GB', { weekday:'long', day:'numeric', month:'long', year:'numeric', timeZone:'Asia/Dubai' });
  const builtAt = new Date().toLocaleString('en-GB', { timeZone:'Asia/Dubai' });

  let council;
  if (totalProspects === 0) {
    council = `Council couldn't pull prospects today. Retry the Action.`;
  } else {
    council = `Council surfaced ${totalProspects} prospect${totalProspects===1?'':'s'} across ${Object.keys(profilesByService).length} service line${Object.keys(profilesByService).length===1?'':'s'}.`;
  }

  let featured = '';
  let featuredWrapperStyle = 'display:none';
  for (const svc of SERVICES) {
    const list = profilesByService[svc.name] || [];
    if (list.length) {
      featured = prospectCardHtml(list[0]);
      featuredWrapperStyle = '';
      break;
    }
  }

  const sections = SERVICES.map(svc => {
    const list = profilesByService[svc.name] || [];
    const cards = list.length
      ? list.map(prospectCardHtml).join('')
      : `<div class="empty">No prospects matched today.</div>`;
    return `
    <section class="service">
      <header class="service-head">
        <div>
          <h2>${escapeHtml(svc.name)}</h2>
          <p class="service-desc">${escapeHtml(svc.desc)}</p>
        </div>
        <div class="count">${list.length} <span>PROSPECT${list.length===1?'':'S'}</span></div>
      </header>
      <div class="cards">${cards}</div>
    </section>`;
  }).join('');

  return tpl
    .replace('{{DATE}}', escapeHtml(date))
    .replace('{{COUNCIL_LINE}}', escapeHtml(council))
    .replace('{{REGION_CHIPS}}', renderRegionChips(regionCounts))
    .replace('{{FEATURED}}', featured)
    .replace('{{FEATURED_WRAPPER_STYLE}}', featuredWrapperStyle)
    .replace('{{CONTENT}}', sections)
    .replace('{{BUILT_AT}}', escapeHtml(builtAt));
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

  const profilesByService = {};
  const regionCounts = {};
  for (const p of profiles) {
    const services = classifyProfile(p);
    const primary = services[0];
    if (!profilesByService[primary.name]) profilesByService[primary.name] = [];
    profilesByService[primary.name].push(p);
    const r = regionFor(p);
    regionCounts[r.name] = (regionCounts[r.name] || 0) + 1;
  }

  const html = buildHtml({
    totalProspects: profiles.length,
    profilesByService,
    regionCounts
  });

  fs.writeFileSync('index.html', html, 'utf8');
  console.log(`Built index.html — ${profiles.length} prospects`);
}

main().catch(e => { console.error(e); process.exit(1); });
