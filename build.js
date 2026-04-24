/* FCIM Daily Intelligence — daily builder v2  * Runs in GitHub Actions on a schedule.
 * Calls Apify LinkedIn scraper to pull named individuals matching FCIM target fi  * classifies them into services/regions, applies compliance, writes index.html.
 */ const fs = require('node:fs');
const APIFY_TOKEN = process.env.APIFY_TOKEN; if (!APIFY_TOKEN) {   console.error('FATAL: APIFY_TOKEN env var missing. Set it as a GitHub repo secr   process.exit(1); }
// Apify Actor for LinkedIn people search (no cookies, ~$3 per 1000 profiles) const APIFY_ACTOR = 'dev_fusion~linkedin-profile-search-scraper';
const SEARCH_QUERIES = [
  { label: 'Egyptian founders Dubai',     query: 'Egyptian founder Dubai',       
  { label: 'Lebanese private bankers',    query: 'Lebanese private banker Dubai',
  { label: 'Indian family office Dubai',  query: 'Indian family office Dubai',   
  { label: 'Russian CIS investors Dubai', query: 'Russian investor Dubai DMCC',  
  { label: 'African family office Dubai', query: 'African family office Dubai',    { label: 'DIFC wealth management',      query: 'DIFC wealth management',       
]; const PROFILES_PER_QUERY = 8;
const SERVICES = [
  { name: 'Portfolio Management',     desc: 'Five CMA-approved models. USD 1M minimum. Risk-matched mandates.',     solution: 'CMA portfolio management on a risk-matched model. Five CMA-approve     match: /\b(portfolio manag|wealth manager|wealth management|investment adviso   { name: 'CMA Private Funds',     desc: 'Five-day approval. UBO privacy. FCIM as manager and administrator.',     solution: 'CMA private fund with FCIM as manager and administrator. Five-day     match: /\b(fund manager|fund launch|private fund|general partner|GP\b|managin   { name: 'Foundation + Private Fund',     desc: 'UAE Foundation over CMA fund. Three-level control. EUR 100M+ restructu     solution: 'UAE Foundation owning a CMA private fund. Three-level control, UBO     match: /\b(family office|single family|multi-family|founder|chairman|owner|pr   { name: 'Commodity Derivatives',     desc: 'SCA-licensed. Direct CME, ICE, LME, EEX, SGX. No clearing account requ     solution: 'SCA-licensed commodity derivatives desk, direct CME/ICE/LME/EEX/SG     match: /\b(commodity|commodities|trader|trading|grain|metals|oil|DMCC)\b/i },   { name: 'Fund Administration',     desc: 'One of only five UAE-authorised fund administrators.',     solution: 'One of only five UAE-authorised fund administrators. End-to-end op     match: /\b(fund admin|fund services|NAV|fund accounting|operations|COO)\b/i }
  { name: 'IB & Advisory',     desc: 'Tim Almashat-led. ECM, DCM, M&A in the USD 50-150M band.',     solution: 'Tim Almashat-led IB desk. ECM, DCM, M&A in the USD 50-150M band wi     match: /\b(investment bank|M&A|mergers|advisor|corporate finance|capital mark
];
const REGIONS = [
  { name: 'Egypt',            lead: 'Ibrahim Hemeida',     warmPath: 'Ibrahim Hemeida via the Egyptian Business Council Dubai and the Eg   { name: 'Lebanon / Levant', lead: 'Amr Fergany',     warmPath: 'Amr Fergany via the Credit Suisse DIFC alumni network and Lebanese   { name: 'Russia / CIS',     lead: 'Dmitri Ganjour',     warmPath: 'Dmitri Ganjour via Fertistream DMCC alumni and Russian business ne   { name: 'India',            lead: 'Saran Sankar',     warmPath: 'Saran Sankar via UBS Mumbai alumni and the Indian Business & Profe   { name: 'Africa',           lead: null,     warmPath: 'Approach via Dubai-based African diaspora professional networks. L   { name: 'UK / Western',     lead: 'Steven Downey',     warmPath: 'Steven Downey via the UK finance community in DIFC and CFA Society
];
const COMPLIANCE_BLOCK = [   /\bgary\s+dugan\b/i,
  /\b(al\s+maktoum|bin\s+rashid\s+al\s+maktoum|mohammed\s+bin\s+rashid|hamdan\s+b
  /\b(al\s+nahyan|bin\s+zayed\s+al\s+nahyan|mohamed\s+bin\s+zayed)\b/i,
  /\b(prigozhin|usmanov|deripaska|abramovich|vekselberg|rotenberg|fridman)\b/i,
  /\barqaam\s+capital\b/i,   /\b(mashreq|emirates\s+nbd|enbd)\b/i
]; const COMPLIANCE_WARN = /\b(PEP|politically\s+exposed|state[- ]owned|sovereign\s+
async function runApifyActor(queryObj) {   const url = `https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset  const body = {     searchQueries: [queryObj.query],     maxProfiles: PROFILES_PER_QUERY,     location: 'Dubai, United Arab Emirates'
  };   try {     const res = await fetch(url, {       method: 'POST',       headers: { 'Content-Type': 'application/json' },       body: JSON.stringify(body)
    });     if (!res.ok) {       const text = await res.text();       console.warn(`Apify "${queryObj.label}": HTTP ${res.status} — ${text.slice(       return [];
    }     const data = await res.json();     if (!Array.isArray(data)) {       console.warn(`Apify "${queryObj.label}": unexpected shape`);       return [];
    }     return data.map(p => ({ ...p, _queryRegion: queryObj.region, _queryLabel: que
  } catch (e) {     console.warn(`Apify "${queryObj.label}" failed: ${e.message}`);     return [];
  } }
function normaliseProfile(raw) {   return {     name: raw.fullName || raw.name || 'Name unavailable',     title: raw.currentPosition || raw.headline || raw.position || '',     company: raw.currentCompany || raw.company || '',     location: raw.location || 'Dubai',     linkedinUrl: raw.profileUrl || raw.url || raw.linkedinUrl || '',     email: raw.email || null,
    about: raw.about || raw.summary || '',     _queryRegion: raw._queryRegion,
    _queryLabel: raw._queryLabel
  }; }
function classifyProfile(p) {   const text = `${p.name} ${p.title} ${p.company} ${p.about} ${p.location}`.toLow   const services = [];   for (const s of SERVICES) if (s.match.test(text) && !services.includes(s.name))   const region = REGIONS.find(r => r.name === p._queryRegion) || null;   return {     primaryService: services[0] || null,     region: region ? region.name : null,     regionLead: region ? region.lead : null,     regionWarmPath: region ? region.warmPath : null
  }; }
function runCompliance(p) {   const text = `${p.name} ${p.title} ${p.company} ${p.about}`;   for (const rule of COMPLIANCE_BLOCK) if (rule.test(text)) return { allowed: fal   if (COMPLIANCE_WARN.test(text)) return { allowed: true, pep: true };   return { allowed: true };
}
function fingerprint(p) {   return `${(p.name || '').toLowerCase().trim()}|${(p.company || '').toLowerCase(
}
function escapeHtml(s) {
  return String(s == null ? '' : s)     .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}
function buildDraftPrompt(p) {   return `Draft Yehya Abdelbaki's FCIM outreach based on this live prospect.
Name: ${p.name}
Title: ${p.title}
Company: ${p.company}
Location: ${p.location}
LinkedIn: ${p.linkedinUrl}
${p.email ? `Email (to verify): ${p.email}` : 'Email: not found — verify via Link
Matched FCIM service: ${p.primaryService || '(pick best fit)'}
Region: ${p.region || '(not detected)'}
Suggested lead: ${p.regionLead || '(route by context)'}
Write a <200 word email in Yehya's voice. Reference the prospect's role and firm.
}
function prospectCardHtml(p, isFeatured) {   const service = SERVICES.find(s => s.name === p.primaryService);   const solutionText = service ? service.solution : 'Service match indeterminate   const warmPath = p.regionWarmPath || 'Region indeterminate. Route to the FCIM c   const complianceBlock = p.pep     ? `<div class="compliance pep"><span class="label">Elevated DD</span>PEP / st
    : '';   const emailLine = p.email
    ? `<strong>Email:</strong> ${escapeHtml(p.email)} <em>(verify before sending)     : `<em>Email not found — use LinkedIn InMail or look up via company site</em>   const prompt = buildDraftPrompt(p);
  return `
    <article class="prospect ${isFeatured ? 'featured' : ''}">
      <div class="service-tag">${escapeHtml(p.primaryService || 'Market context')       <div class="head-row">
        <div class="head-main">
          <h3>${escapeHtml(p.name)}</h3>
          <div class="sub">
            ${escapeHtml(p.title)}${p.company ? `<span class="dot">\u00b7</span>$
          </div>
        </div>
        ${p.regionLead ? `<div class="lead-block"><span class="lead-label">Lead</
      </div>
      <div class="section">         <div class="label">Contact</div>
        <p>
          ${p.linkedinUrl ? `<a href="${escapeHtml(p.linkedinUrl)}" target="_blan
          ${emailLine}
        </p>
      </div>
      ${p.about ? `<div class="section"><div class="label">Background</div><p>${e
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
        <p>Approach ${escapeHtml(p.name)} via ${p.regionLead ? escapeHtml(p.regio         <button class="draft-btn" data-prompt="${escapeHtml(prompt)}">Copy draft 
      </div>
    </article>
  `; }
function renderServiceSection(svc, items) {   return `
    <section class="service-section">
      <div class="service-header">
        <div class="left">
          <h2>${escapeHtml(svc.name)}</h2>
          <p>${escapeHtml(svc.desc)}</p>
        </div>
        <div class="right-meta">${items.length} prospect${items.length === 1 ? ''
      </div>
      ${items.length === 0
        ? `<div class="empty-note">No prospects matched for this service today.</         : `<div class="items">${items.map(p => prospectCardHtml(p, false)).join('       }
    </section>`;
}
function renderRegionChips(regionCounts) {   return REGIONS.map(r => {     const n = regionCounts[r.name] || 0;     const isEmpty = n === 0;     return `
      <button class="region-chip ${isEmpty ? 'empty' : ''}" ${isEmpty ? 'disabled
        <span>${escapeHtml(r.name)}</span>
        <span class="count">${n}</span>         ${r.lead ? `<span class="lead">${escapeHtml(r.lead.split(' ')[0])}</span>
      </button>`;   }).join('');
}
async function main() {   console.log('FCIM Daily Build v2 — starting');
  const results = await Promise.all(SEARCH_QUERIES.map(runApifyActor));   const raw = results.flat();   console.log(`Raw profiles: ${raw.length}`);   let profiles = raw.map(normaliseProfile);
  let blocked = 0;   profiles = profiles.filter(p => {     const c = runCompliance(p);     if (!c.allowed) { blocked++; return false; }
    p.pep = !!c.pep;     return true;
  });
  const seen = new Set();   profiles = profiles.filter(p => {     const fp = fingerprint(p);     if (!fp || fp === '|' || seen.has(fp)) return false;     seen.add(fp);     return true;
  });   console.log(`After compliance+dedupe: ${profiles.length} (blocked: ${blocked})`   profiles.forEach(p => Object.assign(p, classifyProfile(p)));   const featured = profiles.find(p => !p.pep && p.primaryService && p.region) ||   const remaining = featured ? profiles.filter(p => fingerprint(p) !== fingerprin
  let servicesHtml = '';   let emptyServicesHtml = '';   for (const svc of SERVICES) {     const items = remaining.filter(p => p.primaryService === svc.name);     const html = renderServiceSection(svc, items);     if (items.length === 0) emptyServicesHtml += html;     else servicesHtml += html;
  }
  const unclassified = remaining.filter(p => !p.primaryService);   if (unclassified.length) {     servicesHtml += `
      <section class="service-section">
        <div class="service-header">
          <div class="left">
            <h2>Unclassified prospects</h2>             <p>Profiles pulled but not auto-matched to a specific FCIM service \u           </div>
          <div class="right-meta">${unclassified.length} prospect${unclassified.l         </div>
        <div class="items">${unclassified.slice(0, 10).map(p => prospectCardHtml(
      </section>`;
  }
  const regionCounts = {};   REGIONS.forEach(r => regionCounts[r.name] = 0);   profiles.forEach(p => { if (p.region && regionCounts[p.region] !== undefined) r
  const dateStamp = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Dubai', we   const councilLine = featured     ? `Council convened — ${profiles.length} named prospects scanned via LinkedIn     : `Council couldn\u2019t pull fresh prospects today. ${profiles.length} retur
  const template = fs.readFileSync('index.template.html', 'utf-8');   const html = template
    .replace(/\{\{DATE\}\}/g, escapeHtml(dateStamp))
    .replace(/\{\{BUILT_AT\}\}/g, escapeHtml(new Date().toISOString()))
    .replace(/\{\{COUNCIL_LINE\}\}/g, escapeHtml(councilLine))
    .replace(/\{\{REGION_CHIPS\}\}/g, renderRegionChips(regionCounts))
    .replace(/\{\{FEATURED\}\}/g, featured ? prospectCardHtml(featured, true) : '
    .replace(/\{\{CONTENT\}\}/g, servicesHtml + emptyServicesHtml)     .replace(/\{\{FEATURED_WRAPPER_STYLE\}\}/g, featured ? '' : 'display:none');
  fs.writeFileSync('index.html', html);   console.log(`Built index.html — ${profiles.length} prospects, featured: ${featu } main().catch(err => { console.error(err); process.exit(1); });
