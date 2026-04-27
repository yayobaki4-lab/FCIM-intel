/* FCIM Daily Intelligence v3 */
const fs = require(`node:fs`);

const APIFY_TOKEN = process.env.APIFY_TOKEN;
if (!APIFY_TOKEN) { console.error(`FATAL: APIFY_TOKEN missing`); process.exit(1); }
const HUNTER_API_KEY = process.env.HUNTER_API_KEY || null;
if (!HUNTER_API_KEY) console.warn(`NOTE: HUNTER_API_KEY missing`);

const APIFY_ACTOR = `harvestapi~linkedin-profile-search`;
const QUALITY_THRESHOLD = 5;
const MAX_HUNTER_CALLS_PER_RUN = 25;

const QUERY_BUCKETS = [
  { label: `Armenian Dubai`, body: { searchQuery: `Armenian Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: `Caucasus / CIS`, serviceHint: `Foundation + Private Fund` },
  { label: `Russian Dubai`, body: { searchQuery: `Russian Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: `Russia / CIS`, serviceHint: `Foundation + Private Fund` },
  { label: `Nigerian Dubai`, body: { searchQuery: `Nigerian Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: `Africa`, serviceHint: `Foundation + Private Fund` },
  { label: `Lebanese Dubai`, body: { searchQuery: `Lebanese Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: `MENA / Levant`, serviceHint: `Foundation + Private Fund` },
  { label: `Egyptian Dubai`, body: { searchQuery: `Egyptian Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: `Egypt`, serviceHint: `Foundation + Private Fund` },
  { label: `Commodity Dubai`, body: { searchQuery: `commodity trader Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: null, serviceHint: `Commodity Derivatives` },
  { label: `Family office Dubai`, body: { searchQuery: `family office Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: null, serviceHint: `Foundation + Private Fund` },
  { label: `Wealth manager Dubai`, body: { searchQuery: `wealth manager Dubai`, profileScraperMode: `Short ($4 per 1k)`, takePages: 1 }, region: null, serviceHint: `Discretionary Portfolio Management` }
];

function pickTodaysQueries() {
  const start = new Date(Date.UTC(new Date().getUTCFullYear(), 0, 0));
  const day = Math.floor((Date.now() - start.getTime()) / 86400000);
  return [QUERY_BUCKETS[day % QUERY_BUCKETS.length], QUERY_BUCKETS[(day + 1) % QUERY_BUCKETS.length]];
}

const SERVICES = [
  { name: `Discretionary Portfolio Management`, desc: `Five CMA-approved models. USD 1M minimum.`, solution: `CMA-approved discretionary mandate matched to risk profile.` },
  { name: `Foundation + Private Fund`, desc: `UAE Foundation + CMA Private Fund. UBO privacy. 10-day approval.`, solution: `UAE Foundation owning a CMA Private Fund. UBO known only to FCIM and regulator. Used for $15-20M acquisitions and $100M+ real estate restructures.` },
  { name: `Commodity Derivatives`, desc: `SCA-licensed CME/ICE/LME/EEX/SGX access without clearing account.`, solution: `SCA-licensed platform giving direct CME/ICE/LME access without client clearing setup.` },
  { name: `Fund Administration`, desc: `One of five UAE-authorised fund administrators.`, solution: `Fund admin for in-house or third-party funds, NAV calculation, regulatory reporting.` },
  { name: `IB & Advisory`, desc: `Dmitri Tchekalkine-led desk. ECM/DCM/M&A.`, solution: `IB and advisory in $50-150M band. Led by Dmitri Tchekalkine, 30+ years EM, ex-JPM/BNP/HSBC.` },
  { name: `Family Office Advisory`, desc: `Governance, succession, estate, concierge.`, solution: `Full family office build: governance, succession, estate, VC/PE deal access.` },
  { name: `EAM / FI Platform`, desc: `Confidential Client Money accounts at FAB and ENBD.`, solution: `Platform for EAMs and FIs with FAB/ENBD client money accounts and secondary custodianship.` }
];

const PROBLEMS = [
  { id: `multi-venture`, label: `Multiple ventures needing structured holdco`, signals: [/\b(serial|multiple)\s+(entrepreneur|founder|ventures|companies)/i, /\b(investment\s+holding|holding\s+company|group\s+chairman)/i], fcimService: `Foundation + Private Fund`, angle: `Multiple ventures held under personal name or scattered SPVs - Foundation + Private Fund provides one umbrella with three-level control and clean UBO privacy.` },
  { id: `commodity-hedging`, label: `Physical commodity exposure without hedging`, signals: [/\b(physical\s+commodity|commodity\s+trad)/i, /\b(grain|fertili[sz]er|freight|metals|energy)\s+(trad|hedg)/i], fcimService: `Commodity Derivatives`, angle: `Physical commodity exposure without exchange-cleared hedging - FCIM SCA-licensed platform gives direct CME/ICE/LME access without client clearing setup.` },
  { id: `eam-platform`, label: `EAM looking for client-money platform`, signals: [/\b(external\s+asset\s+manager|EAM)\b/i, /\b(independent\s+(wealth|financial))/i, /\b(boutique|managing\s+partner).*(wealth|advisory)/i], fcimService: `EAM / FI Platform`, angle: `EAM needs regulated platform - FCIM provides Confidential Client Money accounts at FAB and ENBD plus secondary custodianship.` },
  { id: `family-succession`, label: `Family succession / generational transition`, signals: [/\b(next\s+generation|2G|3G)/i, /\b(succession|legacy|inheritance|estate)\s+(planning|transition)/i, /\bfamily\s+(business|enterprise|trust|council)/i], fcimService: `Family Office Advisory`, angle: `Family succession and generational transition - full family office build covering governance, succession, estate, concierge.` },
  { id: `pre-ipo`, label: `Pre-IPO or M&A advisory candidate`, signals: [/\b(pre-?IPO|going\s+public|listing\s+plans)/i, /\b(M&A|mergers|acquisitions)\s+(advisor|target|strategy)/i, /\b(capital\s+raise|growth\s+equity)/i], fcimService: `IB & Advisory`, angle: `Capital-markets activity ahead - FCIM IB desk (Dmitri Tchekalkine, 30+ yrs ex-JPM/BNP/HSBC) covers ECM/DCM/M&A in $50-150M band.` },
  { id: `discretionary`, label: `HNW seeking discretionary mandate`, signals: [/\b(post-?exit|exited|sold\s+(my|the))/i, /\b(personal\s+investment\s+company|PIC)/i, /\b(HNW|UHNW|high\s+net\s+worth)/i], fcimService: `Discretionary Portfolio Management`, angle: `Liquid personal capital seeking managed mandate - five CMA-approved model portfolios, $1M minimum entry.` }
];

const REGIONS = [
  { name: `MENA / Levant`, lead: `Amr Fergany`, warmPath: `Amr Fergany via Credit Suisse DIFC alumni network and Levantine community in DIFC and Business Bay.` },
  { name: `Egypt`, lead: `Ibrahim Hemeida`, warmPath: `Ibrahim Hemeida via Egyptian Business Council Dubai and his Egyptian banking-sector relationships.` },
  { name: `Russia / CIS`, lead: `Dmitri Tchekalkine`, warmPath: `Dmitri Tchekalkine via 30+ years EM banking at Chemical Bank, JPMorgan, BNP and HSBC.` },
  { name: `Caucasus / CIS`, lead: `Dmitri Tchekalkine`, warmPath: `Dmitri Tchekalkine via Caucasus and CIS banking relationships from JPMorgan and HSBC.` },
  { name: `India`, lead: `Saran Sankar`, warmPath: `Saran Sankar via UBS IB alumni network and Indian Business Council Dubai.` },
  { name: `Africa`, lead: `Ibrahim Hemeida`, warmPath: `Ibrahim Hemeida via Dubai-based African diaspora networks and his MENA EM relationships.` },
  { name: `UK / Western`, lead: `Steven Downey`, warmPath: `Steven Downey via London Business School alumni community and CFA Society UAE.` }
];

const COMPLIANCE_BLOCK = [/\bgary\s+dugan\b/i, /\b(al\s+maktoum|bin\s+rashid|mohammed\s+bin\s+rashid)/i, /\b(al\s+nahyan|bin\s+zayed|mohamed\s+bin\s+zayed)/i, /\b(prigozhin|usmanov|deripaska|abramovich|vekselberg|rotenberg|fridman)/i, /\barqaam\s+capital\b/i, /\b(mashreq|emirates\s+nbd|enbd)\b/i, /\bindex\s+&\s+cie\b/i, /\bskybound\s+wealth\b/i];

async function runApifyActor(bucket) {
  const url = `https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?token=${APIFY_TOKEN}`;
  try {
    const res = await fetch(url, { method: `POST`, headers: { 'Content-Type': `application/json` }, body: JSON.stringify(bucket.body) });
    if (!res.ok) { const t = await res.text(); console.warn(`Apify ${bucket.label}: HTTP ${res.status} - ${t.slice(0, 300)}`); return []; }
    const data = await res.json();
    if (!Array.isArray(data)) { console.warn(`Apify ${bucket.label}: not array`); return []; }
    console.log(`Apify ${bucket.label}: returned ${data.length} items`);
    if (data.length > 0) console.log(`  Sample keys: ${Object.keys(data[0]).slice(0, 20).join(`, `)}`);
    return data.map(p => ({ ...p, _qRegion: bucket.region, _qLabel: bucket.label, _qHint: bucket.serviceHint }));
  } catch (e) { console.warn(`Apify ${bucket.label} failed: ${e.message}`); return []; }
}

function normalise(raw) {
  const fullName = (raw.firstName || raw.lastName) ? `${raw.firstName || ''} ${raw.lastName || ''}`.trim() : (raw.fullName || raw.name || `Name unavailable`);
  const company = (Array.isArray(raw.currentPosition) && raw.currentPosition[0]) ? (raw.currentPosition[0].companyName || '') : (raw.currentCompany || raw.company || '');
  const locText = (raw.location && typeof raw.location === `object`) ? (raw.location.linkedinText || `Dubai`) : (raw.location || `Dubai`);
  const slug = raw.publicIdentifier && !/^AC[oOwW]A/.test(raw.publicIdentifier) ? raw.publicIdentifier : null;
  const linkedinUrl = slug ? `https://www.linkedin.com/in/${slug}` : (raw.linkedinUrl || raw.profileUrl || raw.url || '');
  return { firstName: raw.firstName || (fullName.split(` `)[0] || ''), lastName: raw.lastName || (fullName.split(` `).slice(1).join(` `) || ''), name: fullName, title: raw.headline || raw.title || '', company, location: locText, linkedinUrl, email: raw.email || null, emailScore: null, emailVerified: !!raw.email, about: raw.about || raw.summary || '', _qRegion: raw._qRegion, _qLabel: raw._qLabel, _qHint: raw._qHint };
}

function scoreProfile(p) {
  const t = `${p.title} ${p.company} ${p.about}`.toLowerCase();
  let s = 0;
  if (/\bfamily\s+office\b/i.test(t)) s += 6;
  if (/\b(private\s+banker|wealth\s+manager|wealth\s+advisor)/i.test(t)) s += 5;
  if (/\b(managing\s+partner|managing\s+director|founding\s+partner)/i.test(t)) s += 4;
  if (/\b(chief\s+investment\s+officer|CIO|head\s+of\s+investment)/i.test(t)) s += 5;
  if (/\b(fund\s+manager|portfolio\s+manager|hedge\s+fund|private\s+equity|venture\s+capital)/i.test(t)) s += 4;
  if (/\b(external\s+asset\s+manager|EAM|independent\s+wealth)/i.test(t)) s += 5;
  if (/\b(multiple\s+ventures|investment\s+holding|group\s+chairman)/i.test(t)) s += 4;
  if (/\b(commodity\s+trad|grain|metals\s+trad|energy\s+trad)/i.test(t)) s += 4;
  if (/\b(d2c|direct[- ]to[- ]consumer|e-?commerce|amazon\s+seller|shopify)/i.test(t)) s -= 8;
  if (/\b(student|intern|junior)/i.test(t)) s -= 5;
  if (/\b(marketing|growth|content|HR|recruit)/i.test(t) && !/\b(wealth|investment|fund)/i.test(t)) s -= 4;
  if (/\bfounder\b/i.test(t) && !/(family\s+office|investment|fund|wealth|holding)/i.test(t)) s -= 3;
  return s;
}

function diagnose(p) {
  const t = `${p.title} ${p.company} ${p.about}`.toLowerCase();
  const matches = [];
  for (const prob of PROBLEMS) {
    let hits = 0;
    for (const sig of prob.signals) if (sig.test(t)) hits++;
    if (hits > 0) matches.push({ prob, hits });
  }
  matches.sort((a, b) => b.hits - a.hits);
  if (matches.length === 0) return { problem: `Profile lookup - manual review`, fcimService: p._qHint || `Discretionary Portfolio Management`, angle: `Inferred from search bucket. Worth manual scan.`, confidence: `low` };
  const top = matches[0];
  return { problem: top.prob.label, fcimService: top.prob.fcimService, angle: top.prob.angle, confidence: top.hits >= 2 ? `high` : `medium` };
}

async function findEmail(p) {
  if (!HUNTER_API_KEY || !p.firstName || !p.lastName || !p.company) return null;
  const params = new URLSearchParams({ company: p.company, first_name: p.firstName, last_name: p.lastName, api_key: HUNTER_API_KEY });
  try {
    const res = await fetch(`https://api.hunter.io/v2/email-finder?${params.toString()}`);
    if (!res.ok) return null;
    const data = await res.json();
    const d = data && data.data;
    if (d && d.email) return { email: d.email, score: d.score || null, status: (d.verification && d.verification.status) || null };
    return null;
  } catch (e) { return null; }
}

function guessEmails(p) {
  if (!p.firstName || !p.company) return [];
  const c = p.company.toLowerCase().replace(/\b(llc|ltd|limited|gmbh|inc|fzc|fze|dmcc|llp|holdings?|group|capital|partners?|investments?|management)\b/gi, '').replace(/[^a-z0-9]+/g, '').trim();
  if (!c) return [];
  const f = p.firstName.toLowerCase().replace(/[^a-z]/g, '');
  const l = (p.lastName || '').toLowerCase().replace(/[^a-z]/g, '');
  const out = [];
  for (const d of [`${c}.com`, `${c}.ae`, `${c}.io`]) {
    if (f) out.push(`${f}@${d}`);
    if (f && l) out.push(`${f}.${l}@${d}`);
  }
  return out.slice(0, 3);
}

function checkCompliance(p) { const t = `${p.name} ${p.company} ${p.about}`; for (const r of COMPLIANCE_BLOCK) if (r.test(t)) return false; return true; }

function escapeHtml(s) { return String(s == null ? '' : s).replace(/&/g, `&amp;`).replace(/</g, `&lt;`).replace(/>/g, `&gt;`).replace(/"/g, `&quot;`); }

function routeRegion(p) {
  if (p._qRegion) return p._qRegion;
  const t = `${p.name} ${p.title} ${p.company} ${p.about} ${p.location}`.toLowerCase();
  if (/\b(armen|azerb|georgi|tbilisi|yerevan|baku)\b/i.test(t)) return `Caucasus / CIS`;
  if (/\b(russia|moscow|kazakh|belarus|ukrain)\b/i.test(t)) return `Russia / CIS`;
  if (/\b(egypt|cairo)\b/i.test(t)) return `Egypt`;
  if (/\b(lebanon|lebanese|jordan|syria|moroc|tunis)\b/i.test(t)) return `MENA / Levant`;
  if (/\b(india|indian|mumbai|delhi|bangalore)\b/i.test(t)) return `India`;
  if (/\b(nigeri|kenya|south\s+africa|ghana|senegal)\b/i.test(t)) return `Africa`;
  if (/\b(london|england|british|swiss|switzerland)\b/i.test(t)) return `UK / Western`;
  return null;
}

function renderCard(p) {
  const dx = p.diagnosis;
  const svc = SERVICES.find(s => s.name === dx.fcimService);
  const sol = svc ? svc.solution : `Service match indeterminate.`;
  let emailLine;
  if (p.emailVerified && p.email) emailLine = `<strong>Verified:</strong> <a href="mailto:${escapeHtml(p.email)}">${escapeHtml(p.email)}</a>${p.emailScore ? ` (Hunter ${p.emailScore})` : ''}`;
  else if (p.emailGuesses && p.emailGuesses.length) emailLine = `<em>Best guess:</em> ${escapeHtml(p.emailGuesses[0])}`;
  else emailLine = `<em>not found</em>`;
  return `<article class="prospect"><div class="service-tag">${escapeHtml(dx.fcimService)}</div><div class="head-row"><div class="head-main"><h3>${escapeHtml(p.name)}</h3><div class="sub">${escapeHtml(p.title)}${p.company ? ` &middot; ${escapeHtml(p.company)}` : ''}${p.region ? ` &middot; ${escapeHtml(p.region)}` : ''}</div></div>${p.regionLead ? `<div class="lead-block"><span class="lead-label">Lead</span>${escapeHtml(p.regionLead)}</div>` : ''}</div><div class="section" style="background:#F6EFD8;padding:12px 14px;border-left:3px solid #C9A544;border-radius:4px;"><div class="label">Diagnosed problem</div><p><strong>${escapeHtml(dx.problem)}</strong></p><p style="font-size:13px;">${escapeHtml(dx.angle)}</p></div><div class="section"><div class="label">Contact</div><p><strong>LinkedIn:</strong> ${p.linkedinUrl ? `<a href="${escapeHtml(p.linkedinUrl)}" target="_blank">profile &rsaquo;</a>` : '<em>n/a</em>'}<br><strong>Email:</strong> ${emailLine}</p></div><div class="section"><div class="label">FCIM solution</div><p>${escapeHtml(sol)}</p></div><div class="section"><div class="label">Warm path</div><p>${escapeHtml(p.regionWarmPath || 'Route by context.')}</p></div></article>`;
}

async function main() {
  console.log(`FCIM Daily Build v3 - starting`);
  const buckets = pickTodaysQueries();
  console.log(`Today's buckets: ${buckets.map(b => b.label).join(` | `)}`);
  const results = await Promise.all(buckets.map(runApifyActor));
  const raw = results.flat();
  console.log(`Raw profiles: ${raw.length}`);
  let profiles = raw.map(normalise).filter(checkCompliance);
  const seen = new Set();
  profiles = profiles.filter(p => { const fp = `${p.name}|${p.company}`.toLowerCase(); if (seen.has(fp)) return false; seen.add(fp); return true; });
  console.log(`After compliance+dedupe: ${profiles.length}`);
  profiles = profiles.map(p => { p._score = scoreProfile(p); return p; }).filter(p => p._score >= QUALITY_THRESHOLD).sort((a, b) => b._score - a._score);
  console.log(`After quality gate: ${profiles.length}`);
  for (const p of profiles) { p.region = routeRegion(p); const m = REGIONS.find(r => r.name === p.region); p.regionLead = m ? m.lead : null; p.regionWarmPath = m ? m.warmPath : null; p.diagnosis = diagnose(p); }
  let calls = 0;
  for (const p of profiles) {
    if (calls >= MAX_HUNTER_CALLS_PER_RUN) break;
    if (p.emailVerified || !HUNTER_API_KEY) continue;
    const r = await findEmail(p); calls++;
    if (r && r.email) { p.email = r.email; p.emailScore = r.score; p.emailVerified = r.status === `valid` || (r.score && r.score >= 70); }
  }
  console.log(`Hunter calls: ${calls}`);
  for (const p of profiles) if (!p.emailVerified) p.emailGuesses = guessEmails(p);
  const cards = profiles.map(renderCard).join(`\n`);
  const dateStamp = new Intl.DateTimeFormat(`en-GB`, { timeZone: `Asia/Dubai`, weekday: `long`, day: `numeric`, month: `long`, year: `numeric` }).format(new Date());
  const verified = profiles.filter(p => p.emailVerified).length;
  const summary = `${profiles.length} qualified, ${verified} verified emails. Buckets: ${buckets.map(b => b.label).join(` / `)}.`;
  let template;
  try { template = fs.readFileSync(`index.template.html`, `utf-8`); } catch (e) { template = `<html><body><h1>FCIM Daily - {{DATE}}</h1><p>{{COUNCIL_LINE}}</p>{{CONTENT}}</body></html>`; }
  const html = template.replace(/\{\{DATE\}\}/g, escapeHtml(dateStamp)).replace(/\{\{BUILT_AT\}\}/g, new Date().toISOString()).replace(/\{\{COUNCIL_LINE\}\}/g, escapeHtml(summary)).replace(/\{\{REGION_CHIPS\}\}/g, ``).replace(/\{\{FEATURED\}\}/g, ``).replace(/\{\{CONTENT\}\}/g, cards).replace(/\{\{FEATURED_WRAPPER_STYLE\}\}/g, `display:none`);
  fs.writeFileSync(`index.html`, html);
  console.log(`Built: ${profiles.length} prospects, ${verified} verified emails`);
}

main().catch(e => { console.error(e); process.exit(1); });
