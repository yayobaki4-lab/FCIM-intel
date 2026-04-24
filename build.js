/* FCIM Daily Intelligence — daily builder
 * Runs in GitHub Actions on a schedule.
 * Fetches Google News RSS, classifies signals, writes index.html.
 */
const fs = require('node:fs');

const FEEDS = [
  { label: 'Family office',    query: '"Dubai" "family office"' },
  { label: 'Exits & M&A',      query: '"Dubai" (acquisition OR "sold to" OR exit OR IPO OR raised)' },
  { label: 'DIFC wealth',      query: '"DIFC" ("private bank" OR wealth OR "relationship manager" OR appointed)' },
  { label: 'DMCC commodities', query: '"DMCC" (commodity OR trader OR grain OR metals)' },
  { label: 'UAE HNWI',         query: '"UAE" (HNWI OR "high net worth" OR "family office" OR "ultra high")' },
  { label: 'MENA founders',    query: 'Dubai (Egyptian OR Lebanese OR Indian OR Russian OR "South African") (founder OR investor OR "family office")' }
];

const SERVICES = [
  { name: 'Portfolio Management',
    desc: 'Five CMA-approved models. USD 1M minimum. Risk-matched mandates.',
    solution: 'CMA portfolio management on a risk-matched model. Five CMA-approved portfolios spanning capital preservation through growth. USD 1M minimum is easily cleared.',
    match: /\b(portfolio manag|discretionary|asset allocation|balanced fund|wealth manager|wealth management|investment advisor|risk[- ]matched|mandate)\b/i },
  { name: 'CMA Private Funds',
    desc: 'Five-day approval. UBO privacy. FCIM as manager and administrator.',
    solution: 'CMA private fund with FCIM as manager and administrator. Five-day approval, UBO privacy, five-figure all-in annual cost at typical AUM levels.',
    match: /\b(cma fund|private fund|fund structure|feeder fund|gp\b|general partner|fund launch)\b/i },
  { name: 'Foundation + Private Fund',
    desc: 'UAE Foundation over CMA fund. Three-level control. EUR 100M+ restructure executed.',
    solution: 'UAE Foundation owning a CMA private fund. Three-level control, UBO privacy, onshore UAE governance. Directly analogous to the EUR 100M+ CIS real-estate restructure FCIM executed last year.',
    match: /\b(family office|single family|multi-family|generational|dynastic|succession|post-exit|wealth transfer|next generation|sold\s+(stake|business|company)|divest)\b/i },
  { name: 'Commodity Derivatives',
    desc: 'SCA-licensed. Direct CME, ICE, LME, EEX, SGX. No clearing account required.',
    solution: 'SCA-licensed commodity derivatives desk, direct CME/ICE/LME/EEX/SGX access. No clearing account required on the counterparty side — FCIM sits in between and carries the exposure.',
    match: /\b(commodity|commodities|dmcc|grain|wheat|corn|soyabean|metal|copper|aluminium|aluminum|oil trader|hedging|derivativ|futures|lme|cme)\b/i },
  { name: 'Fund Administration',
    desc: 'One of only five UAE-authorised fund administrators.',
    solution: 'One of only five UAE-authorised fund administrators. End-to-end operational chassis for external managers, advisors, and private bankers setting up independent vehicles.',
    match: /\b(fund administ|fund services|fund platform|NAV calc|fund accounting|administrator\b)/i },
  { name: 'Distressed & Special Situations',
    desc: 'Tim Almashat leads. Regional distressed and co-investment.',
    solution: 'Tim Almashat-led distressed and special-situations desk. Regional expertise and co-investment structures where dislocation meets insight.',
    match: /\b(distressed|restructur|administration|bankruptcy|insolven|special situations|turnaround|liquidation|workout)\b/i },
  { name: 'IB & Advisory',
    desc: 'Tim Almashat-led. ECM, DCM, M&A in the USD 50-150M band.',
    solution: 'Tim Almashat-led IB desk. ECM, DCM, M&A in the USD 50-150M band with deep GCC family office sell-side coverage. Co-origination for regional transactions.',
    match: /\b(m\s*&\s*a|mergers|acquisition|acquired|takeover|roadshow|bookbuild|underwrit|ecm\b|dcm\b|ipo|equity capital markets|debt capital markets)\b/i }
];

const REGIONS = [
  { name: 'Egypt',            lead: 'Ibrahim Hemeida',
    warmPath: 'Ibrahim Hemeida via the Egyptian Business Council Dubai and the Egyptian professional community in DIFC/Business Bay.',
    match: /\b(egyptian|egypt|cairo|alexandria|AUC)\b/i },
  { name: 'Lebanon / Levant', lead: 'Amr Fergany',
    warmPath: 'Amr Fergany via the Credit Suisse DIFC alumni network and Lebanese professional community in DIFC.',
    match: /\b(lebanese|lebanon|beirut|byblos|levantine|jordanian|jordan|amman)\b/i },
  { name: 'Russia / CIS',     lead: 'Dmitri Ganjour',
    warmPath: 'Dmitri Ganjour via Fertistream DMCC alumni and Russian business networks in DMCC/Business Bay.',
    match: /\b(russian|russia|cis\b|kazakh|kazakhstan|uzbek|belarus|ukraine|moscow|tajik|kyrgyz|azerbaijan)\b/i },
  { name: 'India',            lead: 'Saran Sankar',
    warmPath: 'Saran Sankar via UBS Mumbai alumni and the Indian Business & Professional Council Dubai.',
    match: /\b(indian|india|mumbai|delhi|bangalore|bengaluru|chennai|pune|hyderabad|IIT|IIM)\b/i },
  { name: 'Africa',           lead: null,
    warmPath: 'Approach via Dubai-based African diaspora professional networks. Lead assignment TBD by specific country and sector.',
    match: /\b(south african|south africa|nigerian|nigeria|kenyan|kenya|lagos|nairobi|johannesburg|cape town|ghanaian|ghana)\b/i },
  { name: 'UK / Western',     lead: 'Steven Downey',
    warmPath: 'Steven Downey via the UK finance community in DIFC and CFA Society UAE.',
    match: /\b(british|UK\b|london|england|scottish|manchester)\b/i }
];

const COMPLIANCE_BLOCK = [
  /\bgary\s+dugan\b/i,
  /\b(al\s+maktoum|bin\s+rashid\s+al\s+maktoum|mohammed\s+bin\s+rashid|hamdan\s+bin\s+mohammed|maktoum\s+bin\s+mohammed|ahmed\s+bin\s+saeed|mansoor\s+bin\s+mohammed)\b/i,
  /\b(al\s+nahyan|bin\s+zayed\s+al\s+nahyan|mohamed\s+bin\s+zayed|khaled\s+bin\s+mohamed|theyab\s+bin\s+mohamed|tahnoun\s+bin\s+zayed|mansour\s+bin\s+zayed)\b/i,
  /\b(ruler\s+of\s+dubai|ruler\s+of\s+abu\s+dhabi|uae\s+president|uae\s+vice\s+president|crown\s+prince\s+of\s+(dubai|abu\s+dhabi))\b/i,
  /\b(ofac\s+sanctioned|sdn\s+list|specially\s+designated|eu\s+sanctions|under\s+sanctions|asset\s+freeze)\b/i,
  /\b(prigozhin|usmanov|deripaska|abramovich|vekselberg|rotenberg|fridman)\b/i,
  /\b(hezbollah\s+financier|terror\s+financ)\b/i
];
const COMPLIANCE_COI = /\barqaam\s+capital\b/i;
const COMPLIANCE_WARN = /\b(politically\s+exposed\s+person|pep\b|state[- ]owned\s+enterprise|sovereign\s+wealth\s+fund|ministry\s+of)\b/i;
const SIGNAL_KEYWORDS = /\b(exit|acquired|acquisition|family office|hnwi|high net worth|post-exit|sold to|ipo|founder|ceo|chairman|stake|divest|restructur|raised|funding|appointed|joined)\b/i;

const MAX_STALENESS_DAYS = 21;

// ---------- Fetch + parse ----------
async function fetchFeed(feed) {
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(feed.query)}&hl=en-AE&gl=AE&ceid=AE:en`;
  try {
    const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; FCIM-bot/1.0)' } });
    if (!res.ok) { console.warn(`Feed ${feed.label}: HTTP ${res.status}`); return []; }
    const xml = await res.text();
    return parseRss(xml, feed.label);
  } catch (e) {
    console.warn(`Feed ${feed.label} failed: ${e.message}`);
    return [];
  }
}

function parseRss(xml, feedLabel) {
  const items = [];
  const itemRe = /<item\b[^>]*>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = itemRe.exec(xml)) !== null) {
    const body = m[1];
    const get = (tag) => {
      const r = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i');
      const mm = body.match(r);
      if (!mm) return '';
      let v = mm[1].trim();
      v = v.replace(/^<!\[CDATA\[/, '').replace(/\]\]>$/, '').trim();
      return decodeEntities(v);
    };
    const title = get('title');
    const description = get('description');
    const link = get('link');
    const pubDate = get('pubDate');
    if (!title || !pubDate) continue;
    const date = new Date(pubDate);
    if (isNaN(date)) continue;
    const cleanDesc = description.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    const tm = title.match(/^(.+?)\s+-\s+([^-]+)$/);
    const cleanTitle = tm ? tm[1].trim() : title;
    const source = tm ? tm[2].trim() : 'Unknown';
    const snippet = cleanDesc.slice(0, 240) + (cleanDesc.length > 240 ? '…' : '');
    items.push({ title: cleanTitle, source, link, date, snippet, feedLabel });
  }
  return items;
}

function decodeEntities(s) {
  return String(s || '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&#x27;/g, "'")
    .replace(/&nbsp;/g, ' ').replace(/&apos;/g, "'");
}

function fingerprint(title) {
  return String(title).toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 60);
}

function classifyItem(title, snippet) {
  const text = `${title} ${snippet}`;
  const services = [];
  for (const s of SERVICES) if (s.match.test(text) && !services.includes(s.name)) services.push(s.name);
  let region = null;
  for (const r of REGIONS) { if (r.match.test(text)) { region = r; break; } }
  return {
    services,
    primaryService: services[0] || null,
    region: region ? region.name : null,
    regionLead: region ? region.lead : null,
    regionWarmPath: region ? region.warmPath : null,
    isSignal: SIGNAL_KEYWORDS.test(text)
  };
}

function runCompliance(title, snippet) {
  const text = `${title} ${snippet}`;
  for (const rule of COMPLIANCE_BLOCK) if (rule.test(text)) return { allowed: false };
  if (COMPLIANCE_COI.test(text)) return { allowed: true, coi: true };
  if (COMPLIANCE_WARN.test(text)) return { allowed: true, pep: true };
  return { allowed: true };
}

function scoreItem(it) {
  let s = 0;
  if (it.primaryService) s += 2;
  if (it.region) s += 2;
  if (it.isSignal) s += 1;
  const days = (Date.now() - it.date.getTime()) / 86400000;
  if (days < 2) s += 2; else if (days < 7) s += 1;
  return s;
}

function pickFeatured(items) {
  const eligible = items.filter(it => !it.coi && it.primaryService);
  if (!eligible.length) return null;
  return [...eligible].sort((a, b) => {
    const sa = scoreItem(a), sb = scoreItem(b);
    if (sb !== sa) return sb - sa;
    return b.date - a.date;
  })[0];
}

function signalStrength(item) {
  let s = 0;
  if (item.primaryService) s += 2;
  if (item.region) s += 2;
  if (item.isSignal) s += 1;
  const days = item.date ? (Date.now() - item.date.getTime()) / 86400000 : 999;
  if (days < 7) s += 1;
  if (s >= 5) return 'High';
  if (s >= 3) return 'Medium';
  if (s >= 1) return 'Low';
  return 'Context';
}

function signalTimeline(item) {
  if (!item.date) return 'Monitor';
  const days = (Date.now() - item.date.getTime()) / 86400000;
  if (days < 7) return 'Act this week';
  if (days < 21) return '30-day window';
  return 'Monitor';
}

function relativeTime(date) {
  const diff = Date.now() - date.getTime();
  if (diff < 0) return 'just now';
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d ago`;
  return new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short' }).format(date);
}

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

function buildDraftPrompt(item) {
  return `Draft Yehya Abdelbaki's FCIM outreach based on this live news signal.

Headline: ${item.title}
Source: ${item.source}
Date: ${item.date.toISOString().slice(0,10)}
Link: ${item.link}
Context: ${item.snippet}

Matched FCIM service: ${item.primaryService || '(pick best fit)'}
Region: ${item.region || '(not detected)'}
Suggested lead: ${item.regionLead || '(route by context)'}

Write a <200 word email in Yehya's voice to the decision-maker at the company in the headline. Reference specifics from the news. Mention the FCIM colleague naturally only where it adds credibility. End with the full Yehya signature block (Yehya Abdelbaki / Relationship Manager / Fundament Capital Investment Management / yehya.abdelbaki@fundamentcapital.ae | +971 4 834 8385).`;
}

function renderCard(item, isFeatured) {
  const service = SERVICES.find(s => s.name === item.primaryService);
  const solutionText = service ? service.solution : 'Service match indeterminate — open the article to assess which FCIM product fits.';
  const warmPath = item.regionWarmPath || 'Region indeterminate. Identify the subject\u2019s origin from the source article, then route to the relevant FCIM colleague.';

  let complianceBlock = '';
  if (item.coi) complianceBlock = `<div class="compliance"><span class="label">Compliance</span>Arqaam Capital referenced. Center-of-influence only — not a prospect.</div>`;
  else if (item.pep) complianceBlock = `<div class="compliance pep"><span class="label">Elevated DD</span>PEP or state-linked exposure detected. Enhanced due diligence required before outreach.</div>`;

  const firstStepText = item.region && item.regionLead
    ? `Identify the decision-maker at the company named in this signal and approach via ${escapeHtml(item.regionLead)}\u2019s network. Tap below to copy a draft prompt pre-filled with the context — paste it in your Claude chat and Yehya\u2019s email comes back.`
    : `Open the source article, identify the decision-maker, then approach via the FCIM colleague best matched to their nationality/sector. Tap below to copy a draft prompt with the context.`;

  const prompt = buildDraftPrompt(item);
  const promptAttr = escapeHtml(prompt);

  return `
    <article class="prospect ${isFeatured ? 'featured' : ''} ${item.coi ? 'coi' : ''}" data-region="${escapeHtml(item.region || '')}">
      <div class="service-tag">${escapeHtml(item.primaryService || 'Market context')}</div>
      <div class="head-row">
        <div class="head-main">
          <h3><a href="${escapeHtml(item.link)}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.title)}</a></h3>
          <div class="sub">
            ${escapeHtml(item.source)}<span class="dot">·</span>${escapeHtml(relativeTime(item.date))}${item.region ? `<span class="dot">·</span>${escapeHtml(item.region)}` : ''}
          </div>
        </div>
        ${item.regionLead ? `<div class="lead-block"><span class="lead-label">Lead</span>${escapeHtml(item.regionLead)}</div>` : ''}
      </div>
      <div class="section">
        <div class="label">Signal</div>
        <p>${escapeHtml(item.snippet || 'No excerpt available — open the source article.')}</p>
      </div>
      <div class="section">
        <div class="label">FCIM Solution</div>
        <p>${escapeHtml(solutionText)}</p>
      </div>
      <div class="meta-row">
        <strong>${escapeHtml(signalStrength(item))}</strong> signal strength <span class="sep">/</span> ${escapeHtml(signalTimeline(item))}
      </div>
      <div class="section">
        <div class="label">Warm Path</div>
        <p>${escapeHtml(warmPath)}</p>
      </div>
      ${complianceBlock}
      <div class="first-step">
        <div class="label">First Step</div>
        <p>${firstStepText}</p>
        <button class="draft-btn" data-prompt="${promptAttr}">Copy draft prompt</button>
      </div>
      <a class="source-link" href="${escapeHtml(item.link)}" target="_blank" rel="noopener noreferrer">Read the source article <span class="arrow">›</span></a>
    </article>
  `;
}

function renderServiceSection(svc, items, selectedRegionCount) {
  const isEmpty = items.length === 0;
  return `
    <section class="service-section" data-service="${escapeHtml(svc.name)}">
      <div class="service-header">
        <div class="left">
          <h2>${escapeHtml(svc.name)}</h2>
          <p>${escapeHtml(svc.desc)}</p>
        </div>
        <div class="right-meta">${items.length} signal${items.length === 1 ? '' : 's'}</div>
      </div>
      ${isEmpty
        ? `<div class="empty-note">No live signals for this service today.</div>`
        : `<div class="items">${items.map(it => renderCard(it, false)).join('')}</div>`
      }
    </section>`;
}

function renderRegionChips(regionCounts) {
  return REGIONS.map(r => {
    const n = regionCounts[r.name] || 0;
    const isEmpty = n === 0;
    return `
      <button class="region-chip ${isEmpty ? 'empty' : ''}" ${isEmpty ? 'disabled' : `data-region="${escapeHtml(r.name)}"`}>
        <span>${escapeHtml(r.name)}</span>
        <span class="count">${n}</span>
        ${r.lead ? `<span class="lead">${escapeHtml(r.lead.split(' ')[0])}</span>` : ''}
      </button>`;
  }).join('');
}

// ---------- Main ----------
async function main() {
  console.log('FCIM Daily Build — starting');
  const results = await Promise.all(FEEDS.map(fetchFeed));
  let items = results.flat();
  console.log(`Raw items: ${items.length}`);

  // Staleness
  const cutoff = Date.now() - MAX_STALENESS_DAYS * 86400000;
  items = items.filter(it => it.date.getTime() >= cutoff);

  // Dedupe
  const seen = new Set();
  items = items.filter(it => {
    const fp = fingerprint(it.title);
    if (!fp || seen.has(fp)) return false;
    seen.add(fp);
    return true;
  });
  console.log(`After dedupe: ${items.length}`);

  // Compliance
  let blocked = 0;
  items = items.filter(it => {
    const c = runCompliance(it.title, it.snippet);
    if (!c.allowed) { blocked++; return false; }
    it.coi = !!c.coi; it.pep = !!c.pep;
    return true;
  });
  console.log(`After compliance: ${items.length} (blocked: ${blocked})`);

  // Classify
  items.forEach(it => Object.assign(it, classifyItem(it.title, it.snippet)));

  // Sort by recency
  items.sort((a, b) => b.date - a.date);

  // Pick featured
  const featured = pickFeatured(items);
  const featuredHtml = featured ? renderCard(featured, true) : '';

  // Group remaining
  const remaining = featured ? items.filter(it => it.link !== featured.link) : items;

  let servicesHtml = '';
  let emptyServicesHtml = '';
  for (const svc of SERVICES) {
    const sectionItems = remaining.filter(it => it.primaryService === svc.name);
    const html = renderServiceSection(svc, sectionItems);
    if (sectionItems.length === 0) emptyServicesHtml += html;
    else servicesHtml += html;
  }

  const unclassified = remaining.filter(it => !it.primaryService);
  if (unclassified.length) {
    servicesHtml += `
      <section class="service-section">
        <div class="service-header">
          <div class="left">
            <h2>Market context</h2>
            <p>General Dubai finance news not matched to a specific FCIM service.</p>
          </div>
          <div class="right-meta">${unclassified.length} item${unclassified.length === 1 ? '' : 's'}</div>
        </div>
        <div class="items">${unclassified.slice(0, 6).map(it => renderCard(it, false)).join('')}</div>
      </section>`;
  }

  const finalContent = servicesHtml + emptyServicesHtml;
  const regionCounts = {};
  REGIONS.forEach(r => regionCounts[r.name] = 0);
  items.forEach(it => { if (it.region && regionCounts[it.region] !== undefined) regionCounts[it.region]++; });
  const regionChipsHtml = renderRegionChips(regionCounts);

  const dateStamp = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Dubai', weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }).format(new Date());
  const builtAt = new Date().toISOString();
  const councilLine = featured
    ? `Council convened — ${items.length} live signals scanned, ${blocked} blocked on compliance, ${Object.values(regionCounts).filter(n => n > 0).length} regions active.`
    : `Council couldn\u2019t find a strong signal today. ${items.length} items returned, ${blocked} blocked on compliance.`;

  const template = fs.readFileSync('index.template.html', 'utf-8');
  const html = template
    .replace(/\{\{DATE\}\}/g, escapeHtml(dateStamp))
    .replace(/\{\{BUILT_AT\}\}/g, escapeHtml(builtAt))
    .replace(/\{\{COUNCIL_LINE\}\}/g, escapeHtml(councilLine))
    .replace(/\{\{REGION_CHIPS\}\}/g, regionChipsHtml)
    .replace(/\{\{FEATURED\}\}/g, featuredHtml)
    .replace(/\{\{CONTENT\}\}/g, finalContent)
    .replace(/\{\{FEATURED_WRAPPER_STYLE\}\}/g, featured ? '' : 'display:none');

  fs.writeFileSync('index.html', html);
  console.log(`Built index.html — ${items.length} signals, featured: ${featured ? featured.title : 'none'}`);
}

main().catch(err => { console.error(err); process.exit(1); });
