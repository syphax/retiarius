/// <reference types="node" />
// Reallocate a demand_forecast's total by population: each ZIP3 maps to its nearest
// demand location (great-circle), ZIP3s farther than --max-miles are dropped, and each
// location's population share of the total is split evenly across its products.
// A ZIP3 with several locations within --share-miles splits its population evenly
// among them instead, so dense metros don't hand a whole ZIP3 to one suburb.
//
// Usage (Node 23.6+, from cognition/):
//   node src/utils/popDemand.ts [--max-miles 500] [--share-miles 25] [--scenario Proportional] [--out pop_forecast.csv]
//
// Reads location and demand_forecast from the (link-shared) Google Sheet, and population
// from scimulator/data/zip3_pop_2020.csv. Writes a demand_forecast_pop table to --out: one
// row per active customer × product (products, period and total from demand_forecast).
import fs from 'node:fs';
import path from 'node:path';
import Papa from 'papaparse';

const SHEET_ID = '1C5PcwkrjkGcRdQ3EaXzR1j20bM9ouq6pUJev9Isj5UA';
const ZIP3_POP = path.join(import.meta.dirname, '..', '..', '..', 'scimulator', 'data', 'zip3_pop_2020.csv');
const EARTH_RADIUS_MI = 3958.8;

type Row = Record<string, string>;

function arg(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

function parseCsv(text: string): Row[] {
  return Papa.parse<Row>(text, { header: true, skipEmptyLines: true }).data;
}

async function sheetTab(tab: string): Promise<Row[]> {
  const url = `https://docs.google.com/spreadsheets/d/${SHEET_ID}/gviz/tq?tqx=out:csv&sheet=${encodeURIComponent(tab)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${tab}: HTTP ${res.status}`);
  return parseCsv(await res.text());
}

function haversineMiles(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const r = Math.PI / 180;
  const a =
    Math.sin(((lat2 - lat1) * r) / 2) ** 2 +
    Math.cos(lat1 * r) * Math.cos(lat2 * r) * Math.sin(((lon2 - lon1) * r) / 2) ** 2;
  return 2 * EARTH_RADIUS_MI * Math.asin(Math.sqrt(a));
}

/** Round to integers while preserving the (integer) total: largest remainder method. */
function roundPreservingTotal(values: number[], total: number): number[] {
  const floors = values.map(Math.floor);
  let short = total - floors.reduce((s, v) => s + v, 0);
  const order = values.map((v, i) => [v - floors[i], i] as const).sort((a, b) => b[0] - a[0]);
  for (const [, i] of order) {
    if (short <= 0) break;
    floors[i]++;
    short--;
  }
  return floors;
}

async function main() {
  const maxMiles = Number(arg('max-miles', '500'));
  const shareMiles = Number(arg('share-miles', '25'));
  const outPath = arg('out', 'pop_forecast.csv');
  const scenario = arg('scenario', 'Proportional');

  const [locations, baseDemand] = await Promise.all([sheetTab('location'), sheetTab('demand_forecast')]);
  const zip3s = parseCsv(fs.readFileSync(ZIP3_POP, 'utf8'));

  const total = Math.round(baseDemand.reduce((s, r) => s + Number(r.forecast || 0), 0));
  const products = [...new Set(baseDemand.map((r) => r.product_id))];
  const { period_id, period_name } = baseDemand[0];

  // Candidate locations: every active customer on the location tab, in location_name order.
  const customers = locations
    .filter((l) => l.type === 'Customer' && l.active.toUpperCase() === 'TRUE')
    .sort((a, b) => (a.location_name < b.location_name ? -1 : a.location_name > b.location_name ? 1 : 0));
  const demandLocIds = customers.map((l) => l.location_id);
  const candidates = customers.map((l) => ({ id: l.location_id, lat: Number(l.latitude), lon: Number(l.longitude) }));
  const targetRows = customers.flatMap((l) => products.map((product_id) => ({ product_id, location_id: l.location_id, location_name: l.location_name })));

  // Nearest-location assignment.
  const popByLoc = new Map(demandLocIds.map((id) => [id, 0]));
  let totalPop = 0;
  let missedPop = 0;
  let shared = 0;
  const missed: { zip3: string; pop: number; miles: number }[] = [];
  for (const z of zip3s) {
    const pop = Number(z.population);
    const lat = Number(z.latitude);
    const lon = Number(z.longitude);
    totalPop += pop;
    let best = candidates[0];
    let bestMi = Infinity;
    const near: string[] = [];
    for (const c of candidates) {
      const mi = haversineMiles(lat, lon, c.lat, c.lon);
      if (mi < bestMi) [best, bestMi] = [c, mi];
      if (mi < shareMiles) near.push(c.id);
    }
    if (near.length > 1) {
      shared++;
      for (const id of near) popByLoc.set(id, popByLoc.get(id)! + pop / near.length);
    } else if (bestMi < maxMiles) popByLoc.set(best.id, popByLoc.get(best.id)! + pop);
    else {
      missedPop += pop;
      missed.push({ zip3: z.zip3, pop, miles: Math.round(bestMi) });
    }
  }

  // Each location's share splits evenly across its products.
  const coveredPop = totalPop - missedPop;
  const raw = targetRows.map((r) => (total * popByLoc.get(r.location_id)!) / coveredPop / products.length);
  const forecast = roundPreservingTotal(raw, total);

  const out = targetRows.map((r, i) => ({
    product_id: r.product_id,
    product_name: '',
    location_id: r.location_id,
    location_name: r.location_name,
    period_id,
    period_name,
    active: 'TRUE',
    forecast: forecast[i],
    forecast_qty_uom: 'EA',
    min_fulfillment: '',
    price: '',
    Scenario: scenario,
  }));
  fs.writeFileSync(outPath, Papa.unparse(out) + '\n');

  const pct = (n: number) => `${((100 * n) / totalPop).toFixed(2)}%`;
  const empty = demandLocIds.filter((id) => popByLoc.get(id) === 0);
  console.log(`max distance:        ${maxMiles} mi`);
  console.log(`total forecast:      ${total.toLocaleString()} (from demand_forecast)`);
  console.log(`ZIP3s:               ${zip3s.length} (${shared} shared within ${shareMiles} mi, ${missed.length} beyond ${maxMiles} mi)`);
  console.log(`population covered:  ${coveredPop.toLocaleString()} of ${totalPop.toLocaleString()} (missed ${pct(missedPop)})`);
  console.log(`locations:           ${demandLocIds.length} (${empty.length} with no ZIP3 assigned${empty.length ? `: ${empty.join(', ')}` : ''})`);
  if (missed.length) {
    console.log('missed ZIP3s (largest first):');
    for (const m of missed.sort((a, b) => b.pop - a.pop).slice(0, 15))
      console.log(`  ${m.zip3}  pop ${m.pop.toLocaleString().padStart(10)}  nearest ${m.miles} mi`);
  }
  console.log(`wrote ${forecast.length} rows → ${outPath}`);
}

main();
