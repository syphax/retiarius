// Build the bundled postal-code centroid tables in public/geo/.
//
// Usage: node scripts/build_geo.mjs <path/to/2020_Gaz_zcta_national.txt>
//
// ZIP5: Census 2020 ZCTA gazetteer (public domain):
//   https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_zcta_national.zip
// ZIP3: scimulator/data/zip3_pop_2020.csv (population-weighted centroids).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.join(here, '..', 'public', 'geo');
fs.mkdirSync(outDir, { recursive: true });

const gazPath = process.argv[2];
if (!gazPath) {
  console.error('usage: node scripts/build_geo.mjs <2020_Gaz_zcta_national.txt>');
  process.exit(1);
}

const round = (x) => Number(x).toFixed(5);

// ZIP5
const gazLines = fs.readFileSync(gazPath, 'utf8').trim().split('\n');
const header = gazLines[0].split('\t').map((s) => s.trim());
const iZip = header.indexOf('GEOID');
const iLat = header.indexOf('INTPTLAT');
const iLon = header.indexOf('INTPTLONG');
const zip5 = ['code,lat,lon'];
for (const line of gazLines.slice(1)) {
  const f = line.split('\t').map((s) => s.trim());
  if (!f[iLat] || !f[iLon]) continue;
  zip5.push(`${f[iZip]},${round(f[iLat])},${round(f[iLon])}`);
}
fs.writeFileSync(path.join(outDir, 'zip5.csv'), zip5.join('\n') + '\n');

// ZIP3
const zip3Src = path.join(here, '..', '..', 'scimulator', 'data', 'zip3_pop_2020.csv');
const zip3Lines = fs.readFileSync(zip3Src, 'utf8').trim().split('\n');
const h3 = zip3Lines[0].split(',').map((s) => s.replace(/"/g, ''));
const zip3 = ['code,lat,lon'];
for (const line of zip3Lines.slice(1)) {
  const f = line.split(',').map((s) => s.replace(/"/g, ''));
  if (!f[h3.indexOf('latitude')] || !f[h3.indexOf('longitude')]) continue;
  zip3.push(`${f[h3.indexOf('zip3')]},${round(f[h3.indexOf('latitude')])},${round(f[h3.indexOf('longitude')])}`);
}
fs.writeFileSync(path.join(outDir, 'zip3.csv'), zip3.join('\n') + '\n');

console.log(`zip5: ${zip5.length - 1} rows, zip3: ${zip3.length - 1} rows → ${outDir}`);
