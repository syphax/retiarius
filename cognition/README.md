# Cognition

A lightweight center-of-gravity app. Everything runs in the browser; there is no backend.

Spec and decisions: `retiarius-private/prompts/cognition-v0.1.md`.

```bash
npm install
npm run dev     # http://localhost:5174
npm test        # vitest
npm run build
```

## Data contract

Hard column names (case-insensitive; spaces/dashes become underscores). Extra columns are ignored.

| column | notes |
|---|---|
| `Type` | `demand`, `source`, or `fixed` (existing node) |
| `Volume` | positive number; ignored for `fixed` |
| `Lat`, `Lon` | preferred when set; otherwise geocoded values are shown in gray italics |
| `Postal Code`, `Postal Code Type` | `zip5` or `zip3`; leading zeros restored, ZIP+4 truncated |
| `Active` | optional; TRUE/FALSE (also yes/no, 1/0, x); blank = active |

Rows that fail validation are flagged in the Data tab and excluded from runs. Inactive rows are
validated but excluded from runs and the map. The Data tab filters by Type and Active; edits and
fill down apply only to the rows shown.

## Geo data

`public/geo/zip5.csv` comes from the Census 2020 ZCTA gazetteer (public domain). `public/geo/zip3.csv` comes from `scimulator/data/zip3_pop_2020.csv`. To rebuild them:

```bash
curl -LO https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_zcta_national.zip
unzip 2020_Gaz_zcta_national.zip
npm run geo:build -- 2020_Gaz_zcta_national.txt
```
