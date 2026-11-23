/**
 * Where the job ran: Cloudflare data centres placed by their airport's coordinates, sized by
 * the tasks they ran, with curves for the bytes sent between them.
 */
import type { LiveJobView } from "../src/coordinator/live.ts";
import { bytes, escape } from "./format.ts";

/** Latitude and longitude of the data centres jobs have used, and their European neighbours. */
const COLOS: Record<string, [number, number]> = {
  AMS: [52.31, 4.76], ARN: [59.65, 17.92], ATH: [37.94, 23.94], BCN: [41.3, 2.08], BEG: [44.82, 20.29],
  BRU: [50.9, 4.48], BUD: [47.43, 19.26], CDG: [49.0, 2.55], CPH: [55.62, 12.65], DUB: [53.42, -6.27],
  DUS: [51.29, 6.77], FCO: [41.8, 12.25], FRA: [50.03, 8.57], HAM: [53.63, 9.99], HEL: [60.32, 24.96],
  KBP: [50.35, 30.89], LHR: [51.47, -0.45], LIS: [38.77, -9.13], MAD: [40.47, -3.56], MAN: [53.35, -2.27],
  MRS: [43.44, 5.21], MUC: [48.35, 11.79], MXP: [45.63, 8.72], OSL: [60.19, 11.1], OTP: [44.57, 26.1],
  PRG: [50.1, 14.26], RIX: [56.92, 23.97], SOF: [42.69, 23.41], TLL: [59.41, 24.83], VIE: [48.11, 16.57],
  VNO: [54.63, 25.29], WAW: [52.17, 20.97], ZAG: [45.74, 16.07], ZRH: [47.46, 8.55], IST: [41.26, 28.74],
  KIV: [46.93, 28.93], LUX: [49.63, 6.21], SKG: [40.52, 22.97], BTS: [48.17, 17.21], LJU: [46.22, 14.46],
};

const W = 560;
const H = 300;

export function renderMap(target: HTMLElement, view: LiveJobView): void {
  const placed = view.colos.filter((colo) => COLOS[colo.colo]);
  const unplaced = view.colos.filter((colo) => !COLOS[colo.colo]).map((colo) => colo.colo);
  if (placed.length === 0) {
    target.innerHTML = `<p class="muted">No locations yet.</p>`;
    return;
  }
  // A box around the places used, at least Germany-to-Poland wide, with room for labels.
  const lats = placed.map((colo) => COLOS[colo.colo]![0]);
  const lons = placed.map((colo) => COLOS[colo.colo]![1]);
  const midLat = (Math.min(...lats) + Math.max(...lats)) / 2;
  const midLon = (Math.min(...lons) + Math.max(...lons)) / 2;
  const scaleLon = Math.cos((midLat * Math.PI) / 180);
  const spanLon = Math.max(Math.max(...lons) - Math.min(...lons), 12) * 1.35;
  const spanLat = Math.max(Math.max(...lats) - Math.min(...lats), 6) * 1.5;
  const unit = Math.min(W / (spanLon * scaleLon), H / spanLat);
  const point = (colo: string): [number, number] => {
    const [lat, lon] = COLOS[colo]!;
    return [W / 2 + (lon - midLon) * scaleLon * unit, H / 2 - (lat - midLat) * unit];
  };

  const grid: string[] = [];
  for (let lon = Math.floor((midLon - spanLon) / 5) * 5; lon <= midLon + spanLon; lon += 5) {
    const x = W / 2 + (lon - midLon) * scaleLon * unit;
    grid.push(`<line x1="${x}" y1="0" x2="${x}" y2="${H}" class="grid"/>`);
  }
  for (let lat = Math.floor((midLat - spanLat) / 5) * 5; lat <= midLat + spanLat; lat += 5) {
    const y = H / 2 - (lat - midLat) * unit;
    grid.push(`<line x1="0" y1="${y}" x2="${W}" y2="${y}" class="grid"/>`);
  }

  const most = Math.max(1, ...view.flows.map((flow) => flow.bytes));
  const flows = view.flows
    .filter((flow) => flow.from !== flow.to && COLOS[flow.from] && COLOS[flow.to])
    .map((flow) => {
      const [x1, y1] = point(flow.from);
      const [x2, y2] = point(flow.to);
      // Bend each curve to one side so a pair's two directions do not overlap.
      const mx = (x1 + x2) / 2 - (y2 - y1) * 0.18;
      const my = (y1 + y2) / 2 + (x2 - x1) * 0.18;
      const width = 0.8 + 5 * Math.sqrt(flow.bytes / most);
      return `<path d="M${x1},${y1} Q${mx},${my} ${x2},${y2}" class="flow" stroke-width="${width.toFixed(2)}"><title>${escape(`${flow.from} → ${flow.to}: ${bytes(flow.bytes)}`)}</title></path>`;
    });
  const local = view.flows.filter((flow) => flow.from === flow.to).reduce((sum, flow) => sum + flow.bytes, 0);

  const nodes = placed.map((colo) => {
    const [x, y] = point(colo.colo);
    const tasks = colo.scans + colo.reducers;
    const r = 4 + 2.2 * Math.sqrt(tasks);
    const kind = colo.reducers && colo.scans ? "both" : colo.reducers ? "reduce" : colo.scans ? "scan" : "coord";
    const parts = [colo.scans ? `${colo.scans} scans` : "", colo.reducers ? `${colo.reducers} reducers` : "", colo.coordinator ? "coordinator" : ""].filter(Boolean).join(", ");
    return `<g><circle cx="${x}" cy="${y}" r="${r.toFixed(1)}" class="site ${kind}"/>${
      colo.coordinator ? `<circle cx="${x}" cy="${y}" r="${(r + 4).toFixed(1)}" class="coordinator"/>` : ""
    }<text x="${x + r + 3}" y="${y + 3}" class="site-label">${escape(colo.colo)}</text><title>${escape(`${colo.colo}: ${parts}`)}</title></g>`;
  });

  target.innerHTML = `<svg viewBox="0 0 ${W} ${H}" class="map" role="img" aria-label="Data centres used">${grid.join("")}${flows.join("")}${nodes.join("")}</svg>
<p class="caption">${local ? `${bytes(local)} stayed within a data centre. ` : ""}${unplaced.length ? `Not on the map: ${escape(unplaced.join(", "))}.` : ""}</p>`;
}
