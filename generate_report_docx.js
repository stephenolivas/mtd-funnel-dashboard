const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, BorderStyle, WidthType, ShadingType, VerticalAlign,
  HeadingLevel,
} = require("docx");

const data = JSON.parse(fs.readFileSync("report_data.json", "utf8"));

// ── Colors ────────────────────────────────────────────────────────────────────
const NAVY    = "1F2D4A";
const WHITE   = "FFFFFF";
const LIGHT   = "F2F4F8";
const MID     = "DDE1EA";
const ORANGE  = "E47420";
const GREEN   = "0E9F6E";
const RED     = "E02424";
const MUTED   = "8792A2";

// ── Helpers ───────────────────────────────────────────────────────────────────
const NO_BORDER = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
const NO_BORDERS = { top: NO_BORDER, bottom: NO_BORDER, left: NO_BORDER, right: NO_BORDER };
const THIN_BORDER = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const THIN_BORDERS = { top: THIN_BORDER, bottom: THIN_BORDER, left: THIN_BORDER, right: THIN_BORDER };

function txt(text, opts = {}) {
  return new TextRun({
    text: String(text ?? "—"),
    font: "Arial",
    size: opts.size || 18,
    bold: opts.bold || false,
    color: opts.color || "000000",
    italics: opts.italic || false,
  });
}

function cell(children, opts = {}) {
  const fill  = opts.fill  || "FFFFFF";
  const align = opts.align || AlignmentType.LEFT;
  const vAlign = opts.vAlign || VerticalAlign.CENTER;
  const borders = opts.noBorder ? NO_BORDERS : THIN_BORDERS;
  const paragraphs = Array.isArray(children) ? children : [
    new Paragraph({ alignment: align, children: Array.isArray(children) ? children : [children] })
  ];
  return new TableCell({
    width: opts.width ? { size: opts.width, type: WidthType.DXA } : undefined,
    verticalAlign: vAlign,
    shading: { fill, type: ShadingType.CLEAR },
    borders,
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
    children: paragraphs,
  });
}

function para(runs, opts = {}) {
  const r = Array.isArray(runs) ? runs : [runs];
  return new Paragraph({
    alignment: opts.align || AlignmentType.LEFT,
    spacing: opts.spacing || { before: 0, after: 0 },
    children: r,
  });
}

// ── Page layout ────────────────────────────────────────────────────────────────
// Landscape for the wide table
const PAGE_W  = 15840;  // 11 inches
const PAGE_H  = 12240;  // 8.5 inches
const MARGIN  = 720;    // 0.5 inch
const CONTENT = PAGE_W - 2 * MARGIN;  // 14400 DXA

// ── Section 1: Header branding ────────────────────────────────────────────────
const headerTable = new Table({
  width: { size: CONTENT, type: WidthType.DXA },
  columnWidths: [CONTENT / 2, CONTENT / 2],
  rows: [
    new TableRow({
      children: [
        cell(para(txt("BREAK THROUGH CLOSING  ·  STEEL TRAP", { bold: true, size: 16, color: NAVY })), { noBorder: true }),
        cell(para(txt("CONFIDENTIAL  ·  WEEKLY REPORT", { size: 14, color: MUTED }), { align: AlignmentType.RIGHT }),
             { noBorder: true }),
      ],
    }),
  ],
});

// ── Section 2: Title box ──────────────────────────────────────────────────────
const titleBox = new Table({
  width: { size: CONTENT, type: WidthType.DXA },
  columnWidths: [CONTENT],
  rows: [
    new TableRow({
      children: [
        cell([
          para(txt(`VendingPreneurs  —  Report for Week ending ${data.week_ending}`,
               { bold: true, size: 28, color: WHITE }), { align: AlignmentType.CENTER }),
          para(txt(`${data.date_range_label}   ·   Offer Owner: Jeff Goldstein / Anthony Kolodziej   ·   Issued by: Sales Team`,
               { size: 16, color: MID }), { align: AlignmentType.CENTER }),
        ], { fill: NAVY, noBorder: true }),
      ],
    }),
  ],
});

// ── Section 3: KPI tiles ──────────────────────────────────────────────────────
const g = data.grand;
const ext = data.group_totals.EXTERNAL || {};
const inh = data.group_totals["IN-HOUSE"] || {};

function kpiTile(label, value, sub, extVal, extSub, inhVal, inhSub, accentColor) {
  const tileW = Math.floor(CONTENT / 5);
  return cell([
    para(txt(label, { size: 14, color: "AAAAAA", bold: true })),
    para(txt(value, { size: 36, bold: true, color: accentColor })),
    para(txt(sub,   { size: 15, color: "CCCCCC" })),
    para([txt("External: ", { size: 13, bold: true, color: "BBBBBB" }), txt(extVal, { size: 13, color: "BBBBBB" })]),
    para(txt(extSub, { size: 12, color: "999999" })),
    para([txt("In-House: ", { size: 13, bold: true, color: "BBBBBB" }), txt(inhVal, { size: 13, color: "BBBBBB" })]),
    para(txt(inhSub, { size: 12, color: "999999" })),
  ], { fill: NAVY, width: tileW, noBorder: true });
}

function showRate(sh, bo) { return bo ? `${(sh/bo*100).toFixed(1)}% show rate` : "0% show rate"; }
function closedRate(cl, bo, qu) {
  if (!bo) return "0% b→c";
  return `${(cl/bo*100).toFixed(1)}% b→c  ·  ${qu ? (cl/qu*100).toFixed(1) : 0}% q→c`;
}
function avgDeal(rev, cl) { return cl ? `$${Math.round(rev/cl).toLocaleString()} avg deal` : "—"; }
function fmtRev(v) { return `$${Math.round(v).toLocaleString()}`; }

const kpiRow = new Table({
  width: { size: CONTENT, type: WidthType.DXA },
  columnWidths: Array(5).fill(Math.floor(CONTENT / 5)),
  rows: [
    new TableRow({ children: [
      kpiTile("TOTAL BOOKED",  g.booked,  "new first calls",
              `${ext.booked || 0} (${g.booked ? ((ext.booked||0)/g.booked*100).toFixed(1) : 0}%)`,
              "of total",
              `${inh.booked || 0} (${g.booked ? ((inh.booked||0)/g.booked*100).toFixed(1) : 0}%)`,
              "of total", WHITE),
      kpiTile("SHOWED",        g.showed,  showRate(g.showed, g.booked),
              `${ext.showed || 0}`, `${ext.booked ? ((ext.showed||0)/ext.booked*100).toFixed(1) : 0}% show`,
              `${inh.showed || 0}`, `${inh.booked ? ((inh.showed||0)/inh.booked*100).toFixed(1) : 0}% show`,
              "4472C4"),
      kpiTile("QUALIFIED",     g.qualified, `${g.booked ? (g.qualified/g.booked*100).toFixed(1) : 0}% qual rate`,
              `${ext.qualified || 0}`, `${ext.booked ? ((ext.qualified||0)/ext.booked*100).toFixed(1) : 0}% qual`,
              `${inh.qualified || 0}`, `${inh.booked ? ((inh.qualified||0)/inh.booked*100).toFixed(1) : 0}% qual`,
              "9C27B0"),
      kpiTile("CLOSED WON",    g.closed,  closedRate(g.closed, g.booked, g.qualified),
              `${ext.closed || 0}`, `${ext.booked ? ((ext.closed||0)/ext.booked*100).toFixed(1) : 0}% b→c`,
              `${inh.closed || 0}`, `${inh.booked ? ((inh.closed||0)/inh.booked*100).toFixed(1) : 0}% b→c`,
              ORANGE),
      kpiTile("CLOSED REVENUE", fmtRev(g.revenue), avgDeal(g.revenue, g.closed),
              fmtRev(ext.revenue || 0), `$${ext.closed ? Math.round((ext.revenue||0)/ext.closed).toLocaleString() : 0} avg`,
              fmtRev(inh.revenue || 0), `$${inh.closed ? Math.round((inh.revenue||0)/inh.closed).toLocaleString() : 0} avg`,
              GREEN),
    ]}),
  ],
});

// ── Section 4: Funnel table ───────────────────────────────────────────────────
// Columns: FUNNEL | BOOKED | ON PACE | GOAL% | SHOWED | SHOW% | QUAL | QUAL% | CLOSED | CW% | REVENUE | REV/CLOSE
const COL_W = [2200, 700, 800, 900, 700, 700, 700, 700, 700, 700, 900, 900];
const COL_SUM = COL_W.reduce((a, b) => a + b, 0);
const COLS = COL_W.length;

const HEADERS = ["FUNNEL", "BOOKED", "ON PACE", "GOAL %", "SHOWED", "SHOW %",
                 "QUAL", "QUAL %", "CLOSED", "CW %", "REVENUE", "REV/CLOSE"];

function headerRow() {
  return new TableRow({
    children: HEADERS.map((h, i) => cell(
      para(txt(h, { bold: true, size: 14, color: WHITE }),
           { align: i === 0 ? AlignmentType.LEFT : AlignmentType.CENTER }),
      { fill: NAVY, width: COL_W[i] }
    )),
  });
}

function sectionHeaderRow(label) {
  return new TableRow({
    children: [
      new TableCell({
        columnSpan: COLS,
        width: { size: COL_SUM, type: WidthType.DXA },
        shading: { fill: "2E3F5C", type: ShadingType.CLEAR },
        borders: THIN_BORDERS,
        margins: { top: 60, bottom: 60, left: 100, right: 100 },
        children: [para(txt(label, { bold: true, size: 15, color: WHITE }))],
      }),
    ],
  });
}

function colorPct(str) {
  if (!str || str === "—") return MUTED;
  const val = parseFloat(str);
  if (isNaN(val)) return "000000";
  // Show %: good ≥ 65, bad < 50
  return val >= 65 ? GREEN : val < 50 ? RED : ORANGE;
}
function colorCW(str) {
  if (!str || str === "—") return MUTED;
  const val = parseFloat(str);
  if (isNaN(val)) return "000000";
  return val >= 12 ? GREEN : val < 7 ? RED : ORANGE;
}

function dataRow(row, isExcluded) {
  const fill = isExcluded ? "F8F8F8" : "FFFFFF";
  const muteColor = isExcluded ? MUTED : "000000";
  const onPaceStr = row.on_pace != null ? String(row.on_pace) : "—";
  const closedStr = row.closed != null ? String(row.closed) : "—";
  const revStr    = row.revenue ? `$${Math.round(row.revenue).toLocaleString()}` : "$0";

  const c = (val, color) => txt(String(val ?? "—"), { size: 16, color: isExcluded ? MUTED : color });

  return new TableRow({
    children: [
      cell(para([txt(row.funnel + (isExcluded ? " *" : ""), { size: 16, color: isExcluded ? MUTED : "000000" })]),
           { fill, width: COL_W[0] }),
      cell(para(c(row.booked, "000000"),      { align: AlignmentType.CENTER }), { fill, width: COL_W[1] }),
      cell(para(c(onPaceStr, ORANGE),          { align: AlignmentType.CENTER }), { fill, width: COL_W[2] }),
      cell(para(c(row.goal_pct, MUTED),        { align: AlignmentType.CENTER }), { fill, width: COL_W[3] }),
      cell(para(c(row.showed, "000000"),       { align: AlignmentType.CENTER }), { fill, width: COL_W[4] }),
      cell(para(txt(row.show_pct, { size: 16, bold: true, color: isExcluded ? MUTED : colorPct(row.show_pct) }),
               { align: AlignmentType.CENTER }), { fill, width: COL_W[5] }),
      cell(para(c(row.qualified, "000000"),    { align: AlignmentType.CENTER }), { fill, width: COL_W[6] }),
      cell(para(txt(row.qual_pct, { size: 16, bold: true, color: isExcluded ? MUTED : colorPct(row.qual_pct) }),
               { align: AlignmentType.CENTER }), { fill, width: COL_W[7] }),
      cell(para(c(closedStr, "000000"),        { align: AlignmentType.CENTER }), { fill, width: COL_W[8] }),
      cell(para(txt(row.cw_pct, { size: 16, bold: true, color: isExcluded ? MUTED : colorCW(row.cw_pct) }),
               { align: AlignmentType.CENTER }), { fill, width: COL_W[9] }),
      cell(para(txt(revStr, { size: 16, bold: true, color: isExcluded ? MUTED : GREEN }),
               { align: AlignmentType.RIGHT }), { fill, width: COL_W[10] }),
      cell(para(c(row.rev_per_close, MUTED),   { align: AlignmentType.RIGHT }), { fill, width: COL_W[11] }),
    ],
  });
}

function totalRow() {
  const g = data.grand;
  const booked = g.booked;
  const fill = "E8EBF0";
  const t = (v, bold, color) => txt(String(v ?? "—"), { size: 16, bold: bold || false, color: color || "000000" });
  return new TableRow({
    children: [
      cell(para(t("TOTAL", true, NAVY)), { fill, width: COL_W[0] }),
      cell(para(t(booked, true),          { align: AlignmentType.CENTER }), { fill, width: COL_W[1] }),
      cell(para(t("—", false, MUTED),     { align: AlignmentType.CENTER }), { fill, width: COL_W[2] }),
      cell(para(t("—", false, MUTED),     { align: AlignmentType.CENTER }), { fill, width: COL_W[3] }),
      cell(para(t(g.showed, true),         { align: AlignmentType.CENTER }), { fill, width: COL_W[4] }),
      cell(para(txt(`${booked ? (g.showed/booked*100).toFixed(1) : 0}%`,
               { size: 16, bold: true, color: colorPct(`${booked ? g.showed/booked*100 : 0}`) }),
               { align: AlignmentType.CENTER }), { fill, width: COL_W[5] }),
      cell(para(t(g.qualified, true),      { align: AlignmentType.CENTER }), { fill, width: COL_W[6] }),
      cell(para(txt(`${booked ? (g.qualified/booked*100).toFixed(1) : 0}%`,
               { size: 16, bold: true, color: colorPct(`${booked ? g.qualified/booked*100 : 0}`) }),
               { align: AlignmentType.CENTER }), { fill, width: COL_W[7] }),
      cell(para(t(g.closed, true),         { align: AlignmentType.CENTER }), { fill, width: COL_W[8] }),
      cell(para(txt(`${booked ? (g.closed/booked*100).toFixed(1) : 0}%`,
               { size: 16, bold: true, color: colorCW(`${booked ? g.closed/booked*100 : 0}`) }),
               { align: AlignmentType.CENTER }), { fill, width: COL_W[9] }),
      cell(para(txt(`$${Math.round(g.revenue).toLocaleString()}`, { size: 16, bold: true, color: GREEN }),
               { align: AlignmentType.RIGHT }), { fill, width: COL_W[10] }),
      cell(para(t(g.closed ? `$${Math.round(g.revenue/g.closed).toLocaleString()}` : "—", false, MUTED),
               { align: AlignmentType.RIGHT }), { fill, width: COL_W[11] }),
    ],
  });
}

// Build funnel table rows
const funnelTableRows = [headerRow()];
let lastGroup = null;
const groupLabels = { EXTERNAL: "EXTERNAL LEAD SOURCES", "IN-HOUSE": "IN-HOUSE LEAD SOURCES",
                      UNCATEGORIZED: "UNCATEGORIZED", OTHER: "OTHER" };

for (const row of data.funnel_rows) {
  const grp = row.group || "OTHER";
  if (grp !== lastGroup) {
    funnelTableRows.push(sectionHeaderRow(groupLabels[grp] || grp));
    lastGroup = grp;
  }
  funnelTableRows.push(dataRow(row, row.excluded));
}
funnelTableRows.push(totalRow());

const funnelTable = new Table({
  width: { size: COL_SUM, type: WidthType.DXA },
  columnWidths: COL_W,
  rows: funnelTableRows,
});

// ── Section 5: Synopsis placeholder ──────────────────────────────────────────
const synopsisHeader = para(
  txt("LEADFLOW & PIPELINE SYNOPSIS", { bold: true, size: 20, color: NAVY }),
  { spacing: { before: 240, after: 120 } }
);
const synopsisLine = new Paragraph({
  border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: NAVY, space: 1 } },
  spacing: { before: 0, after: 240 },
  children: [],
});
const synopsisBody = para(
  txt("Add weekly synopsis notes here.", { size: 18, color: MUTED, italic: true })
);

// ── Assemble document ─────────────────────────────────────────────────────────
const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 18 } } },
  },
  sections: [{
    properties: {
      page: {
        size: { width: PAGE_W, height: PAGE_H },
        margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN },
      },
    },
    children: [
      headerTable,
      new Paragraph({ spacing: { before: 120, after: 120 }, children: [] }),
      titleBox,
      new Paragraph({ spacing: { before: 180, after: 60 }, children: [
        txt("WEEK ACTIVITY — RAW NUMBERS", { bold: true, size: 18, color: NAVY }),
      ]}),
      kpiRow,
      new Paragraph({ spacing: { before: 180, after: 60 }, children: [
        txt("FUNNEL BREAKDOWN  —  BOOKED → SHOWED → QUALIFIED → CLOSED WON → REVENUE",
            { size: 15, color: MUTED }),
      ]}),
      funnelTable,
      new Paragraph({ spacing: { before: 360, after: 0 }, children: [] }),
      synopsisHeader,
      synopsisLine,
      synopsisBody,
    ],
  }],
});

// ── Write output ──────────────────────────────────────────────────────────────
const outDir = "reports";
if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);
const fname = `${outDir}/report_${data.start_date}_${data.end_date}.docx`;

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(fname, buf);
  console.log(`Written: ${fname}`);
});
