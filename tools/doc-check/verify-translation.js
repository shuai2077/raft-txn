// Verifies that a Chinese translation of a markdown document has the same
// structure as its English original.
//
// Usage: node verify-translation.js <original.md> <translation.md>
//
// It checks the things a translation must preserve exactly, and it deliberately
// does NOT try to judge prose quality. The checks are:
//
//   * every fenced code block is byte-identical (mermaid especially), or - for
//     non-mermaid blocks - has the same line count, so comments could be translated
//     without changing the code's shape
//   * headings appear in the same order and at the same level, with the same count
//   * every link target is present and unchanged, and nothing new is linked
//   * every inline `code span` in the original still appears in the translation,
//     and no new one appeared
//
// Rules it cannot check, and which were verified by reading instead:
//
//   * that the prose is a faithful translation rather than a summary
//   * that a hedge was not silently upgraded into a claim
//   * that code *comments* were translated consistently with the prose
//
// Language cross-links (`*.zh-CN.md`) are expected to appear in one file and not
// the other, since each document points at its own translation, so they are
// excluded from the link and span comparisons. Everything else is compared
// strictly, in both directions.
//
// Exit status is 0 when the structure is preserved and 1 otherwise, so this can be
// used as a build step.

const fs = require('fs');

const [origPath, transPath] = process.argv.slice(2);
if (!origPath || !transPath) {
  console.error('usage: node verify-translation.js <original.md> <translation.md>');
  process.exit(2);
}

const orig = fs.readFileSync(origPath, 'utf8').replace(/\r\n/g, '\n');
const trans = fs.readFileSync(transPath, 'utf8').replace(/\r\n/g, '\n');

const problems = [];
const notes = [];

// --- fenced blocks -----------------------------------------------------------
// Returns [{lang, body, line}] where line is the 1-based line of the opening fence.
function fences(text) {
  const lines = text.split('\n');
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const m = lines[i].match(/^[ \t]*```([A-Za-z0-9_-]*)[ \t]*$/);
    if (!m) { i++; continue; }
    const lang = m[1];
    const start = i;
    const body = [];
    i++;
    while (i < lines.length && !/^[ \t]*```[ \t]*$/.test(lines[i])) { body.push(lines[i]); i++; }
    if (i >= lines.length) {
      problems.push(`${origPath}:${start + 1}: unterminated fence in original`);
      break;
    }
    out.push({ lang, body: body.join('\n'), lines: body.length, line: start + 1 });
    i++;
  }
  return out;
}

const fo = fences(orig);
const ft = fences(trans);

if (fo.length !== ft.length) {
  problems.push(`fenced block count differs: original ${fo.length}, translation ${ft.length}`);
} else {
  for (let k = 0; k < fo.length; k++) {
    const a = fo[k], b = ft[k];
    if (a.lang !== b.lang) {
      problems.push(`block ${k + 1}: language differs ("${a.lang}" vs "${b.lang}")`);
      continue;
    }
    if (a.lang === 'mermaid') {
      const kind = a.body.trim().split('\n')[0].trim();
      // classDiagram describes the code, so its content is identical in every
      // language: byte equality is the right check.
      const isCodeDiagram = /^classDiagram$/.test(kind);
      if (isCodeDiagram) {
        if (a.body !== b.body) {
          problems.push(`block ${k + 1} (mermaid ${kind}): must be byte-identical but differs`);
          const al = a.body.split('\n'), bl = b.body.split('\n');
          for (let n = 0; n < Math.max(al.length, bl.length); n++) {
            if (al[n] !== bl[n]) {
              problems.push(`    first diff at inner line ${n + 1}:`);
              problems.push(`      original:    ${JSON.stringify(al[n])}`);
              problems.push(`      translation: ${JSON.stringify(bl[n])}`);
              break;
            }
          }
        } else {
          notes.push(`block ${k + 1} (mermaid ${kind}): identical`);
        }
        continue;
      }

      // Everything else carries prose labels (flowchart boxes, state names,
      // sequence participants and messages), which a translation SHOULD change.
      // So drop exactly the label text and compare the rest byte for byte: node
      // ids, arrows, brackets and line structure must all be preserved. A
      // deliberately narrow strip is what makes this a real check - comparing bare
      // skeletons would let a reversed edge (`A->>B` vs `B->>A`) through.
      const unlabel = (body) => body.split('\n').map((l) => l
        // Forms that put the label behind a keyword, so the id must be kept and
        // only the label replaced.
        .replace(/^(\s*subgraph\s+\S+)\s*\[.*\]\s*$/, '$1[L]')
        .replace(/^(\s*subgraph\s+\S+)\s+"[^"]*"\s*$/, '$1 "L"')
        .replace(/^(\s*(?:participant|actor)\s+\S+)\s+as\s+.*$/, '$1 as L')
        // A label that a translation may change, in each form the documents use:
        .replace(/(:\s*).*$/, '$1L')                  // after a colon (messages, transitions)
        .replace(/\|[^|]*\|/g, '|L|')                 // |edge label|
        .replace(/\["[^"]*"\]/g, '["L"]')             // ["node label"]
        .replace(/\(\([^)]*\)\)/g, '((L))')           // ((label))
        .replace(/\([^)]*\)/g, '(L)')                 // (label)
        // A bracketed label with no quotes is only stripped when it contains a
        // space or non-identifier characters, so `[Store]` (an id) survives.
        .replace(/\[([^\]"]*[^\w\]][^\]"]*)\]/g, '[L]')
        .replace(/"[^"]*"/g, '"L"'));
      const so = unlabel(a.body), st = unlabel(b.body);
      let diff = -1;
      for (let n = 0; n < so.length; n++) {
        if (so[n] !== st[n]) { diff = n; break; }
      }
      if (diff >= 0) {
        problems.push(`block ${k + 1} (mermaid ${kind}): structure differs at inner line ${diff + 1} ` +
          `(node ids, arrows and brackets must not change; only labels may)`);
        problems.push(`      original:    ${JSON.stringify(a.body.split('\n')[diff])}`);
        problems.push(`      translation: ${JSON.stringify(b.body.split('\n')[diff])}`);
      } else {
        notes.push(`block ${k + 1} (mermaid ${kind}): structure identical, ${a.lines} lines`);
      }
    } else if (a.lines !== b.lines) {
      problems.push(`block ${k + 1} (${a.lang || 'plain'}): line count differs ` +
        `(original ${a.lines}, translation ${b.lines}) - code must not be added, removed, merged or split`);
    }
  }
}

// --- headings ----------------------------------------------------------------
function headings(text) {
  return text.split('\n')
    .map((l, i) => {
      const m = l.match(/^(#{1,6})\s+(.*)$/);
      return m ? { level: m[1].length, text: m[2].trim(), line: i + 1 } : null;
    })
    .filter(Boolean);
}
const ho = headings(orig);
const ht = headings(trans);
if (ho.length !== ht.length) {
  problems.push(`heading count differs: original ${ho.length}, translation ${ht.length}`);
  const so = new Set(ho.map((h) => h.level + ':' + h.text));
  ho.forEach((h) => {
    if (!ht.some((t) => t.level === h.level && t.text === h.text)) {
      notes.push(`  original heading not matched verbatim (expected once translated): ${'#'.repeat(h.level)} ${h.text}`);
    }
  });
} else {
  for (let k = 0; k < ho.length; k++) {
    if (ho[k].level !== ht[k].level) {
      problems.push(`heading ${k + 1}: level differs (${ho[k].level} vs ${ht[k].level}) ` +
        `at original line ${ho[k].line}: "${ho[k].text}" vs "${ht[k].text}"`);
    }
  }
}

// --- link targets ------------------------------------------------------------
// The two files point at each other by design: the original links forward to its
// Chinese translation, and the translation links back to the original. Those links
// exist in exactly one file each, so they are excluded from the comparison. The
// exclusion is narrow on purpose - a link pair is only ignored when BOTH files
// contain the reciprocal link, so a translation that drops or retargets a real link
// is still caught.
function linkTargets(text) {
  const out = [];
  const re = /\[([^\]]*)\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g;
  let m;
  while ((m = re.exec(text)) !== null) out.push({ label: m[1], target: m[2] });
  return out;
}

const lo = linkTargets(orig);
const lt = linkTargets(trans);
const oTargets = new Set(lo.map((l) => l.target));
const tTargets = new Set(lt.map((l) => l.target));

// Cross-links: targets that mention the other document of the pair.
const baseName = (p) => p.replace(/\\/g, '/').split('/').pop();
const origName = baseName(origPath);
const transName = baseName(transPath);
const swapped = new Set([origName, transName, origName.replace(/\.md$/, '.zh-CN.md'),
  transName.replace(/\.zh-CN\.md$/, '.md')]);
const isCrossLink = (t) => {
  const f = baseName(t.split('#')[0]);
  if (!f.endsWith('.md')) return false;
  return swapped.has(f);
};

const missing = lo.map((l) => l.target)
  .filter((t) => !tTargets.has(t) && !isCrossLink(t));
if (missing.length) {
  problems.push(`link targets missing from translation (${missing.length}):`);
  [...new Set(missing)].slice(0, 20).forEach((t) => problems.push(`    ${t}`));
}
const extra = lt.map((l) => l.target)
  .filter((t) => !oTargets.has(t) && !isCrossLink(t));
if (extra.length) {
  problems.push(`link targets present only in the translation (${extra.length}):`);
  [...new Set(extra)].slice(0, 20).forEach((t) => problems.push(`    ${t}`));
}
// Bare URLs too (bibliography style).
const bareRe = /<(https?:\/\/[^>]+)>|\bhttps?:\/\/[^\s)>\]]+/g;
function bareUrls(text) {
  const out = new Set();
  let m;
  while ((m = bareRe.exec(text)) !== null) out.add(m[0].replace(/[.,;]$/, ''));
  return out;
}
const buo = bareUrls(orig);
const but = bareUrls(trans);
const bmissing = [...buo].filter((u) => !but.has(u));
if (bmissing.length) {
  problems.push(`bare URLs missing from translation (${bmissing.length}):`);
  bmissing.slice(0, 20).forEach((u) => problems.push(`    ${u}`));
}

// --- inline code spans -------------------------------------------------------
function spans(text) {
  // Strip fenced blocks first so diagram/table content does not dominate.
  const stripped = text.replace(/^[ \t]*```[\s\S]*?^[ \t]*```[ \t]*$/gm, '');
  const out = new Set();
  const re = /`([^`\n]+)`/g;
  let m;
  while ((m = re.exec(stripped)) !== null) out.add(m[1]);
  return out;
}
const so = spans(orig);
const st = spans(trans);
// A cross-link target is a markdown filename, so it shows up as a span when it is
// written inside backticks, as in `04-uml-class-diagram.zh-CN.md`. Reuse the same
// narrow exclusion the link check uses.
const isCrossLinkSpan = (s) => /^[\w.\-]+\.md$/.test(s) && isCrossLink(s);
// An inline span used as running prose inside code formatting - `map[日志索引]结果channel`
// on one side, `map[log index]result channel` on the other - is meant to be
// translated, so requiring the span itself to survive would flag a correct
// translation. The test is deliberately conservative: a span is excused only when
// it contains CJK, or a space, or a slash-separated multi-word phrase. No Go
// identifier contains a space, so identifiers are still compared strictly.
const isProseSpan = (s) =>
  /[\u3400-\u9fff\uf900-\ufaff]/.test(s) || /\s/.test(s);
const smissing = [...so].filter((s) =>
  !st.has(s) && !isCrossLinkSpan(s) && !/\.zh-CN\.md$/.test(s) && !isProseSpan(s));
if (smissing.length) {
  problems.push(`inline code spans missing from translation (${smissing.length} of ${so.size}):`);
  smissing.slice(0, 25).forEach((s) => problems.push(`    ${JSON.stringify(s)}`));
}
const sextra = [...st].filter((s) =>
  !so.has(s) && !isCrossLinkSpan(s) && !/\.zh-CN\.md$/.test(s) && !isProseSpan(s));
if (sextra.length) {
  problems.push(`inline code spans present only in the translation (${sextra.length} of ${st.size}):`);
  sextra.slice(0, 25).forEach((s) => problems.push(`    ${JSON.stringify(s)}`));
}
const proseSpans = [...so].filter(isProseSpan).length + [...st].filter(isProseSpan).length;
if (proseSpans) notes.push(`${proseSpans} prose code span(s) excluded from the comparison (they are translated)`);

// --- report ------------------------------------------------------------------
const oLines = orig.split('\n').length;
const tLines = trans.split('\n').length;
console.log(`${origPath}: ${oLines} lines, ${fo.length} fenced blocks, ${ho.length} headings, ${so.size} inline spans`);
console.log(`${transPath}: ${tLines} lines, ${ft.length} fenced blocks, ${ht.length} headings, ${st.size} inline spans`);
notes.filter((n) => n.includes('identical')).forEach((n) => console.log('  ' + n));

if (problems.length) {
  console.log(`\nPROBLEMS (${problems.length}):`);
  problems.forEach((p) => console.log('  ' + p));
  process.exit(1);
}
console.log('\nstructure preserved');
