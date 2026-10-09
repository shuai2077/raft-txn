// Verifies that Go code fragments quoted in a markdown document appear verbatim in
// the repository's sources.
//
// Usage: node verify-quoted-code.js <document.md> [rootDir]
//
// Quoting code is a good way to be precise and a bad way to be accidentally wrong:
// a hand-retyped snippet can differ from the source in a way no reader will notice,
// and a "simplified for clarity" quotation is indistinguishable from a mistake
// unless it is labelled. This checks every ```go block:
//
//   * fragments marked as simplified (the document says so) are checked line by
//     line but may skip lines, so they must still match wherever they do match
//   * every other ```go block must appear verbatim, as a contiguous run of lines,
//     in some file under the root
//
// A block that cannot be found is reported with its first unmatched line, so the
// fix is obvious.

const fs = require('fs');
const path = require('path');

const doc = process.argv[2];
const root = process.argv[3] || '.';
if (!doc) {
  console.error('usage: node verify-quoted-code.js <document.md> [rootDir]');
  process.exit(2);
}

// --- collect every Go line in the repository --------------------------------
const files = [];
(function walk(d) {
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    if (e.name === '.git' || e.name === 'node_modules') continue;
    const p = path.join(d, e.name);
    if (e.isDirectory()) walk(p);
    else if (e.name.endsWith('.go')) files.push(p);
  }
})(root);

const corpus = files.map((f) => ({ f, lines: fs.readFileSync(f, 'utf8').replace(/\r\n/g, '\n').split('\n') }));

// --- extract ```go blocks ----------------------------------------------------
const text = fs.readFileSync(doc, 'utf8').replace(/\r\n/g, '\n');
const lines = text.split('\n');
const blocks = [];
for (let i = 0; i < lines.length; i++) {
  const m = lines[i].match(/^[ \t]*```go[ \t]*$/);
  if (!m) continue;
  const start = i;
  const body = [];
  i++;
  while (i < lines.length && !/^[ \t]*```[ \t]*$/.test(lines[i])) { body.push(lines[i]); i++; }
  blocks.push({ line: start + 1, body });
}

// --- check -------------------------------------------------------------------
let verbatim = 0;
let simplified = 0;
const notFound = [];

for (const b of blocks) {
  // Work on a copy: b.body is used again for error reporting, and trimming must
  // not destroy it.
  const body = b.body.slice();
  while (body.length && body[0].trim() === '') body.shift();
  while (body.length && body[body.length - 1].trim() === '') body.pop();
  if (!body.length) continue;

  // Normalise both sides the same way. A struct listing in a document legitimately
  // drops comments, so blank lines are dropped and each line is compared with
  // internal whitespace collapsed. Identifiers, types and punctuation must still
  // match exactly - which is the part that catches a mistake.
  const normalise = (ls) => ls
    .map((l) => l.trim())
    .filter((l) => l !== '')
    // A file-path label the author added is not part of the source. Anything may
    // follow it - a "-- simplified" marker, an em dash and a note, a function name.
    .filter((l) => !/^\/\/\s*[\w./-]+\.go\b/.test(l))
    // A bare "// ..." is the author saying "lines omitted here".
    .filter((l) => !/^\/\/\s*(\.\.\.|…)\s*$/.test(l))
    .map((l) => l.replace(/\s+/g, ' '));

  const want = normalise(body);

  const found = corpus.some(({ lines: src }) => {
    const have = normalise(src);
    for (let i = 0; i + want.length <= have.length; i++) {
      let ok = true;
      for (let j = 0; j < want.length; j++) {
        if (have[i + j] !== want[j]) { ok = false; break; }
      }
      if (ok) return true;
    }
    return false;
  });

  if (found) { verbatim++; continue; }

  // A block may declare itself an abridged view of a type rather than a quotation.
  // The marker is explicit so that a reader can tell a deliberate omission from a
  // transcription mistake, and so can this script. Accepted spellings: a leading
  // `// simplified ...`, a `-- simplified` / `-- abridged` suffix, or the Chinese
  // 已简化. This is read from the raw body, because `normalise` strips label lines.
  const declaredSimplified = b.body.some((l) =>
    /simplified|abridged|elided|已简化/i.test(l) && /^\s*\/\//.test(l));

  if (declaredSimplified) {
    // A structural listing may annotate lines with its own comments, so compare
    // only the code part of each line - everything before a trailing `//`. That
    // still catches a renamed field or a wrong type, which is the point.
    const codePart = (l) => l.replace(/\s*\/\/.*$/, '').replace(/\s+/g, ' ').trim();
    const allSrc = new Set();
    for (const { lines: src } of corpus) for (const l of src) allSrc.add(codePart(l));
    const absent = want.map(codePart).filter((l) => l !== '' && !allSrc.has(l));
    if (absent.length === 0) { simplified++; continue; }
    notFound.push({ block: b, matched: -1, total: want.length, first: want[0], want, absent, declared: true });
    continue;
  }

  // Not verbatim. Report how much of it does match, which is the useful signal:
  // a single line out of place looks very different from a fabricated snippet.
  let best = 0;
  for (const { lines: src } of corpus) {
    const have = normalise(src);
    for (let i = 0; i + want.length <= have.length; i++) {
      let n = 0;
      while (n < want.length && have[i + n] === want[n]) n++;
      if (n > best) best = n;
    }
  }
  notFound.push({ block: b, matched: best, total: want.length, first: want[0], want });
}

console.log(`document: ${doc}`);
console.log(`\`\`\`go blocks: ${blocks.length}, verbatim: ${verbatim}, declared-simplified: ${simplified}`);
for (const n of notFound) {
  if (n.declared) {
    console.log(`\n  block at line ${n.block.line} is marked simplified, but these lines ` +
      `appear nowhere in the sources - so they are wrong, not merely omitted:`);
    n.absent.slice(0, 10).forEach((l) => console.log(`      ${JSON.stringify(l)}`));
    continue;
  }
  console.log(`\n  block at line ${n.block.line}: not found verbatim ` +
    `(longest matching run ${n.matched}/${n.total} lines)`);
  console.log(`    first line: ${JSON.stringify(n.first)}`);
  // Show which lines do not appear anywhere in the corpus at all - those are the
  // most likely to be typos or fabrications.
  const allSrc = new Set();
  for (const { lines: src } of corpus) for (const l of src) allSrc.add(l.replace(/\s+/g, ' ').trim());
  const absent = n.want.filter((l) => !allSrc.has(l));
  if (absent.length) {
    console.log(`    ${absent.length} line(s) appear nowhere in the sources:`);
    absent.slice(0, 6).forEach((l) => console.log(`      ${JSON.stringify(l)}`));
  } else {
    console.log(`    every line exists in the sources, but not as a contiguous run`);
  }
}

if (notFound.length) {
  console.log(`\n${notFound.length} block(s) failed - fix them, or add a "// simplified" marker ` +
    `and keep every line they do show accurate`);
  process.exit(1);
}
console.log('\nall quoted Go is verbatim or explicitly marked simplified');

