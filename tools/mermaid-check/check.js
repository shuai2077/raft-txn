// Validates the mermaid fenced blocks in a markdown file.
//
// Rendering is not available in this environment (no network, no mermaid
// installed), so this checks the two diagram grammars used in the document
// against their structural rules. It cannot prove a diagram *looks* right; it
// proves the block is well-formed enough to parse, which is the failure mode that
// silently produces an error box instead of a diagram.
//
// Mermaid's own error messages are not available here either, so the rules below
// are deliberately strict: an unrecognised line is reported rather than skipped.

const fs = require('fs');

const file = process.argv[2];
const text = fs.readFileSync(file, 'utf8');

// Collect blocks with the absolute line number of their first body line.
const blocks = [];
const re = /^[ \t]*```mermaid[ \t]*\r?\n([\s\S]*?)^[ \t]*```[ \t]*$/gm;
let m;
while ((m = re.exec(text)) !== null) {
  const bodyStart = m.index + m[0].indexOf(m[1]);
  const line = text.slice(0, bodyStart).split(/\r?\n/).length;
  blocks.push({ line, body: m[1] });
}

const errors = [];

// Mermaid relationship operators, longest first so `o--` is not read as `--`.
const REL_OPS = [
  '<|--', '<|..', '--|>', '..|>', '*--', 'o--', '-->', '<--', '..>', '<..', '--', '..',
];

function parseRelationship(line) {
  // A[ "mult"] OP B[ "mult"] [: label]   possibly several, comma separated.
  for (const op of REL_OPS) {
    const at = line.indexOf(op);
    if (at < 0) continue;
    const lhs = line.slice(0, at).trim();
    let rest = line.slice(at + op.length).trim();
    let label = '';
    const colon = rest.indexOf(':');
    if (colon >= 0) {
      label = rest.slice(colon + 1).trim();
      rest = rest.slice(0, colon).trim();
    }
    const rhs = rest.trim();
    const sideOk = (s) =>
      /^[A-Za-z_]\w*(\s+"[^"]*")?$/.test(s.trim()) || /^"[^"]*"\s+[A-Za-z_]\w*$/.test(s.trim());
    if (sideOk(lhs) && sideOk(rhs)) {
      const name = (s) => s.trim().replace(/^"[^"]*"\s+/, '').replace(/\s+"[^"]*"$/, '');
      return { a: name(lhs), b: name(rhs), op, label };
    }
  }
  return null;
}

// A class-diagram member line:  [+#-~]name(args) type   |   type name   |   name
const MEMBER_RE = [
  /^[+\-#~][A-Za-z_(].*$/,                       // visibility-prefixed
  /^[A-Za-z_]\w*\s*\(.*\)\s*[A-Za-z_][\w,\[\]<>]*$/, // Type name(args)
  /^[A-Za-z_][\w<>,\[\]*]*\s+[A-Za-z_]\w*$/,     // Type name
  /^[A-Za-z_]\w*$/,                              // bare name / enum value
  /^[A-Za-z_]\w*\s*=\s*.+$/,                     // enum = value
  /^<<.*>>$/,                                    // stereotype
  /^\*[A-Za-z_]\w*$/,                            // abstract member marker
];

// Go-style array/slice notation is not mermaid's; flag it explicitly.
const GO_ARRAY_RE = /(^|[+\-#~]\s*|[\s(])\[\]|\[\]\s*[A-Za-z]/;

function validateClass(block) {
  const lines = block.body.replace(/\r/g, '').split('\n');
  const first = lines[0].trim();
  if (first !== 'classDiagram') {
    errors.push(`${file}:${block.line}: first line must be "classDiagram", got ${JSON.stringify(first)}`);
    return null;
  }
  const classes = new Set();
  const declared = new Set();
  let inClass = null;

  for (let i = 1; i < lines.length; i++) {
    const raw = lines[i];
    const line = raw.trim();
    const at = block.line + i;
    if (line === '' || line.startsWith('%%')) continue;

    if (inClass) {
      if (line === '}') { inClass = null; continue; }
      if (GO_ARRAY_RE.test(line)) {
        errors.push(`${file}:${at}: Go-style slice notation is not mermaid member syntax: ${JSON.stringify(line)}`);
        continue;
      }
      if (!MEMBER_RE.some((r) => r.test(line))) {
        errors.push(`${file}:${at}: unrecognised member line ${JSON.stringify(line)}`);
      }
      continue;
    }

    if (line.startsWith('direction ')) continue;

    let mm = line.match(/^class\s+([A-Za-z_]\w*)\s*\{$/);
    if (mm) {
      if (declared.has(mm[1])) errors.push(`${file}:${at}: class ${mm[1]} declared twice`);
      declared.add(mm[1]); classes.add(mm[1]); inClass = mm[1];
      continue;
    }
    mm = line.match(/^class\s+([A-Za-z_]\w*)\s*\{\s*(.*?)\s*\}\s*$/);
    if (mm) {
      classes.add(mm[1]);
      mm[2].split(/\s+/).filter(Boolean).forEach((tok) => {
        if (!MEMBER_RE.some((r) => r.test(tok))) {
          errors.push(`${file}:${at}: unrecognised class annotation ${JSON.stringify(tok)}`);
        }
      });
      continue;
    }
    mm = line.match(/^class\s+([A-Za-z_]\w*)(\s*\[.*\])?\s*$/);
    if (mm) { classes.add(mm[1]); continue; }

    const rel = parseRelationship(line);
    if (rel) { classes.add(rel.a); classes.add(rel.b); continue; }

    errors.push(`${file}:${at}: unrecognised line ${JSON.stringify(line)}`);
  }
  if (inClass) errors.push(`${file}:${block.line + lines.length}: unclosed class body for ${inClass}`);
  return { kind: 'classDiagram', nodes: classes.size };
}

function validateSequence(block) {
  const lines = block.body.replace(/\r/g, '').split('\n');
  const first = lines[0].trim();
  if (first !== 'sequenceDiagram') {
    errors.push(`${file}:${block.line}: first line must be "sequenceDiagram", got ${JSON.stringify(first)}`);
    return null;
  }
  const actors = new Set();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    const at = block.line + i;
    if (line === '' || line.startsWith('%%')) continue;
    let mm = line.match(/^(?:participant|actor)\s+([A-Za-z_]\w*)(\s+as\s+[^:]+)?$/);
    if (mm) { actors.add(mm[1]); continue; }
    if (/^Note\s+(over|left of|right of)\s+[^:]+:/.test(line)) continue;
    if (line === 'autonumber') continue;
    mm = line.match(/^([A-Za-z_]\w*)\s*(--?>>?|--?>)\s*([A-Za-z_]\w*)\s*:\s*.+$/);
    if (mm) { actors.add(mm[1]); actors.add(mm[3]); continue; }
    errors.push(`${file}:${at}: unrecognised sequence line ${JSON.stringify(line)}`);
  }
  return { kind: 'sequenceDiagram', nodes: actors.size };
}

const stats = [];
blocks.forEach((b, idx) => {
  const head = b.body.replace(/\r/g, '').split('\n')[0].trim();
  let s = null;
  if (head === 'classDiagram') s = validateClass(b);
  else if (head === 'sequenceDiagram') s = validateSequence(b);
  else errors.push(`${file}:${b.line}: unknown diagram type ${JSON.stringify(head)}`);
  if (s) stats.push({ idx: idx + 1, ...s });
});

console.log(`blocks found: ${blocks.length}`);
stats.forEach((s) => console.log(`  block ${s.idx}: ${s.kind} (${s.nodes} nodes)`));

if (errors.length) {
  console.log(`\nERRORS (${errors.length}):`);
  errors.forEach((e) => console.log('  ' + e));
  process.exit(1);
}
console.log('\nall blocks structurally valid');
