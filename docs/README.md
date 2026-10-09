# Documentation index

Five documents describe this repository. Each has an English version and a Simplified
Chinese translation. They are independent; §4 is the structural overview and §5 the
behavioural one, so those two are the best place to start.

| # | Document | 中文版 | What it covers |
|---|---|---|---|
| 01 | [Raft analysis](01-raft-analysis.md) | [中文](01-raft-analysis.zh-CN.md) | The upstream `eliben/raft` project: architecture, elections, replication, the Part-3 optimisations, the KV service, exactly-once delivery, and what it deliberately leaves out |
| 02 | [Transaction design](02-transaction-design.md) | [中文](02-transaction-design.zh-CN.md) | The transaction layer: isolation model, the 2PC commit protocol, lock resolution rules and their safety argument, the timestamp oracle, and rejected alternatives |
| 03 | [Read barrier](03-read-barrier.md) | [中文](03-read-barrier.zh-CN.md) | Why a cross-key snapshot read was inconsistent, the log-position argument that closes it, what it does *not* cover, and the lease-rollback race found while investigating it |
| 04 | [UML class diagrams](04-uml-class-diagram.md) | [中文](04-uml-class-diagram.zh-CN.md) | The structural view: layer dependencies, a class diagram per package, the commit path as a sequence diagram, and how the diagrams were validated |
| 05 | [Workflow analysis](05-workflow-analysis.md) | [中文](05-workflow-analysis.zh-CN.md) | The behavioural view: module boundaries, what each module talks to and through which interface, the write and read flows decomposed into numbered steps, and a code-level walkthrough of the transaction mechanism and its interaction with the consensus layer |

Suggested reading order: **04** for the shape of the system, **05** for how it behaves
at run time, **01** for the foundation it is built on, **02** for the transaction
protocol, **03** for the two correctness issues that took the most work to get right.

## Keeping the translations honest

The Chinese files are translations, not rewrites, and their structure is checked
mechanically rather than trusted:

```sh
node tools/doc-check/verify-translation.js docs/02-transaction-design.md docs/02-transaction-design.zh-CN.md
```

That verifier checks what a translation must preserve — identical code blocks,
identical mermaid diagrams, the same headings at the same levels, the same link
targets, and neither a lost nor an invented inline `code span`. It cannot judge
prose, and it says so in its own header comment.

For mermaid blocks it distinguishes two cases, because treating them alike would be
wrong in one direction or the other. A `classDiagram` describes the code, so it must be
byte-identical in every language. A flowchart, sequence diagram or state diagram
carries prose labels that *should* be translated, so those are compared on structure:
node ids, arrows, brackets and line count must all survive, and only labels may differ.

## Keeping the quoted code honest

Documents in this repository quote Go source at length, and a hand-retyped snippet is
indistinguishable from a wrong one unless something checks. So:

```sh
node tools/doc-check/verify-quoted-code.js docs/05-workflow-analysis.zh-CN.md .
```

This extracts every ```` ```go ```` block and requires it to appear verbatim in the Go
sources. A block may declare itself an abridged view by carrying a `-- simplified`
marker on its label line; even then, every line it *does* show must exist in the
sources — so a renamed field, a wrong type or an invented identifier still fails.

That check has already caught real errors, which is the point of having it: the
workflow analysis described `raft.Storage.Set` as taking `any` when it takes `[]byte`,
and one snippet wrote `entries :=` where the source has `entries =`. Both are fixed.

## Which document is authoritative

A translation is a derived artefact, so when the two disagree the **English original is
authoritative** — except for `docs/05`, which was written in Chinese first. If you
change a document, change its translation in the same commit, and run both scripts
above.
