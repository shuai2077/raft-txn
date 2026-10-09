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
| 05 | [Workflow analysis](05-workflow-analysis.md) | [中文](05-workflow-analysis.zh-CN.md) | The behavioural view: module boundaries, what each module talks to and through which interface, and the write and read flows decomposed into numbered steps |

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

A translation is a derived artefact, so when the two disagree the **English
original is authoritative**. If you change a document, change its translation in the
same commit.
