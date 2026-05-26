---
name: write-commit-message
description: Draft a commit message for an Axiom commit. Use when the user asks to write, draft, or compose a commit message for an Axiom change. Encodes the project's content rules so the draft is showable without a separate review pass.
---

# Write Commit Message

Drafts a commit message that follows the rules in `.claude/CLAUDE.md` (sections "Commit Messages" and "Body length and shape"). The rules there are authoritative — this skill is the workflow for applying them.

## Guiding principle

**Write to orient a reviewer, not to defend the change.** A commit message's job is to orient the reader to the change — not to enumerate every affected file, restate every claim at multiple abstraction levels, or hedge against "you didn't mention X". The diff and the version-control history are the system of record. The message picks the smallest set of facts the reader needs to navigate the diff, and stops.

Apply the **would-I-say-this-aloud** test to every sentence: read it as if briefing a teammate verbally. Sentences that exist to prove a claim, recite enum values, attribute jargon to a subsystem, or acknowledge symbols by name die on first contact with speech. Sentences that orient — "this query failed", "Velox doesn't support it", "treat it as a regular function call" — survive. If a sentence wouldn't survive being spoken aloud, delete it.

The per-pattern rules below all derive from this principle. If a draft passes the rules but still reads like a legal brief, trust the principle and trim further.

## Process

1. **Read the rules** — Open `.claude/CLAUDE.md` and re-read the "Commit Messages" and "Body length and shape" sections. Do not draft from memory.

2. **Gather facts from the diff itself** — Never draft from the conversation alone. The conversation has scaffolding; the diff is the truth.
   - Read the existing commit message (if amending).
   - Read the full diff, not just file names.
   - List the files touched.
   - If the change fixes a bug, identify the user-visible symptom (error string, wrong output, crash) from the diff or the conversation. If you cannot state the symptom concretely, ask the user before drafting.

3. **Draft** — Match length to the change. Each paragraph is one long line (no hard wraps). The right shape depends on what the diff actually does:

   - **Trivial** (typo fix, comment edit, one-line rename, dependency bump): title alone, or title + one sentence + one-line test plan. Padding a trivial change with three paragraphs is a fail.
   - **Small** (a focused bug fix, a single-file refactor, a small new helper): title + one paragraph (what + why with a concrete anchor) + one-line test plan.
   - **Standard** (most fixes and features): title + 2-4 short paragraphs as below.
   - **Large**: if you find yourself wanting >4 body paragraphs, the change should probably be split — or the extra material belongs in separate documentation (design doc, issue, wiki page) that the summary links to, not inlined in the commit message.

   **Title**: `[Axiom] type(scope): Description` — capital start, no trailing period, ≤67 chars. Type ∈ {feat, fix, refactor, test, docs}. Scope optional.

   Body paragraphs (include only those that carry weight for this change):
   - **What + why**: lead with user-visible behavior change. Include one concrete example query, error message, or before/after fact. A reader without internals knowledge should get the gist.
   - **Mechanism**: the core idea as ONE concept — a new field, a swapped algorithm, an added check, a rewrite step. Skip when the title + "what + why" already conveys it.
   - **Deferred**: name a deliberately-not-done case and how it surfaces (NYI message, follow-up issue). Skip if none.
   - **Test plan**: high-level coverage. Name the test file(s) and what scenarios they cover. If the test work covers more than one distinct category (e.g., extracting tests + adding new coverage + a bug-fix regression test), use bullets — one per category. Don't cram multiple categories into one comma-separated sentence. Don't write "tests pass" or "CI green" — CI reports that; restating it is noise. State what was *covered*, not that it succeeded. For pure refactors with no new tests, omit the Test Plan section entirely — "existing tests cover this" / "covered by CI" is implied by "pure refactor" and adds no information.

   **Prose clarity** — write so a tired reader gets each sentence on first read.
   - Prefer short sentences. If a sentence has two clauses joined by "so", "because", "but", "even though", "although", or a comma + participle, consider splitting it. Contrastive joiners ("but X", "even though Y") are especially risky when both halves introduce a fact the reader does not already have — pack two new facts into one sentence and the reader stalls. State each rule in its own sentence, then connect them.
   - Avoid stacked abstractions like "left the outer scope advertising the column as X" or "the projection inherits the source's reverseLookup names". Replace with a concrete chain.
   - Avoid compiler/optimizer jargon ("outer reference", "outer scope", "binding context", "name resolution scope") unless the rest of the paragraph already established it. If you must use it, define it inline with a tiny example.
   - Prefer plain verbs (`used`, `dropped`, `kept`) over jargon verbs (`advertise`, `surface`, `propagate`, `materialize`) unless the jargon is the precise term.
   - Avoid hyphenated compound-noun stacks ("user-written case", "lookup-based fallback", "context-aware resolver"). They require the reader to unpack a modifier chain before getting to the noun. Rewrite as a relative clause ("the case the user wrote") or a single concrete noun.
   - Prefer the word with one obvious meaning in this context. "Case" can mean legal case, match case, or upper/lower case — use "capitalization" when you mean letter case. Similarly: "operator" vs "function", "key" vs "column", "type" vs "kind" — pick the one a SQL reader and a C++ reader both interpret the same way.
   - Show a complete example query that demonstrates the failure, paired with the resulting error. Don't describe a query in prose ("a reference to X from an outer query") when you can show one (`SELECT x FROM (...)`); the example carries the meaning without the reader holding context across sentences.
   - Break any long code, query, or error string out into a fenced block on its own line. "Long" means more than ~6 words or anything that wraps the surrounding paragraph awkwardly. This applies whether the long string is paired with another or stands alone — a single long error string embedded mid-sentence still overloads the reader. Patterns:

     Lead with the error, then explain:

     ```
     Planning failed with:

         Ranking filter limit not consumed by RowNumber or TopNRowNumber

     This happened for ranking queries whose ORDER BY became empty after redundancy elimination.
     ```

     Query + result:

     ```
     For example:

         SELECT x FROM (SELECT x_2024 AS x FROM src)

     fails with `Cannot resolve column` — the inner subquery produces a column named `x_2024`, not `x`.
     ```

     Short fragments (a single column name, a flag, a 2-3-word error name) stay inline.
   - Any enumeration of 3+ items goes in a sub-bullet list, not in a sentence — even a short one. Examples: list of recognized constructs, list of new flags, list of test scenarios. Forcing the reader to parse a list while tracking the surrounding clause overloads them.
   - For deletions and additions, lead with the active verb: "Removes the foo helper — no longer needed." not "The foo helper is no longer needed and is removed." Easier to skim and locate.
   - When in doubt, read the paragraph aloud. If you pause mid-sentence to decode it, split or simplify it.

4. **Self-check before showing** — Walk every item; do not skip any.
   - [ ] Title matches `[Axiom] type(scope): Description`, capital, no period, ≤67 chars.
   - [ ] Para 1 leads with behavior, not internal symbol names.
   - [ ] Para 1 has a concrete anchor (example, error message, before/after).
   - [ ] Mechanism is one concept, not a diff retrace with sibling-function names.
   - [ ] No reasoning journey (alternatives considered, sibling reused, layered fixes).
   - [ ] Code symbols in plain backticks; never `` \`escaped\` ``.
   - [ ] Each paragraph is ONE long line. No hard wrap at 72/80 columns. Bullet lists in the test plan are the exception.
   - [ ] No "tests pass" / "N tests pass" / "CI green".
   - [ ] Every factual claim verified against the diff, not recalled.
   - [ ] Reads in ~30 seconds.
   - [ ] Length matches the change. Trivial changes are not padded to standard length; standard changes are not condensed to one line.
   - [ ] Prose clarity: no sentence longer than ~30 words; no stacked abstractions ("X advertising Y", "scope of Z"). Each sentence is parseable on first read.
   - [ ] No comma-separated enumeration of 3+ items inside any sentence — lists go in sub-bullets.
   - [ ] Sentences describing deletions or additions lead with the active verb ("Removes X", "Adds Y").
   - [ ] Test Plan uses bullets if covering 2 or more distinct categories.
   - [ ] Every sentence passes the would-I-say-this-aloud test. No defensive citations (file:line, enum value lists), no claims restated at a different abstraction level, no symbol-by-symbol cleanup recitations.

   If any item fails, fix the draft before showing.

5. **Show the draft** — Present the final draft. Mention any facts you could not verify and want the user to confirm.

## Common failure modes to avoid

These are the patterns drafts most often hit, and that this skill exists to prevent:

- **Defensive completionism** (the meta-pattern). Each clause exists to ward off "you didn't mention X" rather than to inform the reader: citing source file:line to back a claim ("`Subfield.h:28-33` enumerates only ..."), restating the same fact at two abstraction levels ("step has no Velox representation" + "Velox doesn't support pushdown"), enumerating removed symbols/files in a cleanup tail ("Removes the dead enum, its handlers in `A`/`B`/`C`, and the `X::registerY`/`y()` accessors"), or hedging title phrasings ("X without crash"). All five derive from writing for a future challenger instead of a present reader. Apply the would-I-say-this-aloud test; cut everything that wouldn't survive being spoken.
- **Restating the diff as prose** — "Adds X. Modifies Y. Changes Z." That's what the diff shows. State the behavior change and the one concept behind it.
- **Function-by-function walkthrough** — "In `foo()`, we now do A. In `bar()`, we adjust B." The reviewer reads the diff for that. Collapse into the single mechanism.
- **Enumerating touched files, classes, or call sites** — "Adopt it in `Foo`, `Bar`, `Baz`, `Qux`, and `Quux`." or "Updated across N call sites." The diff is the source of truth for scope. Naming the touched symbols eats space without informing the reader. Exception: name a specific file only when its role in the change is not obvious from the title (e.g., the test file that needed expectation updates, or the one production file the rest of the diff supports).
- **Long lists embedded in a sentence** — any comma-separated enumeration of more than ~3 items inside a sentence forces the reader to parse a list while tracking the surrounding clause. Examples: a list of test scenarios ("covers aliases, qualified refs, star expansion, CTE expansion, AliasedRelation, set operations, and global aggregation"), a list of SQL variants ("DATE+INTERVAL, TIMESTAMP+INTERVAL, BIGINT same-type, INTEGER→BIGINT coercion, DESC ordering, ..."). Fix by either (a) breaking the list out into its own bullet sub-list, or (b) summarizing as a category ("the main alias-resolution scenarios"). The bullet form scales with item count; the summary form is right when individual items don't carry weight.
- **Missing big picture** — Diving into internal symbols in paragraph 1. Lead with user-visible behavior; descend into mechanism in paragraph 2.
- **Reasoning scaffolding** — "We considered X but chose Y because Z." Belongs in design docs or PR threads, not the commit log.
- **Hard-wrapped paragraphs** — Hard line breaks at ~70/80 columns render as ragged short lines wherever the message is reflowed.
- **Pass-counting test plans** — "All 47 tests pass." CI says that. State *what was covered*, not that it succeeded.
- **Stale Test Plan after amend** — when amending a commit whose diff has grown or changed shape (new test file, new coverage, dropped scenarios), re-derive the Test Plan from the current diff. Text written for the first draft of the change rots: it references items that no longer exist or omits new categories. Re-read the diff before keeping the prior Test Plan as-is.

## When NOT to invoke

- Pure typo fixes to an already-approved message.
- The user provides the full message themselves and asks you to commit it verbatim.

For all other Axiom commit-message work — drafting, revising, or rewriting — invoke this skill.
