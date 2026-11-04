## Response Style
Always reply in clear, natural, everyday English — the way a helpful person would speak or write in normal conversation.
Use fluent, direct wording, contractions where they feel natural, and short-to-medium sentences.
Prefer to using the Hemingway style: short sentences, zero emotional adjectives, brutally direct, and plain English.

## Comments & Documentation Style
When writing or editing comments, docs, or explanatory text:
- See docs/DOCUMENTATION-STANDARD.md for more documentation standards.
- Prefer to using the Hemingway style: short sentences, zero emotional adjectives, brutally direct, and plain English.
- If you find *anything* including your own output that is contary immediately rewrite it using todo flow below

## who commits

Nothing gets committed and no branches get made. That covers
`git commit`, `git branch`, `git checkout -b`, `git merge`, `git rebase`, `git push`
and `git tag`. Leave the changes sitting in the working tree, say what was changed, and
let the commit be made by hand, because deciding what becomes a commit is the author's
job. This applies just as much when the change is finished and tested and obviously
correct, since that is precisely the situation in which an unwanted commit is hardest
to spot afterwards.

Reading git is fine and worth doing - `status`, `diff`, `log`, `show`, `blame` and
`worktree list` are all encouraged, and understanding the history usually makes the
work better. If a task looks like it genuinely needs one of the three things above,
stop and ask rather than finding a way around it.

## Tracking work in TODO.md and DONE.md

`TODO.md` holds open questions and unverified assumptions as a numbered list. Each
entry says what is uncertain and what would settle it.

Open a new `TODO.md` entry for every instruction from the user that will change
the tree, unless an existing entry already covers that work. Write the entry
before the edits: what was asked, and what would settle it. Questions, reviews,
and planning that do not change the tree do not get one. Do not open a second
entry for work that already has a number.

When an entry is finished, do not delete it and do not leave the detail in `TODO.md`:

1. Append the full write up to `DONE.md` as the next numbered entry. Record what was
   actually found, not just what the original entry predicted - the two often differ,
   and the difference is the useful part.
2. Replace the `TODO.md` entry, keeping its original number, with a single line:

   ```
   N. [Done] <short description> [DD-MM-YYYY] Nr <number in DONE.md> <git hash>
   ```

   for example:

   ```
   5. [Done] Out of bounds read in both glob matchers [26-07-2026] Nr 1 a98494b
   ```

   The hash is `git rev-parse --short HEAD` at the moment the entry is closed.
   Nothing is committed here, so that is the tree the working copy sits on, not
   a commit of the change. Older done lines have no hash; do not backfill them.

Numbers in `TODO.md` stay put once assigned, so the remaining entries are never
renumbered and references to them keep working. `DONE.md` numbering is independent
and only ever grows.
