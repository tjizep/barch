# Working notes for Claude

## Where work happens, and who commits

All work happens in `/home/test/barch` itself, on whichever branch is already checked
out, and there are three rules about this that Claude does not get to make exceptions
to on its own.

The first is that nothing gets created outside the repository root. That means no git
worktrees, and in particular none under `.claude/`, which is tool configuration rather
than somewhere to put source. If a tool offers to isolate the work into a worktree,
decline it and edit the files where they already are.

The second is that the tree does not get copied. There is no `cp -r` of the project
into a scratch directory, no second checkout under `/tmp` or a job directory, and no
taking a safe copy before editing something. Scratch space is for scripts and
intermediate output, and never for a source file that also exists here, since git is
already holding the previous version of anything you are about to change.

The third is that nothing gets committed and no branches get made. That covers
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

### Why this is written down

A worktree was created at `.claude/worktrees/ordered-sharding`, on a branch called
`worktree-ordered-sharding`, and a commit was made on it, none of which had been asked
for. It was checked on 09-08-2026 and turned out to hold nothing that the working tree
does not already have: the branch sits eleven commits behind `ordered-sharding`, and
its one unique line is a `get()` that has since been rewritten for the GCC 11 atomic
path. Because `.gitignore` line 44 already excludes `.claude/worktrees/`, nothing
tracked was affected either.

So no work was lost, and the rule is not really about that. It is about changes being
made and committed somewhere other than where the author was looking, which is a
problem even when the changes happen to be harmless.

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

## How to write

Say what to do with a thing, not what the thing "is". Finish the sentence: if a
preposition needs an object, put the object in. Prefer an ordinary verb (use, read,
check, write) over a compressed identity.

  Not: Entry 41 is the caution to read these with.
  But: Use entry 41 as an example of the level of caution when reading these.

Do not pad a step with first, at all, only then, or whether it needs changing.
Name the two actions.

  Not: So establish first whether either of these is reached on a shutdown path
       at all, and only then decide whether the lifetime needs changing.
  But: Determine if these are reached on shutdown, then decide on actual lifetime.

Short is fine. A riddle is not. One claim per sentence. More examples will be added
here as they come up.