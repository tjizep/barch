# Working notes for Claude

## Tracking work in TODO.md and DONE.md

`TODO.md` holds open questions and unverified assumptions as a numbered list. Each
entry says what is uncertain and what would settle it.

When an entry is finished, do not delete it and do not leave the detail in `TODO.md`:

1. Append the full write up to `DONE.md` as the next numbered entry. Record what was
   actually found, not just what the original entry predicted - the two often differ,
   and the difference is the useful part.
2. Replace the `TODO.md` entry, keeping its original number, with a single line:

   ```
   N. [Done] <short description> [DD-MM-YYYY] Nr <number in DONE.md>
   ```

   for example:

   ```
   5. [Done] Out of bounds read in both glob matchers [26-07-2026] Nr 1
   ```

Numbers in `TODO.md` stay put once assigned, so the remaining entries are never
renumbered and references to them keep working. `DONE.md` numbering is independent
and only ever grows.