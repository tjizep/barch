# BARCH Documentation Standard

This guide is for agents writing or revising BARCH documentation. It applies to
the documentation site and to standalone reference pages unless a page says
otherwise.

## Write For The Reader

Write for someone who wants to use BARCH successfully, not for someone who has
already read the implementation.

- Start with what the feature does and how the reader uses it.
- Explain the consequence before the implementation detail.
- Assume the reader knows the product and its basic vocabulary, but not private
  class names, storage markers, parser stages, or historical implementation
  choices.
- Include a detail when it changes configuration, API usage, performance,
  permissions, failure handling, recovery, or data safety.
- Leave out a detail when it only says what the implementation is *not* and does
  not change anything the reader should do.

Good documentation answers questions such as:

- Where do I declare or enable this feature?
- What command or function do I call?
- What arguments and return values should I expect?
- What happens when it fails, restarts, or runs under load?
- What configuration must be set before the first use?

## Use Direct Prose

Prefer ordinary English, complete sentences, and active verbs. Use the most
direct statement that preserves the technical meaning.

Prefer:

```text
Function keys are not evicted.
```

Avoid:

```text
Never evicted. No eviction policy removes a function key.
```

Prefer:

```text
The pipe separates the gram from its numeric offset.
```

Avoid:

```text
The pipe is not a type tag and not a lead byte.
```

Prefer:

```text
Build an H3 index by storing each point as a composite key.
```

Avoid:

```text
BARCH does not need a separate geospatial command.
```

Prefer:

```text
The routing table is rebuilt from the first key in each shard when data loads.
```

Avoid:

```text
The routing table is never persisted. There is no separate index file.
```

## Use Negative Statements Carefully

Negative wording is useful when it prevents a realistic mistake or explains an
observable failure. Keep it when the reader needs to know the restriction in
order to use the feature safely.

Useful:

- “A queue uses at-least-once delivery, so a crash can deliver a message again.”
- “Cron schedules run in UTC.”
- “Changing a per-space option after the space is open requires a restart.”
- “There is no public ACK command; successful handler completion acknowledges a
  queue message.”
- “A foreign DSN belongs in a file or environment variable, not in the replicated
  configuration space.”

These statements describe an action, consequence, or recovery decision. They
are worth keeping.

Remove or rewrite negative wording when it only introduces an implementation
concept the reader did not ask about:

- “Not a type tag” is parser terminology. Say what the separator means.
- “Not a lead byte” is storage-layout terminology. Omit it unless the page is
  specifically about storage encoding.
- “Not half-honoured” is internal commentary. Say which mode the system uses.
- “Not a kill” is implementation language. Say that the call yields and then
  continues.
- “Not a transaction” is useful only when explaining rollback behavior. Otherwise
  describe the actual write and failure behavior directly.

When a contrast is necessary, name the practical choice:

```text
Foreign sources fill local misses from MySQL, Postgres, or Luau.
```

This is better than leading with a comparison to another feature the reader may
not know about.

## Avoid Contrastive Noise

Do not stack “not,” “rather than,” “instead of,” or “there is no” clauses simply
to anticipate every possible misunderstanding. One clear explanation is easier
to read than a list of rejected interpretations.

Before keeping a contrast, ask:

1. Does the reader need to choose between these two things?
2. Does the distinction change a command, setting, result, permission, or
   recovery step?
3. Would a direct description explain the behavior just as accurately?

If the answer to the first two questions is no, remove the contrast. If the
answer to the third is yes, use the direct description.

Avoid repeating the same warning in several sections. Put the full explanation
where the feature is defined, then link to it from the places where the reader
may encounter the setting.

## Keep Technical Claims Current

- Describe the implementation that exists in the checked-out source, not a
  familiar Redis feature or an earlier BARCH design.
- Do not call BARCH binary `.dat` shard files RDB files.
- Do not describe BARCH's cyclic atomic queue as an append-only file.
- Check limits and defaults against the current constants and configuration
  readers. State exact values when they are known, with an approximate unit in
  prose when that helps readers, such as “524,032 bytes, about 512 KiB.”
- Check command syntax, reply shapes, status output, and error text against the
  implementation or tests before documenting them.
- Mark branch-specific, incomplete, or intentionally ignored options clearly.
- Do not turn a parsed-but-ignored option into a promised behavior.

When a statement depends on a source-backed fact, prefer a short explanation of
the user-visible consequence over a dump of internal names.

## Structure Reference Pages Clearly

Each reference article should make its purpose obvious in the opening paragraph.
The opening should say what the reader can do with the feature and identify the
main interface or command family.

Use headings that describe the reader's task or the system behavior:

- “How the services are declared”
- “Delivery and failure behavior”
- “Schedules”
- “Measuring footprint”

Use “Design context” for the explanation of how the feature behaves, “Endpoint
reference” for syntax and fields, and “Code matrix” for working examples. Keep
examples close to the rules they demonstrate.

Tables should be concise but complete. A field row should say what the field
does, whether it is required, and its default or valid values. Do not hide an
important failure mode in a terse “—” note when it changes how the feature is
used.

## Tone And Formatting

- Use “BARCH,” “Redis,” and “Valkey” consistently when referring to products.
- Use sentence case for prose headings and natural contractions where they make
  the sentence easier to read.
- Put commands, settings, paths, function names, and exact replies in `<code>`.
- Prefer “you can,” “use,” “set,” and “call” over impersonal phrases such as
  “is utilized” or “is required in order to.”
- Keep comments in examples conversational and useful. A comment should explain
  why a step matters, not restate the code.
- Do not use a warning box to emphasize an ordinary fact. Reserve warnings for
  data loss, security, compatibility, recovery, or a failure that is easy to
  misread.

## Final Review

Before finishing a documentation change:

- Read the page as a user who has not seen the implementation.
- Search for new “not,” “never,” “rather than,” and “instead of” phrases. Keep
  each only when it protects a user decision or explains a real failure mode.
- Search for stale product terminology such as RDB, AOF, or Redis-specific names
  that do not describe BARCH's current implementation.
- Check every example against the documented syntax and the relevant test.
- Confirm that links, anchors, code blocks, and tables still render.
- Run `git diff --check` and an HTML parser or equivalent document validation.
