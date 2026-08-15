## N-gram text index

There is no n-gram command. Put the frames in a keyspace of their own
(`txt`) so they stay out of the document space.

Each frame is one composite key: the gram, a space, then the window
offset. `SET` splits on that space. The offset is a number, so the
parser encodes it as an integer — the same (base 128)+1 order every
numeric key already uses.

Spaces inside the gram would also split the key. Write them as `_`.

```redis
txt:SET "This 0"
txt:SET "his_i 1"
txt:SET "is_is 2"
txt:SET "s_is_a 3"
```

That is a width-5 window over `This is a doc. This is a dog`.

#### Time Complexity O(k) to write one frame, O(k + m) to RANGE a gram

A miss RANGE answers an empty array. A SET of a frame that already
exists overwrites the value.

#### Arguments

- key (composite string) -- `<gram> <offset>`. The gram is text; `_`
  stands for a space inside the window. The offset is an integer.
- value -- unused by the lookup; omit a payload or store the source
  name if you need it.

### Example

Find every frame whose gram is `is_is`:

```redis
txt:RANGE "is_is 0" "is_is 999999" 100
```

```redis
1) "is_is 2"
```

The offset sits after the gram as a number, so RANGE from offset 0 to
a large integer sees every position of that gram. Keep the source
document in another space if you want `GET` by the original name.
`REM` each frame when the source is deleted.
