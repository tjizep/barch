
148. `store.get` cannot tell a tomb from an absent key.

    A foreign space has three states for a key, and a script can see
    only two of them: present, tombed (the source was asked and had
    nothing, cached so it is not asked again), and absent (nothing
    local, nobody has asked). `barch.store.get` answers nil for the
    last two, so a script cannot tell "this does not exist" from "I
    do not know yet".

    `sharded_store::search` has the answer and throws it away -
    sharded_store.cpp:135 returns false for a tomb and for a null
    alike. So the information is there, it just does not reach the
    interface.

    A token for the tombed case. `nil` keeps meaning "nothing here",
    a string is the value, and the token means "asked, and the
    source had nothing" - which is a fact a script may want to act
    on rather than a gap.

    What would settle it: a space with a fake source, a key that
    exists there, a key that does not, and a key nothing has asked
    for - and a script telling all three apart.
