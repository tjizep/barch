-- A pure-Luau SHA-256, so a password never has to leave this space to be
-- hashed. There is no crypto library in the Luau sandbox (see open_safe in
-- luau_driver.cpp: base, math, string, table, utf8, bit32, coroutine,
-- integer, buffer, vector, nk, simdjson - and nothing that hashes), so this
-- is the whole of it, in the vocabulary bit32 already gives every word.
--
-- Used by account.hex(salt .. password) in shopapi.luau. Not constant-time
-- and not a KDF (no iteration, no per-call cost) - good enough for an
-- example storefront, not for anything that needs to resist real attackers.

local band, bor, bxor, bnot = bit32.band, bit32.bor, bit32.bxor, bit32.bnot
local lshift, rshift, rrotate = bit32.lshift, bit32.rshift, bit32.rrotate

local K = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
}

local function pad(msg)
    local len = #msg
    local bits = len * 8
    local out = msg .. "\128"
    while (#out % 64) ~= 56 do
        out = out .. "\0"
    end
    -- an 8 byte big-endian bit length; a password is nowhere near 2^32 bits,
    -- so the high word is always zero
    local hi = math.floor(bits / 4294967296)
    local lo = bits % 4294967296
    for _, w in ipairs({hi, lo}) do
        out = out ..
            string.char(band(rshift(w, 24), 0xff), band(rshift(w, 16), 0xff),
                        band(rshift(w, 8), 0xff), band(w, 0xff))
    end
    return out
end

function hex(msg)
    local h = {0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,
               0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19}
    local m = pad(msg)
    local w = {}
    for chunk = 1, #m, 64 do
        for i = 0, 15 do
            local o = chunk + i * 4
            local b1, b2, b3, b4 = string.byte(m, o, o + 3)
            w[i + 1] = bor(lshift(b1, 24), lshift(b2, 16), lshift(b3, 8), b4)
        end
        for i = 17, 64 do
            local a, b = w[i - 15], w[i - 2]
            local s0 = bxor(rrotate(a, 7), rrotate(a, 18), rshift(a, 3))
            local s1 = bxor(rrotate(b, 17), rrotate(b, 19), rshift(b, 10))
            w[i] = (w[i - 16] + s0 + w[i - 7] + s1) % 4294967296
        end
        local a, b, c, d, e, f, g, hh = h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8]
        for i = 1, 64 do
            local s1 = bxor(rrotate(e, 6), rrotate(e, 11), rrotate(e, 25))
            local ch = bxor(band(e, f), band(bnot(e), g))
            local t1 = (hh + s1 + ch + K[i] + w[i]) % 4294967296
            local s0 = bxor(rrotate(a, 2), rrotate(a, 13), rrotate(a, 22))
            local maj = bxor(band(a, b), band(a, c), band(b, c))
            local t2 = (s0 + maj) % 4294967296
            hh, g, f, e = g, f, e, (d + t1) % 4294967296
            d, c, b, a = c, b, a, (t1 + t2) % 4294967296
        end
        h[1] = (h[1] + a) % 4294967296
        h[2] = (h[2] + b) % 4294967296
        h[3] = (h[3] + c) % 4294967296
        h[4] = (h[4] + d) % 4294967296
        h[5] = (h[5] + e) % 4294967296
        h[6] = (h[6] + f) % 4294967296
        h[7] = (h[7] + g) % 4294967296
        h[8] = (h[8] + hh) % 4294967296
    end
    local out = {}
    for i = 1, 8 do
        out[i] = string.format("%08x", h[i])
    end
    return table.concat(out)
end
