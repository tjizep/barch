#pragma once

enum OPERATION_BIT {
    eq = 1,
    gt = 2,
    lt = 4
};

extern unsigned bits_oper16(const unsigned char * a, const unsigned char * b, unsigned mask, int operbits);
