#include "keyspec.h"

// signed: redis reads a negative argument as an integer and then judges it, so
// `EXPIRE k -1` is a valid number with an invalid expire time rather than a syntax
// error, and the message a caller gets has to say which - see DONE 53
std::regex art::base_key_spec::integer("-?[0-9]+");
