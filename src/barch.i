%module barch
%include "typemaps.i"

%include <std_string.i>
%include <std_vector.i>

%{
#define SWIG_FILE_WITH_INIT
#include "swig_api.h"
%}
%template(Strings) std::vector<std::string>;
%template(Values) std::vector<Value>;
%include "swig_api.h"


