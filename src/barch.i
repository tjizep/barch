%module barch
%include <std_string.i>
%include <std_vector.i>

%{
#define SWIG_FILE_WITH_INIT
#include "swig_api.h"
%}
%include "swig_api.h"
