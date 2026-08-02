%module barch
%include "typemaps.i"

%include <std_string.i>
%include <std_vector.i>

%{
#define SWIG_FILE_WITH_INIT
#include "swig_api.h"
#include "configuration.h"
%}

// taking configuration from the environment on import is this binding's equivalent of
// what the valkey module does at the end of OnLoad. It runs before anything the caller
// can do, so an explicit setConfiguration() still wins.
//
// Only for the languages that have a module init to hang it on. Java has none - SWIG
// emits nothing it can attach to and the generated wrapper does not compile - so a Java
// caller configures through setConfiguration() instead.
#if defined(SWIGPYTHON) || defined(SWIGLUA)
%init %{
    barch::apply_environment_configuration();
%}
#endif
%template(Strings) std::vector<std::string>;
%template(Values) std::vector<Value>;
%include "swig_api.h"


