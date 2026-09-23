%module barch
%include "typemaps.i"

%include <std_string.i>
%include <std_vector.i>

%{
#define SWIG_FILE_WITH_INIT
#include <cstdlib>
#include "swig_api.h"
#include "configuration.h"
#include "cron.h"
#include "queue_service.h"
%}

// taking configuration from the environment on import is this binding's equivalent of
// what the valkey module does at the end of OnLoad. It runs before anything the caller
// can do, so an explicit setConfiguration() still wins.
//
// Only for the languages that have a module init to hang it on. Java has none - SWIG
// emits nothing it can attach to and the generated wrapper does not compile - so a Java
// caller configures through setConfiguration() instead.
//
// The scheduler is started here for the same reason, and stopped again on the way
// out - which is not decoration. A tick reads the key space registry, and that
// registry is a function local static built on first use, so it is destroyed
// *before* anything this init could register with atexit: the thread then walks a
// destroyed regex and a destroyed map, which is 40 heap-use-after-free reports
// under TSan and undefined behaviour without it. The hook has to run before C++
// static destruction starts, so each binding uses the one its host gives it.
// TODO 249.
#if defined(SWIGPYTHON)
%init %{
    barch::apply_environment_configuration();
    barch::cron::start();
    barch::mq::start();
    // fires from Py_FinalizeEx, well before the module's statics go
    Py_AtExit(barch::cron::stop);
    Py_AtExit(barch::mq::stop);
%}
#endif
#if defined(SWIGLUA)
%init %{
    barch::apply_environment_configuration();
    barch::cron::start();
    barch::mq::start();
    // lua has no finalization hook to hang it on, so atexit is what there is. It
    // runs late, but it still runs before the destructors registered after this
    // point, which is every static the tick actually touches.
    std::atexit(barch::cron::stop);
    std::atexit(barch::mq::stop);
%}
#endif
// a page is bytes, not text - TODO 416. Everywhere else a std::string comes back
// as a python str, which would try to decode it
#if defined(SWIGPYTHON)
%typemap(out) std::string Pages::data {
    $result = PyBytes_FromStringAndSize($1.data(), (Py_ssize_t) $1.size());
}
%typemap(out) std::string freeList {
    $result = PyBytes_FromStringAndSize($1.data(), (Py_ssize_t) $1.size());
}
%typemap(out) std::string shardStats {
    $result = PyBytes_FromStringAndSize($1.data(), (Py_ssize_t) $1.size());
}
%typemap(out) std::string StreamSave::data {
    $result = PyBytes_FromStringAndSize($1.data(), (Py_ssize_t) $1.size());
}
// and bytes back in for a streamed load - TODO 418
%typemap(in) const std::string& block_data (std::string temp) {
    char* p = nullptr;
    Py_ssize_t n = 0;
    if (PyBytes_AsStringAndSize($input, &p, &n) != 0)
        SWIG_fail;
    temp.assign(p, (size_t) n);
    $1 = &temp;
}
%typemap(freearg) const std::string& block_data ""
%typemap(typecheck) const std::string& block_data {
    $1 = PyBytes_Check($input) ? 1 : 0;
}
#endif
%template(Strings) std::vector<std::string>;
%template(Values) std::vector<Value>;
%include "swig_api.h"


