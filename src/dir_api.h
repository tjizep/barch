#pragma once
//
// DIR - a key namespace walked as a tree. See TODO 254.
//
// `FS` is the file store, which has a layout behind it - name records, inodes and
// chunks. This is the other half: keys that are a tree only because somebody named
// them that way, with no metadata anywhere. The configuration space is full of them
// - `git/repositories/<name>/<setting>` is read by hand in git_repos.cpp today - and
// so is anything LOADKEYS imported, which joins directories with a colon.
//
// The separator is an argument rather than a convention, defaulting to `:` because
// that is what the importer produces. That is what stops this being a fifth spelling
// of "path" rather than a way of walking the four that exist.
//
#include "barch_apis.h"
#include "caller.h"

int DIR(caller& call, const arg_t& argv);
int cmd_DIR(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
void register_dir_api(function_map& r);
