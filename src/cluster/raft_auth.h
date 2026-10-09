//
// Signing Raft messages with the cluster's shared secret - TODO 620.
//
#ifndef BARCH_CLUSTER_RAFT_AUTH_H
#define BARCH_CLUSTER_RAFT_AUTH_H

#include "nuraft.hxx"

#include <string>
#include <vector>

namespace barch::cluster {
    /*
     * NuRaft's listener takes any connection, so every message carries an
     * HMAC-SHA256 of itself, keyed by `cluster_secret`, in the metadata NuRaft
     * lets a message have. A node checks it before the message goes any further,
     * and closes the connection when it doesn't match.
     *
     * A request is signed over its header and every log entry's term, type and
     * payload: what the other side rebuilds from the wire. A response is signed
     * over its own header and its request's, because NuRaft checks a response's
     * metadata before it reads the rest. A response's context isn't covered;
     * raft_tls covers that, and replays.
     */
    std::string sign_request(const std::string& secret, nuraft::req_msg& req);
    std::string sign_response(const std::string& secret, nuraft::req_msg& req, nuraft::resp_msg& resp);
    /**
     * Hex HMAC-SHA256 of a list of words, for a RESP call one node makes to another
     * that has to show it holds the secret: CLUSTER ADMIT. `what` keeps a signature
     * for one call from passing as one for another.
     */
    std::string sign_words(const std::string& secret, const char* what, const std::vector<std::string>& words);
    /** in constant time */
    bool same_signature(const std::string& a, const std::string& b);
}

#endif //BARCH_CLUSTER_RAFT_AUTH_H
