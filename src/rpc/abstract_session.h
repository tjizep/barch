//
// Created by teejip on 11/10/25.
//

#ifndef BARCH_ABSTRACT_SESSION_H
#define BARCH_ABSTRACT_SESSION_H
#include <memory>
namespace barch {
    class abstract_session {
    public:
        virtual ~abstract_session() = default;
        // schedules a call to unblock on this sessions thread
        virtual void do_block_continue() = 0;
    };

    typedef std::shared_ptr<abstract_session> abstract_session_ptr;
};
#endif //BARCH_ABSTRACT_SESSION_H