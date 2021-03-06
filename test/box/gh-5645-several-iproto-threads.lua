#!/usr/bin/env tarantool

require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = os.getenv('LISTEN'),
    iproto_threads = tonumber(arg[1]),
    wal_mode='none'
})

box.schema.user.grant('guest', 'read,write,execute,create,drop', 'universe')
function errinj_set(thread_id)
    if thread_id ~= nil then
        box.error.injection.set("ERRINJ_IPROTO_SINGLE_THREAD_STAT", thread_id)
    end
end
function ping() return "pong" end
