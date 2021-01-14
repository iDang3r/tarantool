#!/usr/bin/env tarantool

local granularity = 8
if arg[1] then
    granularity = tonumber(arg[1])
end

require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = os.getenv("LISTEN"),
    granularity = granularity,
})
