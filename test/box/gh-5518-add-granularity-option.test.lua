
-- write data recover from latest snapshot
env = require('test_run')
test_run = env.new()

test_run:cmd("setopt delimiter ';'")
test_run:cmd('create server test with script=\z
             "box/gh-5518-add-granularity-option.lua"');
test_run:cmd("setopt delimiter ''");
-- Start server test with granularity == 2 failed
-- (must be greater than or equal to 4)
test_run:cmd('start server test with args="2" with crash_expected=True')
-- Start server test with granularity == 7 failed (must be exponent of 2)
test_run:cmd('start server test with args="7" with crash_expected=True')

test_run:cmd('start server test with args="4"')
test_run:cmd('switch test')

s = box.schema.space.create('test')
_ = s:create_index('test')

-- Granularity determines not only alignment of objects,
-- but also size of the objects in the pool. Thus, the greater
-- the granularity, the greater the memory loss per one memory allocation,
-- but tuples with different sizes are allocated from the same mempool,
-- and we do not lose memory on the slabs, when we have highly
-- distributed tuple sizes. This is somewhat similar to a large alloc factor

-- Try to insert/delete to space, in case when UB sanitizer on,
-- we check correct memory aligment
for i = 1, 1000 do s:insert{i, i + 1} end
-- Get item_size from box.slab.stats
-- Show it and check that item_size is a multiple of granularity
box_slab_stats = box.slab.stats()
print("box_slab_stats for granularity = 4")
test_run:cmd("setopt delimiter ';'")
for _, stats in pairs(box_slab_stats) do
    assert(type(stats) == "table")
    for key, value in pairs(stats) do
        if key == "item_size" then
            print("item_size = ", value)
            assert((value % 4) == 0)
        end
    end
end;
test_run:cmd("setopt delimiter ''");
-- Save box.slab.info in file for further comparison
-- file format: key "space" value
slab_info_4 = box.slab.info()
file = io.open("slab_info_4", "w")
test_run:cmd("setopt delimiter ';'")
for name, value in pairs(slab_info_4) do
    file:write(name) file:write(" ")
    file:write(value) file:write("\n")
end;
test_run:cmd("setopt delimiter ''");
file:close()
for i = 1, 1000 do s:delete{i} end
s:drop()
test_run:cmd('switch default')
test_run:cmd('stop server test')

test_run:cmd('start server test with args="64"')
test_run:cmd('switch test')
s = box.schema.space.create('test')
_ = s:create_index('test')
for i = 1, 1000 do s:insert{i, i + 1} end
-- Get item_size from box.slab.stats
-- Show it and check that item_size is a multiple of granularity
box_slab_stats = box.slab.stats()
print("box_slab_stats for granularity = 64")
test_run:cmd("setopt delimiter ';'")
for _, stats in pairs(box_slab_stats) do
    assert(type(stats) == "table")
    for key, value in pairs(stats) do
        if key == "item_size" then
            print("item_size = ", value)
            assert((value % 64) == 0)
        end
    end
end;
test_run:cmd("setopt delimiter ''");
-- Save box.slab.info in file for further comparison
-- file format: key "space" value
slab_info_64 = box.slab.info()
file = io.open("slab_info_64", "w")
test_run:cmd("setopt delimiter ';'")
for name, value in pairs(slab_info_64) do
    file:write(name) file:write(" ")
    file:write(value) file:write("\n")
end;
test_run:cmd("setopt delimiter ''");
file:close()
for i = 1, 1000 do s:delete{i} end
s:drop()
test_run:cmd('switch default')
test_run:cmd('stop server test')

-- Start server test with granularity = 8192
-- This is not a user case (such big granularity leads
-- to an unacceptably large memory consumption).
-- For test purposes only.
test_run:cmd('start server test with args="8192"')
test_run:cmd('switch test')
s = box.schema.space.create('test')
_ = s:create_index('test')
for i = 1, 1000 do s:insert{i, i + 1} end
-- Get item_size from box.slab.stats
-- Show it and check that item_size is a multiple of granularity
box_slab_stats = box.slab.stats()
print("box_slab_stats for granularity = 8192")
test_run:cmd("setopt delimiter ';'")
for _, stats in pairs(box_slab_stats) do
    assert(type(stats) == "table")
    for key, value in pairs(stats) do
        if key == "item_size" then
            print("item_size = ", value)
            assert((value % 8192) == 0)
        end
    end
end;
test_run:cmd("setopt delimiter ''");
-- Save box.slab.info in file for further comparison
-- file format: key "space" value
slab_info_8192 = box.slab.info()
file = io.open("slab_info_8192", "w")
test_run:cmd("setopt delimiter ';'")
for name, value in pairs(slab_info_8192) do
    file:write(name) file:write(" ")
    file:write(value) file:write("\n")
end;
test_run:cmd("setopt delimiter ''");
file:close()
for i = 1, 1000 do s:delete{i} end
s:drop()

-- Restore box.slab.info of all tests.
-- We read file line by line, first we separate first
-- word (the space is separator character), then we look for
-- the "%" character, and we separate the second word without
-- "%" (we need umerical value).
slab_info_4 = {}
test_run:cmd("setopt delimiter ';'")
for line in io.lines("slab_info_4") do
    i, j = string.find(line, "%s")
    s1 = string.sub(line, 1, j - 1)
    k = string.find(line, "%%")
    if k then
        s2 = string.sub(line, j, k - 1)
    else
        s2 = string.sub(line, j)
    end
    slab_info_4[s1] = s2
end;
test_run:cmd("setopt delimiter ''");

slab_info_64 = {}
test_run:cmd("setopt delimiter ';'")
for line in io.lines("slab_info_64") do
    i, j = string.find(line, "%s")
    s1 = string.sub(line, 1, j - 1)
    k = string.find(line, "%%")
    if k then
        s2 = string.sub(line, j, k - 1)
    else
        s2 = string.sub(line, j)
    end
    slab_info_64[s1] = s2
end;
test_run:cmd("setopt delimiter ''");

slab_info_8192 = {}
test_run:cmd("setopt delimiter ';'")
for line in io.lines("slab_info_8192") do
    i, j = string.find(line, "%s")
    s1 = string.sub(line, 1, j - 1)
    k = string.find(line, "%%")
    if k then
        s2 = string.sub(line, j, k - 1)
    else
        s2 = string.sub(line, j)
    end
    slab_info_8192[s1] = s2
end;
test_run:cmd("setopt delimiter ''");

-- After restoring box.slab.info check that the larger the granularity,
-- the larger memory usage.
test_run:cmd("setopt delimiter ';'")
for key, value in pairs(slab_info_4) do
    if (key == "items_used" or key == "arena_used" or
        key == "items_used_ratio" or key == "arena_used_ratio") then
        assert(tonumber(slab_info_4[key]) < tonumber(slab_info_64[key]) and
               tonumber(slab_info_64[key]) < tonumber(slab_info_8192[key]))
    end
end;
test_run:cmd("setopt delimiter ''");

os.execute("rm slab_info_4 slab_info_64 slab_info_8192")

test_run:cmd('switch default')
test_run:cmd('stop server test')
test_run:cmd('cleanup server test')
test_run:cmd('delete server test')
