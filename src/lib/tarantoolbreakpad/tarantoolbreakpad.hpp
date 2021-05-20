#pragma once

/*
class Tarantoolbreakpad
{
public:

    Tarantoolbreakpad();

    ~Tarantoolbreakpad();

    int dump();

private:
    void* pimpl_{nullptr};
};
*/

extern "C" void do_dump();
