#include "proxy.h"

Proxy::Proxy(Configuration* config, Connection* conn, Client* client)
    : config_(config), conn_(conn),client_(client)
{
    
}

Proxy::~Proxy()
{
}

void Proxy::Run()
{
    
}

uint64 Proxy::GenerateTid()
{
    uint64 tid = GetTime();
    return tid;
}
