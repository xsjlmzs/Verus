#ifndef STORAGE_H
#define STORAGE_H

#include <map>
#include <shared_mutex>
#include "common.h"
class Storage
{
private:
    std::map<std::string, std::string>* kvs_;
    std::shared_mutex smutex_;
public:
    Storage(/* args */);
    ~Storage();
    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key);

    void LockWrite();
    void UnlockWrite();
    void LockRead();
    void UnlockRead();
};



#endif