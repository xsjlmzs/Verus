
#include "tpcc.h"

Tpcc::Tpcc(Configuration* config, uint32 warehouse)
    : config_(config), warehouses_per_node_(warehouse) 
{
    int nparts = config_->replica_size_;
    warehouse_end_ = warehouses_per_node_ * nparts;
    district_end_ = warehouse_end_ + nparts * (warehouses_per_node_ * DISTRICTS_PER_WAREHOUSE);
    customer_end_ = district_end_ + nparts * (warehouses_per_node_ * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT);
    item_end_ = customer_end_ + nparts * (warehouses_per_node_ * NUMBER_OF_ITEMS);

    DBSize_ = item_end_;
}

Tpcc::~Tpcc()
{
}

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Tpcc::GetRandomKeys(std::set<uint64>* keys, uint32 num_keys, uint32 key_start, uint32 key_limit, uint32 part) {
    keys->clear();
    int nparts = config_->replica_size_;
    for (size_t i = 0; i < num_keys; i++)
    {
        uint64 key;
        do
        {
            key = key_start + part + nparts* (rand() % ((key_limit - key_start) / nparts));
        } while (keys->count(key));
        keys->insert(key);
    }
}

std::string RandomString()
{
    size_t length = rand()%18 + 40;
    std::string str = "";
    for (size_t i = 0; i < length; i++)
    {
        str += rand()%26 + 'a';
    }
    return str;
}

//--------- Create a  single-partition transaction -------------------------
PB::Txn* Tpcc::TpccTxnSP(uint64 txn_id, uint32 part)
{
    int nparts = config_->replica_size_;
    PB::Txn* txn = new PB::Txn();
    txn->set_txn_id(txn_id);

    // Add warehouse to read set.
    uint64 hotkey1 = part + nparts * (rand() % warehouses_per_node_);

    PB::Command* cmd = txn->add_commands();
    cmd->set_type(PB::OpType::GET);
    cmd->set_key(UInt64ToString(hotkey1));

    // Add district to the read-write set
    std::set<uint64> keys;
    GetRandomKeys(&keys, 1, warehouse_end_, district_end_, part);
    for (std::set<uint64>::iterator iter = keys.begin(); iter != keys.end(); ++iter)
    {
        cmd = txn->add_commands();
        cmd->set_type(PB::OpType::PUT);
        cmd->set_key(UInt64ToString(*iter));
        cmd->set_value(UInt64ToString(txn_id));
    }
    
    // Add coustomer to the read set
    keys.clear();
    GetRandomKeys(&keys, 1, district_end_, customer_end_, part);
    for (std::set<uint64>::iterator iter = keys.begin(); iter != keys.end(); ++iter)
    {
        cmd = txn->add_commands();
        cmd->set_type(PB::OpType::GET);
        cmd->set_key(UInt64ToString(*iter));
    }

    // Add item stock to the read_write set
    keys.clear();
    GetRandomKeys(&keys, 10, customer_end_, item_end_, part);
    for (std::set<uint64>::iterator iter = keys.begin(); iter != keys.end(); ++iter)
    {
        cmd = txn->add_commands();
        cmd->set_type(PB::OpType::PUT);
        cmd->set_key(UInt64ToString(*iter));
        cmd->set_value(UInt64ToString(txn_id));
    }

    return txn;
}

//----------- Create a multi-partition transaction -------------------------
PB::Txn* Tpcc::TpccTxnMP(uint64 txn_id, uint32 part1, uint32 part2)
{
    int nparts = config_->replica_size_;
    PB::Txn* txn = new PB::Txn();
    txn->set_txn_id(txn_id);

    uint64 hotkey1 = part1 + nparts * (rand() % warehouses_per_node_);
    uint64 hotkey2 = part2 + nparts * (rand() % warehouses_per_node_);

    PB::Command* cmd = txn->add_commands();
    cmd->set_type(PB::OpType::GET);
    cmd->set_key(UInt64ToString(hotkey1));

    cmd = txn->add_commands();
    cmd->set_type(PB::OpType::GET);
    cmd->set_key(UInt64ToString(hotkey2));

    // Add district to the read-write set
    std::set<uint64> keys;
    GetRandomKeys(&keys, 1, warehouse_end_, district_end_, part1);
    for (std::set<uint64>::iterator iter = keys.begin(); iter != keys.end(); ++iter)
    {
        cmd = txn->add_commands();
        cmd->set_type(PB::OpType::PUT);
        cmd->set_key(UInt64ToString(*iter));
        cmd->set_value(UInt64ToString(txn_id));
    }

    // Add customer to the read set
    keys.clear();
    GetRandomKeys(&keys, 1, district_end_, customer_end_, part1);
    for (std::set<uint64>::iterator iter = keys.begin(); iter != keys.end(); ++iter)
    {
        cmd = txn->add_commands();
        cmd->set_type(PB::OpType::GET);
        cmd->set_key(UInt64ToString(*iter));
    }
    
    // Add item stock to the read-write set
    keys.clear();
    GetRandomKeys(&keys, 10, customer_end_, item_end_, part2);
    for (std::set<uint64>::iterator iter = keys.begin(); iter != keys.end(); ++iter)
    {
        cmd = txn->add_commands();
        cmd->set_type(PB::OpType::PUT);
        cmd->set_key(UInt64ToString(*iter));
        cmd->set_value(UInt64ToString(txn_id));
    }
    return txn;
}
