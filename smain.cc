#include <getopt.h>

#include "server.h"

std::string isolations[]{"READ_COMMIT", "REPEATABLE_READ"};
uint32 node_id = 0,thread_num = 16;
std::string server_path = "../conf/server_ip.conf";
std::string proxy_path = "../conf/proxy_ip.conf";
uint32 epoch_length = 10ul;
uint64 run_epoch = 100ull;
uint32 limit_txns = 0x3f3f3f3f;
// 0:RC, 1:RR, 2:SI
taas::Isolation isol = taas::kReadCommit;

int main(int argc, char *argv[])
{
    FLAGS_log_dir = "./";
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::GLOG_INFO);
    using google::INFO;
    using google::ERROR;
    using google::WARNING;
    using google::FATAL;

    // parse command line 
    while (true)
    {
        int option_index = 0;
        static struct option long_options[] = 
        {
            {"node_id",     required_argument, nullptr,    'n'},
            {"epoch_length",optional_argument, nullptr,    'e'},
            {"thread_num",  optional_argument, nullptr,    't'},
            {"run_epoch",   optional_argument, nullptr,    'r'},
            {"isolation",   optional_argument, nullptr,    'i'},
            {"limit_txns",  optional_argument, nullptr,    'l'},
            { nullptr,      0,                 nullptr,     0 }
        };

        int c = getopt_long(argc, argv, "n:e::t::r::i::l::", long_options, &option_index);
        if (c == -1)
        {
            break;
        }
        
        switch (c)
        {
        case 'n':
            node_id = std::stoi(optarg);
            break;
        case 'e':
            epoch_length = std::stoul(optarg);
            break;
        case 't':
            thread_num = std::stoi(optarg);
            break;
        case 'r':
            run_epoch = std::stoull(optarg);
            break;
        case 'i':
            isol = taas::Isolation(std::stoi(optarg));
            break;
        case 'l':
            limit_txns = std::stoul(optarg);
            break;
        case  0 :
            if (long_options[option_index].flag != nullptr)
                break;
            if (optarg)
                LOG(INFO) << "with arg " << optarg;
            break;
        default:
            break;
        }
    }
    
    LOG(INFO) << "node : " << node_id;
    LOG(INFO) << "epoch_length : " << epoch_length;
    LOG(INFO) << "thread_num : " << thread_num;
    LOG(INFO) << "run_epoch : " << run_epoch; 
    LOG(INFO) << "isolation : " << isolations[isol];
    LOG(INFO) << "limit_txns : " << limit_txns;
    std::unique_ptr<Configuration> config(new Configuration(server_path, proxy_path));
    std::unique_ptr<Connection> conn(new Connection(config.get(), config->all_nodes_[node_id]->port));
    Spin(1);

    std::unique_ptr<taas::Server> server(new taas::Server(config.get(), conn.get(), node_id));
    server->Join();    

    google::ShutdownGoogleLogging();
    return 0;
}
