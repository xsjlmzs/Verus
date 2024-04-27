#include <getopt.h>

#include "proxy.h"
#include "client.h"

uint32 warerhouse = 100, percent_mp = 10;
std::string server_path = "../conf/server_ip.conf";
std::string proxy_path = "../conf/proxy_ip.conf";
uint32 proxy_id = 0;

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
            {"proxy_id",     required_argument, nullptr,    'n'},
            {"warehouse",   optional_argument, nullptr,    'w'},
            {"percent_mp",  optional_argument, nullptr,    'm'},
            { nullptr,      0,                 nullptr,     0 }
        };

        int c = getopt_long(argc, argv, "n:w::m::", long_options, &option_index);
        if (c == -1)
        {
            break;
        }
        
        switch (c)
        {
        case 'n':
            proxy_id = std::stoi(optarg);
            break;
        case 'w':
            warerhouse = std::stoi(optarg);
            break;
        case 'm':
            percent_mp = std::stoi(optarg);
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

    LOG(INFO) << "proxy_id : " << proxy_id;
    LOG(INFO) << "warehouse : " << warerhouse;
    LOG(INFO) << "percent_mp : " << percent_mp;

    std::unique_ptr<Configuration> config(new Configuration(server_path, proxy_path));
    std::unique_ptr<Connection> conn(new Connection(config.get(), config->proxy_->port));
    std::unique_ptr<Client> client(new Client(config.get(), percent_mp, warerhouse));

    std::unique_ptr<Proxy>  proxy(new Proxy(config.get(), conn.get(), client.get()));
    proxy->Join();

    google::ShutdownGoogleLogging();
    return 0;
}