// coordinator.cc
#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;

struct zNode {
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
};

std::mutex v_mutex;
std::vector<std::vector<zNode*>> clusters = { {}, {}, {} };

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

bool zNode::isActive() {
    if (!missed_heartbeat) return true;
    return (difftime(getTimeNow(), last_heartbeat) < 10);
}

int findServer(const std::vector<zNode*>& v, int id) {
    for (size_t i = 0; i < v.size(); ++i)
        if (v[i]->serverID == id) return (int)i;
    return -1;
}

void checkHeartbeat() {
    while (true) {
        v_mutex.lock();
        for (auto& c : clusters) {
            for (auto& s : c) {
                if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                    std::cout << "[Coordinator] missed heartbeat from server " << s->serverID
                              << " (host=" << s->hostname << ", port=" << s->port << ")" << std::endl;
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }
        v_mutex.unlock();
        sleep(3);
    }
}

class CoordServiceImpl final : public CoordService::Service {
public:
    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        if (!serverinfo) {
            confirmation->set_status(false);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "no serverinfo");
        }

        std::string type = serverinfo->type();
        int clusterId = 1;
        bool parsed = false;
        try {
            clusterId = std::stoi(type);
            parsed = true;
        } catch (...) {
            std::string digits;
            for (char ch : type) if (isdigit(ch)) digits.push_back(ch);
            if (!digits.empty()) {
                try { clusterId = std::stoi(digits); parsed = true; } catch(...) { parsed=false; }
            }
        }
        if (!parsed) clusterId = 1;
        if (clusterId < 1) clusterId = 1;
        if (clusterId > 3) clusterId = 3;

        int sid = serverinfo->serverid();
        std::string host = serverinfo->hostname();
        std::string port = serverinfo->port();

        v_mutex.lock();
        auto &cluster = clusters[clusterId - 1];
        int idx = findServer(cluster, sid);
        if (idx >= 0) {
            zNode* zn = cluster[idx];
            zn->hostname = host;
            zn->port = port;
            zn->type = type;
            zn->last_heartbeat = getTimeNow();
            zn->missed_heartbeat = false;
            std::cout << "[Coordinator] Updated heartbeat for serverID=" << sid
                      << " in cluster=" << clusterId << " (host=" << host << ", port=" << port << ")\n";
        } else {
            zNode* zn = new zNode();
            zn->serverID = sid;
            zn->hostname = host;
            zn->port = port;
            zn->type = type;
            zn->last_heartbeat = getTimeNow();
            zn->missed_heartbeat = false;
            cluster.push_back(zn);
            std::cout << "[Coordinator] Registered serverID=" << sid
                      << " in cluster=" << clusterId << " (host=" << host << ", port=" << port << ")\n";
        }
        v_mutex.unlock();

        confirmation->set_status(true);
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        if (!id) return Status(grpc::StatusCode::INVALID_ARGUMENT, "no id");
        int client_id = id->id();
        if (client_id <= 0) return Status(grpc::StatusCode::INVALID_ARGUMENT, "client id > 0 required");

        int clusterId = ((client_id - 1) % 3) + 1;

        v_mutex.lock();
        auto &cluster = clusters[clusterId - 1];
        zNode* chosen = nullptr;
        for (auto& s : cluster) {
            if (s->isActive()) { chosen = s; break; }
        }
        if (!chosen && !cluster.empty()) chosen = cluster.front();
        if (!chosen) { v_mutex.unlock(); return Status(grpc::StatusCode::NOT_FOUND, "no server for cluster"); }

        serverinfo->set_serverid(chosen->serverID);
        serverinfo->set_hostname(chosen->hostname);
        serverinfo->set_port(chosen->port);
        serverinfo->set_type(chosen->type);
        v_mutex.unlock();
        return Status::OK;
    }
};

void RunServer(std::string port_no) {
    std::thread hb(checkHeartbeat);
    hb.detach();

    std::string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "[Coordinator] Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "9090";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
            case 'p': port = optarg; break;
            default: std::cerr << "Invalid Command Line Argument\n";
        }
    }
    RunServer(port);
    return 0;
}
