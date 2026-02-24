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
using csce438::ServerList;

struct zNode {
    int serverID = -1;
    int clusterID = -1;
    std::string hostname;
    std::string port;
    std::string type;      // "master"/"slave"/"synchronizer"
    bool isMaster = false; // for servers
    std::time_t last_heartbeat = 0;
    int missed_count = 0;
    bool alive = true;
};

struct SyncNode {
    int serverID = -1;
    int clusterID = -1;
    std::string hostname;
    std::string port;
    std::time_t last_heartbeat = 0;
    bool alive = true;
};

std::mutex v_mutex;
// Index 0..2 for cluster 1..3
std::vector<std::vector<zNode>> clusters(3);
std::vector<SyncNode> synchronizers;

constexpr int HEARTBEAT_INTERVAL_SEC = 5;
constexpr int HEARTBEAT_MISS_THRESHOLD = 2; // 2 misses => failover

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

void promoteSlaveIfNeeded(std::vector<zNode>& cluster) {
    // If there is no alive master, promote an alive slave.
    bool hasAliveMaster = false;
    for (auto& n : cluster) {
        if (n.isMaster && n.alive) { hasAliveMaster = true; break; }
    }
    if (hasAliveMaster) return;

    for (auto& n : cluster) {
        if (!n.alive) continue;
        // Promote the first alive node
        n.isMaster = true;
        std::cout << "[Coordinator] Promoting server " << n.serverID << " in cluster " << n.clusterID << " to master\n";
        // Demote others
        for (auto& other : cluster) {
            if (other.serverID != n.serverID) other.isMaster = false;
        }
        break;
    }
}

void checkHeartbeat() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_INTERVAL_SEC));
        std::lock_guard<std::mutex> lk(v_mutex);
        std::time_t now = getTimeNow();
        for (auto& cluster : clusters) {
            for (auto& s : cluster) {
                double diff = difftime(now, s.last_heartbeat);
                if (diff > HEARTBEAT_INTERVAL_SEC) {
                    s.missed_count += 1;
                    if (s.missed_count >= HEARTBEAT_MISS_THRESHOLD) {
                        if (s.alive) {
                            std::cout << "[Coordinator] Marking server " << s.serverID << " (cluster "
                                      << s.clusterID << ") as DOWN after missed heartbeats\n";
                        }
                        s.alive = false;
                        s.isMaster = false;
                    }
                }
            }
            promoteSlaveIfNeeded(cluster);
        }

        for (auto& sync : synchronizers) {
            double diff = difftime(now, sync.last_heartbeat);
            if (diff > HEARTBEAT_INTERVAL_SEC * HEARTBEAT_MISS_THRESHOLD) {
                sync.alive = false;
            }
        }
    }
}

zNode* findServer(std::vector<zNode>& v, int id) {
    for (auto& n : v) {
        if (n.serverID == id) return &n;
    }
    return nullptr;
}

SyncNode* findSynchronizer(int id) {
    for (auto& n : synchronizers) {
        if (n.serverID == id) return &n;
    }
    return nullptr;
}

class CoordServiceImpl final : public CoordService::Service {
public:
    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        if (!serverinfo) {
            confirmation->set_status(false);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "no serverinfo");
        }

        const std::string type = serverinfo->type();
        const int clusterId = std::max(1, std::min(3, serverinfo->clusterid() > 0 ? serverinfo->clusterid() : 1));
        const int sid = serverinfo->serverid();
        const std::string host = serverinfo->hostname();
        const std::string port = serverinfo->port();

        std::lock_guard<std::mutex> lk(v_mutex);
        if (type.find("sync") != std::string::npos) {
            SyncNode* n = findSynchronizer(sid);
            if (!n) {
                synchronizers.push_back({sid, clusterId, host, port, getTimeNow(), true});
                std::cout << "[Coordinator] Registered synchronizer " << sid << " (cluster " << clusterId
                          << ") host=" << host << " port=" << port << std::endl;
            } else {
                n->hostname = host;
                n->port = port;
                n->clusterID = clusterId;
                n->last_heartbeat = getTimeNow();
                n->alive = true;
            }
            confirmation->set_status(true);
            return Status::OK;
        }

        auto& cluster = clusters[clusterId - 1];
        zNode* n = findServer(cluster, sid);
        if (!n) {
            zNode newNode;
            newNode.serverID = sid;
            newNode.clusterID = clusterId;
            newNode.hostname = host;
            newNode.port = port;
            newNode.type = type;
            newNode.isMaster = serverinfo->ismaster();
            newNode.last_heartbeat = getTimeNow();
            newNode.missed_count = 0;
            newNode.alive = true;
            cluster.push_back(newNode);
            std::cout << "[Coordinator] Registered serverID=" << sid
                      << " in cluster=" << clusterId << " (host=" << host << ", port=" << port
                      << ", isMaster=" << newNode.isMaster << ")\n";
        } else {
            n->hostname = host;
            n->port = port;
            n->type = type;
            n->isMaster = serverinfo->ismaster();
            n->last_heartbeat = getTimeNow();
            n->missed_count = 0;
            n->alive = true;
        }

        // Ensure only one master per cluster
        if (serverinfo->ismaster()) {
            for (auto& s : cluster) {
                if (s.serverID != sid) s.isMaster = false;
            }
        } else {
            promoteSlaveIfNeeded(cluster);
        }

        confirmation->set_status(true);
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        if (!id) return Status(grpc::StatusCode::INVALID_ARGUMENT, "no id");
        int client_id = id->id();
        if (client_id <= 0) return Status(grpc::StatusCode::INVALID_ARGUMENT, "client id > 0 required");

        int clusterId = ((client_id - 1) % 3) + 1;

        std::lock_guard<std::mutex> lk(v_mutex);
        auto& cluster = clusters[clusterId - 1];
        zNode* chosen = nullptr;
        for (auto& s : cluster) {
            if (s.isMaster && s.alive) { chosen = &s; break; }
        }
        if (!chosen) {
            for (auto& s : cluster) {
                if (s.alive) { chosen = &s; break; }
            }
        }
        if (!chosen) return Status(grpc::StatusCode::NOT_FOUND, "no server for cluster");

        serverinfo->set_serverid(chosen->serverID);
        serverinfo->set_hostname(chosen->hostname);
        serverinfo->set_port(chosen->port);
        serverinfo->set_type(chosen->type);
        serverinfo->set_clusterid(clusterId);
        serverinfo->set_ismaster(chosen->isMaster);
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        if (!id) return Status(grpc::StatusCode::INVALID_ARGUMENT, "no id");
        int clusterId = ((id->id() - 1) % 3) + 1;
        std::lock_guard<std::mutex> lk(v_mutex);
        auto& cluster = clusters[clusterId - 1];
        zNode* chosen = nullptr;
        for (auto& s : cluster) {
            if (!s.isMaster && s.alive) { chosen = &s; break; }
        }
        if (!chosen) return Status(grpc::StatusCode::NOT_FOUND, "no slave for cluster");
        serverinfo->set_serverid(chosen->serverID);
        serverinfo->set_hostname(chosen->hostname);
        serverinfo->set_port(chosen->port);
        serverinfo->set_type(chosen->type);
        serverinfo->set_clusterid(clusterId);
        serverinfo->set_ismaster(false);
        return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext* context, const ID* /*id*/, ServerList* out) override {
        std::lock_guard<std::mutex> lk(v_mutex);
        for (const auto& s : synchronizers) {
            if (!s.alive) continue;
            out->add_serverid(s.serverID);
            out->add_hostname(s.hostname);
            out->add_port(s.port);
            out->add_type("synchronizer");
        }
        return Status::OK;
    }

    Status GetFollowerServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        if (!id) return Status(grpc::StatusCode::INVALID_ARGUMENT, "no id");
        int sid = id->id();
        std::lock_guard<std::mutex> lk(v_mutex);
        SyncNode* n = findSynchronizer(sid);
        if (!n || !n->alive) return Status(grpc::StatusCode::NOT_FOUND, "no such synchronizer");
        serverinfo->set_serverid(n->serverID);
        serverinfo->set_hostname(n->hostname);
        serverinfo->set_port(n->port);
        serverinfo->set_type("synchronizer");
        serverinfo->set_clusterid(n->clusterID);
        serverinfo->set_ismaster(true);
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
