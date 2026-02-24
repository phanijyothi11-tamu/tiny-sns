// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce438::AllUsers;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::SynchronizerListReply;
using csce438::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
// tl = timeline, fl = follow list
using csce438::TLFL;

int synchID = 1;
int clusterID = 1;
bool isMaster = false;
int total_number_of_registered_synchronizers = 6; // update this by asking coordinator
std::string coordAddr;
std::string clusterSubdirectory;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;
std::vector<int> registeredSyncIds; // refreshed from coordinator

std::vector<std::string> get_lines_from_file(std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user);
std::vector<std::string> get_followers_for_user_across_clusters(int targetId);
void rebuild_followers_from_follow_lists();
void rebuild_timelines_from_follow_lists();
void write_union_all_users_to_all_clusters();
void rebuild_followers_from_follow_lists();

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;

// Helper to read and merge all users across every cluster/master+slave to avoid drift
static std::vector<std::string> read_all_users_across_clusters()
{
    std::unordered_set<std::string> all;
    for (int cid = 1; cid <= 3; ++cid)
    {
        for (int sub = 1; sub <= 2; ++sub)
        {
            std::string path = "./cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/all_users.txt";
            auto lines = get_lines_from_file(path);
            all.insert(lines.begin(), lines.end());
        }
    }
    std::vector<std::string> merged(all.begin(), all.end());
    std::sort(merged.begin(), merged.end());
    return merged;
}

// Write a sorted union of all users into every cluster's all_users.txt so lists stay consistent
void write_union_all_users_to_all_clusters()
{
    std::vector<std::string> merged = read_all_users_across_clusters();
    for (int cid = 1; cid <= 3; ++cid)
    {
        for (int sub = 1; sub <= 2; ++sub)
        {
            std::string usersFile = "./cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/all_users.txt";
            std::string semName = "/" + std::to_string(cid) + "_" + std::to_string(sub) + "_all_users.txt";
            sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
            std::ofstream userStream(usersFile, std::ios::trunc);
            for (auto &u : merged)
            {
                userStream << u << std::endl;
            }
            sem_close(fileSem);
        }
    }
}

class SynchronizerRabbitMQ
{
private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    std::string hostname;
    int port;
    int synchID;

    void setupRabbitMQ()
    {
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, hostname.c_str(), port);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
    }

    void declareQueue(const std::string &queueName)
    {
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    void ensureQueuesForTargets(const std::string &suffix)
    {
        for (int id : registeredSyncIds)
        {
            declareQueue("synch" + std::to_string(id) + suffix);
        }
    }

    void publishMessage(const std::string &queueName, const std::string &message)
    {
        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

    std::string consumeMessage(const std::string &queueName, int timeout_ms = 5000)
    {
        amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type != AMQP_RESPONSE_NORMAL)
        {
            return "";
        }

        std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        amqp_destroy_envelope(&envelope);
        return message;
    }

public:
    // SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id)
    SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname("rabbitmq"), port(p), channel(1), synchID(id)
    {
        setupRabbitMQ();
        declareQueue("synch" + std::to_string(synchID) + "_users_queue");
        declareQueue("synch" + std::to_string(synchID) + "_clients_relations_queue");
        declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");
        // TODO: add or modify what kind of queues exist in your clusters based on your needs
    }

    void publishUserList()
    {
        std::vector<std::string> users = get_all_users_func(synchID);
        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users)
        {
            userList["users"].append(user);
        }
        Json::FastWriter writer;
        std::string message = writer.write(userList);
        // broadcast to all other synchronizers so they can consume from their own queues
        ensureQueuesForTargets("_users_queue");
        for (int id : registeredSyncIds)
        {
            publishMessage("synch" + std::to_string(id) + "_users_queue", message);
        }
    }

    void consumeUserLists()
    {
        std::vector<std::string> allUsers;
        std::string queueName = "synch" + std::to_string(synchID) + "_users_queue";
        while (true)
        {
            std::string message = consumeMessage(queueName, 200); // short timeout to drain
            if (message.empty()) break;
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &user : root["users"])
                {
                    allUsers.push_back(user.asString());
                }
            }
        }
        if (!allUsers.empty())
            updateAllUsersFile(allUsers);
    }

    void publishClientRelations()
    {
        Json::Value relations;
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            // use cross-cluster scan of follow lists so followers are found even if they reside in other clusters
            std::vector<std::string> followers = get_followers_for_user_across_clusters(clientId);

            Json::Value followerList(Json::arrayValue);
            for (const auto &follower : followers)
            {
                followerList.append(follower);
            }

            if (!followerList.empty())
            {
                relations[client] = followerList;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(relations);
        for (int id : registeredSyncIds)
        {
            ensureQueuesForTargets("_clients_relations_queue");
            publishMessage("synch" + std::to_string(id) + "_clients_relations_queue", message);
        }
    }

    void consumeClientRelations()
    {
        std::vector<std::string> allUsers = get_all_users_func(synchID);

        std::string queueName = "synch" + std::to_string(synchID) + "_clients_relations_queue";
        while (true)
        {
            std::string message = consumeMessage(queueName, 200); // 0.2 second timeout
            if (message.empty()) break;
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &client : allUsers)
                {
                    std::string followerFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + client + "_followers.txt";
                    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + client + "_followers.txt";
                    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

                    std::ofstream followerStream(followerFile, std::ios::app | std::ios::out | std::ios::in);
                    if (root.isMember(client))
                    {
                        for (const auto &follower : root[client])
                        {
                            if (!file_contains_user(followerFile, follower.asString()))
                            {
                                followerStream << follower.asString() << std::endl;
                            }
                        }
                    }
                    sem_close(fileSem);
                }
            }
        }
    }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client
    void publishTimelines()
    {
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            // only do this for clients in your own cluster
            if (client_cluster != clusterID)
            {
                continue;
            }

            std::vector<std::string> timeline = get_tl_or_fl(synchID, clientId, true);
            // Pull followers from all clusters (not just local) so cross-cluster timelines propagate
            std::vector<std::string> followers = get_followers_for_user_across_clusters(clientId);

            for (const auto &follower : followers)
            {
                int followerId = std::stoi(follower);
                int followerCluster = ((followerId - 1) % 3) + 1;
                // send to both master and slave synchronizer in follower's cluster
                std::vector<int> targets = {followerCluster, followerCluster + 3};

                Json::Value payload;
                payload["user"] = client;
                for (auto &entry : timeline)
                {
                    payload["timeline"].append(entry);
                }
                Json::FastWriter writer;
                std::string message = writer.write(payload);

                for (int tid : targets)
                {
                    ensureQueuesForTargets("_timeline_queue");
                    publishMessage("synch" + std::to_string(tid) + "_timeline_queue", message);
                }
            }
        }
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimelines()
    {
        std::string queueName = "synch" + std::to_string(synchID) + "_timeline_queue";
        std::string message = consumeMessage(queueName, 1000); // 1 second timeout

        if (!message.empty())
        {
            // consume the message from the queue and update the timeline file of the appropriate client with
            // the new updates to the timeline of the user it follows
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                std::string poster = root["user"].asString();
                for (const auto &entry : root["timeline"])
                {
                    std::string content = entry.asString();
                    // For every user in this cluster that follows poster, append to their timeline
                    std::vector<std::string> users = get_all_users_func(synchID);
                    for (auto &u : users)
                    {
                        std::string followFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + u + "_follow_list.txt";
                        if (file_contains_user(followFile, poster))
                        {
                            std::string timelineFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + u + "_timeline.txt";
                            std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + u + "_timeline.txt";
                            sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
                            std::ofstream tlStream(timelineFile, std::ios::app | std::ios::out | std::ios::in);
                            tlStream << content << std::endl;
                            sem_close(fileSem);
                        }
                    }
                }
            }
        }
    }

    // Force local all_users.txt to be the union of all clusters' user lists
    void mergeAllUsersFromFiles()
    {
        // push union to every cluster's all_users.txt to keep lists consistent everywhere
        write_union_all_users_to_all_clusters();
    }

private:
    void updateAllUsersFile(const std::vector<std::string> &users)
    {

        std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

        std::vector<std::string> existing = get_lines_from_file(usersFile);
        std::unordered_set<std::string> dedup(existing.begin(), existing.end());
        for (auto &u : users) dedup.insert(u);

        std::ofstream userStream(usersFile, std::ios::trunc);
        std::vector<std::string> merged(dedup.begin(), dedup.end());
        std::sort(merged.begin(), merged.end());
        for (auto &u : merged) userStream << u << std::endl;
        sem_close(fileSem);
    }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service
{
    // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    log(INFO, "Starting synchronizer server at " + server_address);
    SynchServiceImpl service;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Initialize RabbitMQ connection
    // SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);
    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672, synchID);

    std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

    // Create a consumer thread
    std::thread consumerThread([&rabbitMQ]()
                               {
        while (true) {
            rabbitMQ.consumeUserLists();
            rabbitMQ.consumeClientRelations();
            rabbitMQ.consumeTimelines();
            std::this_thread::sleep_for(std::chrono::seconds(5));
            // you can modify this sleep period as per your choice
        } });

    server->Wait();

    //   t1.join();
    //   consumerThread.join();
}

int main(int argc, char **argv)
{
    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'i':
            synchID = std::stoi(optarg);
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("synchronizer-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    clusterID = ((synchID - 1) % 3) + 1;
    clusterSubdirectory = (synchID <= 3) ? "1" : "2";
    isMaster = (clusterSubdirectory == "1");
    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    serverInfo.set_ismaster(isMaster);
    Heartbeat(coordIP, coordPort, serverInfo, synchID);

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{
    // setup coordinator stub
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    // TODO: begin synchronization process
    while (true)
    {
        // the synchronizers sync files every 5 seconds
        sleep(5);

        // Periodically force a union of all clusters' user lists into every cluster's all_users.txt
        rabbitMQ.mergeAllUsersFromFiles();
        // Periodically rebuild followers based on all follow_list files across clusters
        rebuild_followers_from_follow_lists();

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        // making a request to the coordinator to see count of follower synchronizers
        coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        for (std::string host : followerServers.hostname())
        {
            hosts.push_back(host);
        }
        for (std::string port : followerServers.port())
        {
            ports.push_back(port);
        }
        for (int serverid : followerServers.serverid())
        {
            server_ids.push_back(serverid);
        }

        registeredSyncIds = server_ids;
        // Always ensure we broadcast to all known synchronizer IDs (1..6) so lists propagate
        for (int i = 1; i <= 6; ++i) {
            if (std::find(registeredSyncIds.begin(), registeredSyncIds.end(), i) == registeredSyncIds.end()) {
                registeredSyncIds.push_back(i);
            }
        }
        total_number_of_registered_synchronizers = static_cast<int>(registeredSyncIds.size());

        // update the count of how many follower sychronizer processes the coordinator has registered

        // below here, you run all the update functions that synchronize the state across all the clusters
        // make any modifications as necessary to satisfy the assignments requirements

        // Publish user list
        rabbitMQ.publishUserList();

        // Publish client relations
        rabbitMQ.publishClientRelations();

        // Publish timelines
        rabbitMQ.publishTimelines();
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename)
{
    std::vector<std::string> users;
    std::string user;
    std::ifstream file;
    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    file.open(filename);
    if (file.peek() == std::ifstream::traits_type::eof())
    {
        // return empty vector if empty file
        // std::cout<<"returned empty vector bc empty file"<<std::endl;
        file.close();
        sem_close(fileSem);
        return users;
    }
    while (file)
    {
        getline(file, user);

        if (!user.empty())
            users.push_back(user);
    }

    file.close();
    sem_close(fileSem);

    return users;
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID)
{
    // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
    // servers to register the synchronizer with the coordinator and determine whether it is a master

    log(INFO, "Sending initial heartbeat to coordinator");
    std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort;
    std::unique_ptr<CoordService::Stub> stub = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));

    serverInfo.set_type("synchronizer");
    grpc::ClientContext ctx;
    Confirmation conf;
    grpc::Status st = stub->Heartbeat(&ctx, serverInfo, &conf);
    if (!st.ok()) {
        std::cerr << "[Synchronizer] Initial heartbeat failed: " << st.error_message() << std::endl;
    }
}

bool file_contains_user(std::string filename, std::string user)
{
    std::vector<std::string> users;
    // check username is valid
    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    users = get_lines_from_file(filename);
    for (int i = 0; i < users.size(); i++)
    {
        // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
        if (user == users[i])
        {
            // std::cout<<"found"<<std::endl;
            sem_close(fileSem);
            return true;
        }
    }
    // std::cout<<"not found"<<std::endl;
    sem_close(fileSem);
    return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
    // Merge users across all clusters/master+slave to avoid partitioned lists
    return read_all_users_across_clusters();
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
    // std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    // std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    std::string master_fn = "cluster_" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
    std::string slave_fn = "cluster_" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
    if (tl)
    {
        master_fn.append("_timeline.txt");
        slave_fn.append("_timeline.txt");
    }
    else
    {
        master_fn.append("_followers.txt");
        slave_fn.append("_followers.txt");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if (m.size() >= s.size())
    {
        return m;
    }
    else
    {
        return s;
    }
}

std::vector<std::string> getFollowersOfUser(int ID)
{
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);

    for (auto userID : usersInCluster)
    { // Examine each user's following file
        std::string file = "cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + userID + "_follow_list.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + userID + "_follow_list.txt";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
        if (file_contains_user(file, clientID))
        {
            followers.push_back(userID);
        }
        sem_close(fileSem);
    }

    return followers;
}

// Cross-cluster variant: scan every cluster/master+slave follow_list to find followers of targetId
std::vector<std::string> get_followers_for_user_across_clusters(int targetId)
{
    std::vector<std::string> followers;
    std::string target = std::to_string(targetId);
    std::vector<std::string> allUsers = get_all_users_func(synchID);

    for (auto &candidate : allUsers)
    {
        for (int cid = 1; cid <= 3; ++cid)
        {
            for (int sub = 1; sub <= 2; ++sub)
            {
                std::string file = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + candidate + "_follow_list.txt";
                if (file_contains_user(file, target))
                {
                    followers.push_back(candidate);
                    goto next_candidate;
                }
            }
        }
    next_candidate:;
    }
    return followers;
}

// Rebuild followers files for all clusters from all follow_list files (cross-cluster aware)
void rebuild_followers_from_follow_lists()
{
    std::unordered_map<std::string, std::unordered_set<std::string>> followersMap; // target -> followers
    std::vector<std::string> users = read_all_users_across_clusters();

    for (auto &u : users)
    {
        // read every follow_list for user u across clusters/subdirs
        for (int cid = 1; cid <= 3; ++cid)
        {
            for (int sub = 1; sub <= 2; ++sub)
            {
                std::string flPath = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + u + "_follow_list.txt";
                auto lines = get_lines_from_file(flPath);
                for (auto &target : lines)
                {
                    followersMap[target].insert(u);
                }
            }
        }
    }

    // write followers files for every cluster/subdir
    for (int cid = 1; cid <= 3; ++cid)
    {
        for (int sub = 1; sub <= 2; ++sub)
        {
            for (auto &entry : followersMap)
            {
                std::string followerFile = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + entry.first + "_followers.txt";
                std::vector<std::string> vals(entry.second.begin(), entry.second.end());
                std::sort(vals.begin(), vals.end());
                vals.erase(std::unique(vals.begin(), vals.end()), vals.end());

                std::string semName = "/" + std::to_string(cid) + "_" + std::to_string(sub) + "_" + entry.first + "_followers.txt";
                sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
                std::ofstream ofs(followerFile, std::ios::trunc);
                for (auto &f : vals)
                    ofs << f << std::endl;
                sem_close(fileSem);
            }
        }
    }
}

// Ensure follower timelines include posts from every user they follow (across clusters)
void rebuild_timelines_from_follow_lists()
{
    std::vector<std::string> users = read_all_users_across_clusters();

    // Build map of target -> followers based on follow_list files
    std::unordered_map<std::string, std::vector<std::string>> followersMap;
    for (auto &u : users)
    {
        for (int cid = 1; cid <= 3; ++cid)
        {
            for (int sub = 1; sub <= 2; ++sub)
            {
                std::string flPath = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + u + "_follow_list.txt";
                auto lines = get_lines_from_file(flPath);
                for (auto &target : lines)
                {
                    followersMap[target].push_back(u);
                }
            }
        }
    }

    // For each target, read their timeline (best effort union across clusters) and propagate to followers
    for (auto &entry : followersMap)
    {
        const std::string &target = entry.first;
        // Choose best timeline for target (union of both master/slave)
        std::vector<std::string> targetTimeline;
        int targetId = 0;
        try { targetId = std::stoi(target); } catch (...) {}
        int targetCluster = targetId > 0 ? ((targetId - 1) % 3) + 1 : 1;
        std::vector<std::string> t1 = get_lines_from_file("cluster_" + std::to_string(targetCluster) + "/1/" + target + "_timeline.txt");
        std::vector<std::string> t2 = get_lines_from_file("cluster_" + std::to_string(targetCluster) + "/2/" + target + "_timeline.txt");
        targetTimeline = t1.size() >= t2.size() ? t1 : t2;

        for (auto &follower : entry.second)
        {
            for (int cid = 1; cid <= 3; ++cid)
            {
                for (int sub = 1; sub <= 2; ++sub)
                {
                    std::string followerTimelinePath = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + follower + "_timeline.txt";
                    std::vector<std::string> existing = get_lines_from_file(followerTimelinePath);
                    std::unordered_set<std::string> seen(existing.begin(), existing.end());
                    bool changed = false;
                    for (auto &line : targetTimeline)
                    {
                        if (seen.insert(line).second)
                        {
                            existing.push_back(line);
                            changed = true;
                        }
                    }
                    if (changed)
                    {
                        std::string semName = "/" + std::to_string(cid) + "_" + std::to_string(sub) + "_" + follower + "_timeline.txt";
                        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
                        std::ofstream ofs(followerTimelinePath, std::ios::trunc);
                        for (auto &ln : existing) ofs << ln << std::endl;
                        sem_close(fileSem);
                    }
                }
            }
        }
    }
}
