
/*
 * Final tsd.cc implementation for MP1 - SNS Server
 */
// tsd.cc - MP1 SNS server + Coordinator heartbeat integration
#include <ctime>
#include <algorithm>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <fstream>
#include <iostream>
#include <atomic>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <filesystem>
#include <semaphore.h>
#include <fcntl.h>


#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include <thread>
using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
namespace fs = std::filesystem;

using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;

#define MAX_DATA 256

struct Client {
    std::string username;
    bool connected = true;
    int following_file_size = 0;
    std::vector<Client*> client_followers;
    std::vector<Client*> client_following;
    ServerReaderWriter<Message, Message>* stream = nullptr;
    std::vector<Message> timeline; 
    std::unordered_map<std::string, time_t> following_since; // New map to store follow time

    bool operator==(const Client& c1) const {
        return (username == c1.username);
    }
};

std::vector<Client*> client_db;

// Utility to find client by username
Client* findClient(const std::string& username) {
    for (auto* c : client_db) {
        if (c->username == username)
            return c;
    }
    return nullptr;
}

///////////////////// Coordinator heartbeat globals /////////////////////
std::unique_ptr<CoordService::Stub> coord_stub;
std::string coord_host = "127.0.0.1";
std::string coord_port = "9090";
int cluster_id_global = 1;
int server_id_global = 1;
std::string server_listen_port = "3010"; // will be set from main args
std::string base_dir;                     // where this server persists data
std::string mirror_dir;                   // where the paired server persists data (for mirroring)
/////////////////////////////////////////////////////////////////////////

// --------------- File helpers ---------------
std::string semName(const std::string& path) {
    std::string name = "/" + path;
    std::string normalized = name;
    std::replace(normalized.begin(), normalized.end(), '/', '_');
    std::replace(normalized.begin(), normalized.end(), '.', '_');
    return normalized;
}

void ensureDirs() {
    if (!base_dir.empty()) fs::create_directories(base_dir);
    if (!mirror_dir.empty()) fs::create_directories(mirror_dir);
}

// Timeline helper: split stored line into poster/content. Format: "<poster>|<msg>"
static void splitTimelineLine(const std::string& line, const std::string& current_user, std::string& poster, std::string& content) {
    auto pos = line.find('|');
    if (pos == std::string::npos) {
        poster = current_user;
        content = line;
    } else {
        poster = line.substr(0, pos);
        content = line.substr(pos + 1);
    }
}

void semLockedAppend(const std::string& path, const std::string& line) {
    sem_t* sem = sem_open(semName(path).c_str(), O_CREAT, 0666, 1);
    sem_wait(sem);
    std::ofstream ofs(path, std::ios::app);
    ofs << line << "\n";
    ofs.close();
    sem_post(sem);
    sem_close(sem);
}

std::vector<std::string> readLines(const std::string& path) {
    std::vector<std::string> out;
    std::ifstream ifs(path);
    std::string line;
    while (std::getline(ifs, line)) {
        if (!line.empty()) out.push_back(line);
    }
    return out;
}

void touchFile(const std::string& path) {
    std::ofstream ofs(path, std::ios::app);
    ofs.close();
}

std::string userFile(const std::string& dir, const std::string& user, const std::string& suffix) {
    return dir + "/" + user + "_" + suffix;
}

bool lineExists(const std::string& path, const std::string& value) {
    auto lines = readLines(path);
    return std::find(lines.begin(), lines.end(), value) != lines.end();
}

std::vector<std::string> gatherAllUsersAcrossClusters() {
    std::unordered_set<std::string> users;
    for (int cid = 1; cid <= 3; ++cid) {
        for (int sub = 1; sub <= 2; ++sub) {
            auto lines = readLines("cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/all_users.txt");
            users.insert(lines.begin(), lines.end());
        }
    }
    return std::vector<std::string>(users.begin(), users.end());
}

std::vector<std::string> gatherFollowersAcrossClusters(const std::string& user) {
    std::unordered_set<std::string> followers;
    for (int cid = 1; cid <= 3; ++cid) {
        for (int sub = 1; sub <= 2; ++sub) {
            auto lines = readLines("cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + user + "_followers.txt");
            followers.insert(lines.begin(), lines.end());
        }
    }
    std::vector<std::string> out(followers.begin(), followers.end());
    std::sort(out.begin(), out.end());
    return out;
}

// Write follower entry for target across all clusters/subdirs (idempotent)
void addFollowerAllClusters(const std::string& target, const std::string& follower) {
    for (int cid = 1; cid <= 3; ++cid) {
        for (int sub = 1; sub <= 2; ++sub) {
            std::string followerPath = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + target + "_followers.txt";
            if (!lineExists(followerPath, follower)) {
                semLockedAppend(followerPath, follower);
            }
        }
    }
}

// Append a post to every follower's timeline file across all clusters
void appendPostToFollowerTimelinesAcrossClusters(const std::string& poster, const std::string& msg) {
    auto allUsers = gatherAllUsersAcrossClusters();
    std::string line = poster + "|" + msg;
    for (const auto& user : allUsers) {
        // check follow list files across clusters for this user
        bool follows = false;
        for (int cid = 1; cid <= 3 && !follows; ++cid) {
            for (int sub = 1; sub <= 2 && !follows; ++sub) {
                std::string followList = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + user + "_follow_list.txt";
                if (lineExists(followList, poster)) {
                    follows = true;
                }
            }
        }
        if (follows && user != poster) {
            for (int cid = 1; cid <= 3; ++cid) {
                for (int sub = 1; sub <= 2; ++sub) {
                    std::string timelineFile = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + user + "_timeline.txt";
                    semLockedAppend(timelineFile, line);
                }
            }
        }
    }
}

// Clear a user's timeline across all clusters (used when they start following to avoid past history)
void truncateTimelineAllClusters(const std::string& user) {
    for (int cid = 1; cid <= 3; ++cid) {
        for (int sub = 1; sub <= 2; ++sub) {
            std::string timelineFile = "cluster_" + std::to_string(cid) + "/" + std::to_string(sub) + "/" + user + "_timeline.txt";
            sem_t* sem = sem_open(semName(timelineFile).c_str(), O_CREAT, 0666, 1);
            sem_wait(sem);
            std::ofstream ofs(timelineFile, std::ios::trunc);
            ofs.close();
            sem_post(sem);
            sem_close(sem);
        }
    }
}

void mirrorAppend(const std::string& relPath, const std::string& line) {
    std::string primary = base_dir + "/" + relPath;
    semLockedAppend(primary, line);
    if (!mirror_dir.empty() && mirror_dir != base_dir) {
        std::string mirrorPath = mirror_dir + "/" + relPath;
        semLockedAppend(mirrorPath, line);
    }
}

Message MakeMessage(const std::string &username, const std::string &msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}

class SNSServiceImpl final : public SNSService::Service {
public:
    Status Login(ServerContext* context, const Request* request, Reply* reply) override {
        std::string uname = request->username();
        Client* existing = findClient(uname);
        if (!existing) {
            Client* new_client = new Client();
            new_client->username = uname;
            new_client->connected = true;
            client_db.push_back(new_client);
            // Only persist if not already recorded to avoid duplicates in all_users.txt
            if (!lineExists(base_dir + "/all_users.txt", uname)) {
                mirrorAppend("all_users.txt", uname);
            }
            touchFile(userFile(base_dir, uname, "followers.txt"));
            touchFile(userFile(base_dir, uname, "follow_list.txt"));
            touchFile(userFile(base_dir, uname, "timeline.txt"));
        } else {
            // Allow re-login even if a previous session did not cleanly disconnect
            existing->connected = true;
        }
        reply->set_msg("Login successful");
        return Status::OK;
    }

    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        Client* current = findClient(request->username());
        if (!current) return Status(grpc::StatusCode::NOT_FOUND, "User not found");

        // All users across all clusters (union) to avoid stale local lists
        auto all_users = gatherAllUsersAcrossClusters();
        std::sort(all_users.begin(), all_users.end());
        for (auto& u : all_users) list_reply->add_all_users(u);

        // Followers
        auto followers = gatherFollowersAcrossClusters(current->username);
        for (auto& f : followers) list_reply->add_followers(f);
        return Status::OK;
    }

    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string follower_name = request->username();
        std::string target_name = request->arguments(0);

        Client* follower = findClient(follower_name);
        Client* target = findClient(target_name);

        if (!follower) {
            reply->set_msg("User not found");
            return Status::OK;
        }
        if (follower_name == target_name) {
            reply->set_msg("Cannot follow yourself");  
            return Status::OK;
        }

        // Case 1: target is local (in-memory)
        if (target) {
            for (auto* f : follower->client_following) {
                if (f == target) {
                    reply->set_msg("Already following");
                    return Status::OK;
                }
            }

            follower->client_following.push_back(target);
            target->client_followers.push_back(follower);

        
            follower->following_since[target->username] = time(NULL);

            std::string followList = userFile(base_dir, follower_name, "follow_list.txt");
        if (!lineExists(followList, target_name)) {
            mirrorAppend(follower_name + "_follow_list.txt", target_name);
        }
        std::string followersFile = userFile(base_dir, target_name, "followers.txt");
        if (!lineExists(followersFile, follower_name)) {
            mirrorAppend(target_name + "_followers.txt", follower_name);
        }
        // propagate follower to all clusters so cross-cluster timelines/followers are in sync
        addFollowerAllClusters(target_name, follower_name);

        reply->set_msg("Followed successfully");
        return Status::OK;
    }

        // Case 2: target not local, but exists in union of all_users across clusters (cross-cluster)
        auto all_users = gatherAllUsersAcrossClusters();
        if (std::find(all_users.begin(), all_users.end(), target_name) == all_users.end()) {
            reply->set_msg("User not found");
            return Status::OK;
        }
        // avoid duplicate
        std::string followList = userFile(base_dir, follower_name, "follow_list.txt");
        if (lineExists(followList, target_name)) {
            reply->set_msg("Already following");
            return Status::OK;
        }

        follower->following_since[target_name] = time(NULL);
        mirrorAppend(follower_name + "_follow_list.txt", target_name);
        // ensure target's followers file is updated across all clusters
        addFollowerAllClusters(target_name, follower_name);
        reply->set_msg("Followed successfully");
        return Status::OK;
    }

    Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
        std::string follower_name = request->username();
        std::string target_name = request->arguments(0);

        Client* follower = findClient(follower_name);
        Client* target = findClient(target_name);

        if (!follower || !target) {
            reply->set_msg("User not found");
            return Status::OK;
        }
        if (follower == target) {
            reply->set_msg("Cannot unfollow yourself");
            return Status::OK;
        }

        auto it = std::find(follower->client_following.begin(), follower->client_following.end(), target);
        if (it == follower->client_following.end()) {
            reply->set_msg("Not following this user");
            return Status::OK;
        }

        follower->client_following.erase(it);
        follower->following_since.erase(target->username); // Remove follow timestamp

        auto it2 = std::remove_if(target->client_followers.begin(), target->client_followers.end(),
                                  [&](Client* c) { return c == follower; });
        target->client_followers.erase(it2, target->client_followers.end());

        // Persist removal - rewrite files without the entry
        auto followersList = readLines(userFile(base_dir, target_name, "followers.txt"));
        followersList.erase(std::remove(followersList.begin(), followersList.end(), follower_name), followersList.end());
        {
            sem_t* sem = sem_open(semName(userFile(base_dir, target_name, "followers.txt")).c_str(), O_CREAT, 0666, 1);
            sem_wait(sem);
            std::ofstream ofs(userFile(base_dir, target_name, "followers.txt"), std::ios::trunc);
            for (auto& u : followersList) ofs << u << "\n";
            ofs.close();
            sem_post(sem);
            sem_close(sem);
        }

        auto followList = readLines(userFile(base_dir, follower_name, "follow_list.txt"));
        followList.erase(std::remove(followList.begin(), followList.end(), target_name), followList.end());
        {
            sem_t* sem = sem_open(semName(userFile(base_dir, follower_name, "follow_list.txt")).c_str(), O_CREAT, 0666, 1);
            sem_wait(sem);
            std::ofstream ofs(userFile(base_dir, follower_name, "follow_list.txt"), std::ios::trunc);
            for (auto& u : followList) ofs << u << "\n";
            ofs.close();
            sem_post(sem);
            sem_close(sem);
        }

        reply->set_msg("Unfollowed successfully");
        return Status::OK;
    }

    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
        Client* current_user = nullptr;

        // Wait until client sends username for identification
        Message init_msg;
        if (!stream->Read(&init_msg)) {
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "No username received");
        }

        current_user = findClient(init_msg.username());
        if (!current_user) {
            return Status(grpc::StatusCode::NOT_FOUND, "User not found");
        }

        current_user->stream = stream;

        // Send last 20 messages from user's own timeline file
        auto timeline_lines = readLines(userFile(base_dir, current_user->username, "timeline.txt"));
        if (timeline_lines.size() > 20) {
            timeline_lines.erase(timeline_lines.begin(), timeline_lines.end() - 20);
        }
        for (auto& ln : timeline_lines) {
            Message m;
            std::string poster, content;
            splitTimelineLine(ln, current_user->username, poster, content);
            if (poster == current_user->username) continue; // skip own posts on own timeline
            m.set_username(poster);
            m.set_msg(content);
            Timestamp* ts = m.mutable_timestamp();
            ts->set_seconds(time(NULL));
            ts->set_nanos(0);
            stream->Write(m);
        }

        // Background watcher to push any new lines appended to this user's timeline file
        std::atomic<bool> keep_running(true);
        std::thread tail_thread([&, last_sent = timeline_lines.size()]() mutable {
            while (keep_running) {
                auto lines = readLines(userFile(base_dir, current_user->username, "timeline.txt"));
                if (lines.size() > last_sent) {
                    for (size_t i = last_sent; i < lines.size(); ++i) {
                        Message m;
                        std::string poster, content;
                        splitTimelineLine(lines[i], current_user->username, poster, content);
                        if (poster == current_user->username) continue; // skip own posts on own timeline
                        m.set_username(poster);
                        m.set_msg(content);
                        Timestamp* ts = m.mutable_timestamp();
                        ts->set_seconds(time(NULL));
                        ts->set_nanos(0);
                        stream->Write(m);
                    }
                    last_sent = lines.size();
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        });

        // Continue receiving future posts from this user
        Message client_msg;
        while (stream->Read(&client_msg)) {
            current_user->timeline.push_back(client_msg);

            // Persist to timeline file and mirror
            mirrorAppend(current_user->username + "_timeline.txt", current_user->username + "|" + client_msg.msg());
            // Fan out to followers across all clusters immediately
            appendPostToFollowerTimelinesAcrossClusters(current_user->username, client_msg.msg());

            for (auto* follower : current_user->client_followers) {
                if (follower->stream) {
                    follower->stream->Write(client_msg);
                }
            }
        }

        keep_running = false;
        if (tail_thread.joinable()) tail_thread.join();

        current_user->stream = nullptr;
        return Status::OK;
    }
};

////////////////////////// Heartbeat thread //////////////////////////
void heartbeat_loop(const std::string &coord_addr) {
    if (!coord_stub) {
        coord_stub = CoordService::NewStub(grpc::CreateChannel(coord_addr, grpc::InsecureChannelCredentials()));
    }
    while (true) {
        ServerInfo info;
        info.set_serverid(server_id_global);
        info.set_hostname("127.0.0.1");
        info.set_port(server_listen_port);
        info.set_type("server");
        info.set_clusterid(cluster_id_global);
        info.set_ismaster(server_id_global == 1);

        Confirmation conf;
        grpc::ClientContext ctx;
        grpc::Status st = coord_stub->Heartbeat(&ctx, info, &conf);
        if (!st.ok()) {
            std::cerr << "[tsd] Heartbeat RPC failed: " << st.error_message() << std::endl;
        } else {
            // successful heartbeat; optional debug:
            // std::cout << "[tsd] Heartbeat OK\n";
        }
        sleep(5);
    }
}
////////////////////////////////////////////////////////////////////////

void RunServer(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Server listening on " + server_address);

    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "3010";
    int opt = 0;
    std::string coord_h = "127.0.0.1";
    std::string coord_k = "9090";
    int cluster = 1;
    int serverid = 1;
    while ((opt = getopt(argc, argv, "p:c:s:h:k:")) != -1) {
        switch (opt) {
            case 'p': port = optarg; break;
            case 'c': cluster = atoi(optarg); break;
            case 's': serverid = atoi(optarg); break;
            case 'h': coord_h = optarg; break;
            case 'k': coord_k = optarg; break;
            default: std::cerr << "Invalid Command Line Argument\n";
        }
    }

    server_listen_port = port;
    cluster_id_global = cluster;
    server_id_global = serverid;
    coord_host = coord_h;
    coord_port = coord_k;
    std::string coord_addr = coord_host + ":" + coord_port;

    base_dir = "cluster_" + std::to_string(cluster_id_global) + "/" + std::to_string(server_id_global);
    mirror_dir = "cluster_" + std::to_string(cluster_id_global) + "/" + (server_id_global == 1 ? "2" : "1");
    ensureDirs();

    std::string log_file_name = std::string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    // Start heartbeat thread so the server registers with coordinator
    std::thread hb(heartbeat_loop, coord_addr);
    hb.detach();

    RunServer(port);

    return 0;
}
