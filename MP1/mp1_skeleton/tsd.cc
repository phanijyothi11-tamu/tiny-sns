/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

/*
 * Final tsd.cc implementation for MP1 - SNS Server
 */

 #include <ctime>
 #include <algorithm>
 #include <google/protobuf/timestamp.pb.h>
 #include <google/protobuf/duration.pb.h>
 #include <fstream>
 #include <iostream>
 #include <memory>
 #include <string>
 #include <stdlib.h>
 #include <unistd.h>
 #include <google/protobuf/util/time_util.h>
 #include <grpc++/grpc++.h>
 #include <glog/logging.h>
 #define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);
 
 #include "sns.grpc.pb.h"
 
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
 
 struct Client {
     std::string username;
     bool connected = true;
     int following_file_size = 0;
     std::vector<Client*> client_followers;
     std::vector<Client*> client_following;
     ServerReaderWriter<Message, Message>* stream = nullptr;
     std::vector<Message> timeline; // Store this client's posts
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
         } else {
             if (existing->connected) {
                 reply->set_msg("Login failed: User already logged in");
                 return Status(grpc::StatusCode::ALREADY_EXISTS, "User already logged in");
             }
             existing->connected = true;
         }
         reply->set_msg("Login successful");
         return Status::OK;
     }
 
     Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
         Client* current = findClient(request->username());
         if (!current) return Status(grpc::StatusCode::NOT_FOUND, "User not found");
 
         for (auto* c : client_db) {
             list_reply->add_all_users(c->username);
         }
 
         for (auto* f : current->client_followers) {
             list_reply->add_followers(f->username);
         }
         return Status::OK;
     }
 
     Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
         std::string follower_name = request->username();
         std::string target_name = request->arguments(0);
 
         Client* follower = findClient(follower_name);
         Client* target = findClient(target_name);
 
         if (!follower || !target) {
             reply->set_msg("User not found");
             return Status::OK;
         }
         if (follower == target) {
             reply->set_msg("Cannot follow yourself");  
             return Status::OK;
         }
 
         for (auto* f : follower->client_following) {
             if (f == target) {
                 reply->set_msg("Already following");
                 return Status::OK;
             }
         }
 
         follower->client_following.push_back(target);
         target->client_followers.push_back(follower);
 
         // Record follow time
         follower->following_since[target->username] = time(NULL);
 
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
 
         // Collect last 20 messages from followed users (after following timestamp)
         std::vector<Message> recent_posts;
 
         for (auto* followed : current_user->client_following) {
             auto follow_time = current_user->following_since[followed->username];
 
             for (auto& m : followed->timeline) {
                 if (m.timestamp().seconds() >= follow_time) {
                     recent_posts.push_back(m);
                 }
             }
         }
 
         std::sort(recent_posts.begin(), recent_posts.end(),
                   [](const Message& a, const Message& b) {
                       return a.timestamp().seconds() > b.timestamp().seconds();
                   });
 
         if (recent_posts.size() > 20) {
             recent_posts.resize(20);
         }
 
         for (auto& m : recent_posts) {
             stream->Write(m);
         }
 
         // Continue receiving future posts from this user
         Message client_msg;
         while (stream->Read(&client_msg)) {
             current_user->timeline.push_back(client_msg);
 
             for (auto* follower : current_user->client_followers) {
                 if (follower->stream) {
                     follower->stream->Write(client_msg);
                 }
             }
         }
 
         current_user->stream = nullptr;
         return Status::OK;
     }
 };
 
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
     while ((opt = getopt(argc, argv, "p:")) != -1) {
         switch (opt) {
             case 'p':
                 port = optarg;
                 break;
             default:
                 std::cerr << "Invalid Command Line Argument\n";
         }
     }
 
     std::string log_file_name = std::string("server-") + port;
     google::InitGoogleLogging(log_file_name.c_str());
     log(INFO, "Logging Initialized. Server starting...");
     RunServer(port);
 
     return 0;
 }
 