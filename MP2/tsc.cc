// tsc.cc - Tiny SNS client that first queries Coordinator for server routing
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "client.h"
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

using csce438::CoordService;
using csce438::ServerInfo;
using csce438::ID;

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

class Client : public IClient {
public:
    Client(const std::string &coordHost, const std::string &userId, const std::string &coordPort)
        : coord_host(coordHost), username(userId), coord_port(coordPort) {}

protected:
    virtual int connectTo();
    virtual IReply processCommand(std::string &input);
    virtual void processTimeline();

private:
    std::string coord_host;
    std::string username; 
    std::string coord_port;
    std::unique_ptr<SNSService::Stub> stub_;

    IReply Login();
    IReply List();
    IReply Follow(const std::string &username);
    IReply UnFollow(const std::string &username);
    IReply Timeline(const std::string &username);   

};

///////////////////////////////////////////////////////////
// Connect to coordinator, get server info, then login to server
//////////////////////////////////////////////////////////
int Client::connectTo() {
    std::string coord_addr = coord_host + ":" + coord_port;
    auto coord_stub = CoordService::NewStub(grpc::CreateChannel(coord_addr, grpc::InsecureChannelCredentials()));

    // client id expected to be numeric (per MP2.1)
    ID id;
    try {
        id.set_id(std::stoi(username));
    } catch (...) {
        std::cerr << "Client id must be numeric (use -u <id>)\n";
        return -1;
    }

    ServerInfo serverinfo;
    ClientContext ctx;
    Status st = coord_stub->GetServer(&ctx, id, &serverinfo);
    if (!st.ok()) {
        std::cerr << "GetServer failed: " << st.error_message() << std::endl;
        return -1;
    }

    std::string server_host = serverinfo.hostname();
    std::string server_port = serverinfo.port();
    std::string addr = server_host + ":" + server_port;

    stub_ = SNSService::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));

    IReply login_reply = Login();
    if (!login_reply.grpc_status.ok() || login_reply.comm_status != SUCCESS) {
        std::cerr << "Login failed: " << login_reply.grpc_status.error_message() << std::endl;
        return -1;
    }
    std::cout << "Login successful as " << username << " connected to " << addr << std::endl;
    return 1;
}

///////////////////////////////////////////////////////////
// Parse and process user commands
//////////////////////////////////////////////////////////
IReply Client::processCommand(std::string &input) {
    IReply ire;
    std::string cmd, arg;
    size_t space = input.find(" ");
    if (space != std::string::npos) {
        cmd = input.substr(0, space);
        arg = input.substr(space + 1);
    } else {
        cmd = input;
    }

    if (cmd == "FOLLOW") {
        ire = Follow(arg);
    } else if (cmd == "UNFOLLOW") {
        ire = UnFollow(arg);
    } else if (cmd == "LIST") {
        ire = List();
    } else if (cmd == "TIMELINE") {
   
    grpc::ClientContext ctx;
    auto health_stub = SNSService::NewStub(
        grpc::CreateChannel(coord_host + ":" + coord_port,
                            grpc::InsecureChannelCredentials()));

    // Quick RPC health check using List
    Request req;
    req.set_username(username);
    ListReply dummy;
    Status st = stub_->List(&ctx, req, &dummy);

    if (st.ok()) {
        ire.comm_status = SUCCESS;
        ire.grpc_status = grpc::Status::OK;
    } else {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = grpc::Status::OK;
    }
}

else {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = grpc::Status::OK;
    }

    return ire;
}

///////////////////////////////////////////////////////////
// CALL LIST RPC
//////////////////////////////////////////////////////////
IReply Client::List() {
    IReply ire;
    Request req;
    req.set_username(username);
    ClientContext context;
    ListReply list_reply;

    Status status = stub_->List(&context, req, &list_reply);

    if (!status.ok()) {
       
        ire.grpc_status = grpc::Status::OK;
        ire.comm_status = FAILURE_INVALID;
        return ire;
    }

  
    ire.grpc_status = grpc::Status::OK;
    ire.comm_status = SUCCESS;

    for (int i = 0; i < list_reply.all_users_size(); i++)
        ire.all_users.push_back(list_reply.all_users(i));

    for (int i = 0; i < list_reply.followers_size(); i++)
        ire.followers.push_back(list_reply.followers(i));

    return ire;
}
///////////////////////////////////////////////////////////
// FOLLOW RPC
//////////////////////////////////////////////////////////
IReply Client::Follow(const std::string &username2) {
    IReply ire;
    ClientContext context;
    Request req;
    Reply rep;

    req.set_username(username);
    req.add_arguments(username2);

    Status status = stub_->Follow(&context, req, &rep);
    ire.grpc_status = status;

    std::string msg = rep.msg();

    if (msg == "Followed successfully") {
        ire.comm_status = SUCCESS;
    } else if (msg == "Already following") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    } else if (msg == "Cannot follow yourself") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
    } else if (msg == "User not found") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }

    return ire;
}

///////////////////////////////////////////////////////////
// UNFOLLOW RPC
//////////////////////////////////////////////////////////
IReply Client::UnFollow(const std::string &username2) {
    IReply ire;
    ClientContext context;
    Request req;
    Reply rep;

    req.set_username(username);
    req.add_arguments(username2);

    Status status = stub_->UnFollow(&context, req, &rep);
    ire.grpc_status = status;

    std::string msg = rep.msg();

    if (msg == "Unfollowed successfully") {
        ire.comm_status = SUCCESS;
    } else if (msg == "Not following this user") {
        ire.comm_status = FAILURE_NOT_A_FOLLOWER;
    } else if (msg == "Cannot unfollow yourself") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    } else if (msg == "User not found") {
        ire.comm_status = FAILURE_INVALID_USERNAME;
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }

    return ire;
}

///////////////////////////////////////////////////////////
// LOGIN RPC
//////////////////////////////////////////////////////////
IReply Client::Login() {
    IReply ire;
    Request req;
    req.set_username(username);

    ClientContext context;
    Reply reply;
    Status status = stub_->Login(&context, req, &reply);
    ire.grpc_status = status;

    if (status.ok() && reply.msg() == "Login successful") {
        ire.comm_status = SUCCESS;
    } else if (reply.msg() == "Username already exists") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
        std::cerr << reply.msg() << std::endl; 
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

///////////////////////////////////////////////////////////
// TIMELINE Streaming
//////////////////////////////////////////////////////////
IReply Client::Timeline(const std::string &username) {
    IReply ire;
    ClientContext context;

    std::shared_ptr<ClientReaderWriter<Message, Message>> stream;
    try {
        stream = stub_->Timeline(&context);
    } catch (...) {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = grpc::Status::OK;
        return ire;
    }

    if (!stream) {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = grpc::Status::OK;
        return ire;
}

    Message init_msg;
    init_msg.set_username(username);
    if (!stream->Write(init_msg)) {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = grpc::Status::OK;
        return ire;
    }

    // Connection succeeded — now begin streaming
    ire.comm_status = SUCCESS;
    ire.grpc_status = grpc::Status::OK;

    // Reader thread
    std::thread reader([stream]() {
        Message server_msg;
        while (stream->Read(&server_msg)) {
            std::time_t post_time = server_msg.timestamp().seconds();
            displayPostMessage(server_msg.username(), server_msg.msg(), post_time);
        }
    });

    // Writer loop
    while (true) {
        std::string text = getPostMessage();
        if (text.empty()) continue;
        Message m = MakeMessage(username, text);
        if (!stream->Write(m)) break;
    }

    stream->WritesDone();
    reader.join();

    return ire;
}


void Client::processTimeline() {
    IReply reply = Timeline(username);
    if (reply.comm_status != SUCCESS) {
        std::cout << "Command failed with invalid command" << std::endl;
    }
}


///////////////////////////////////////////////////////////
// MAIN
//////////////////////////////////////////////////////////
int main(int argc, char **argv) {
    std::string coord_host = "localhost";
    std::string coord_port = "9090";
    std::string user_id = "1"; // numeric id required by MP2.1

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:k:u:")) != -1) {
        switch (opt) {
            case 'h': coord_host = optarg; break;   // coordinator host
            case 'k': coord_port = optarg; break;   // coordinator port
            case 'u': user_id = optarg; break;      // numeric user id
            default: std::cout << "Invalid Command Line Argument\n";
        }
    }

    std::cout << "Logging Initialized. Client starting...\n";
    Client myc(coord_host, user_id, coord_port);
    myc.run();

    return 0;
}
