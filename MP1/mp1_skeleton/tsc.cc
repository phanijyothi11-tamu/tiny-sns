#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "client.h"
#include "sns.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

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
    Client(const std::string &hname, const std::string &uname, const std::string &p)
        : hostname(hname), username(uname), port(p) {}

protected:
    virtual int connectTo();
    virtual IReply processCommand(std::string &input);
    virtual void processTimeline();

private:
    std::string hostname;
    std::string username;
    std::string port;
    std::unique_ptr<SNSService::Stub> stub_;

    IReply Login();
    IReply List();
    IReply Follow(const std::string &username);
    IReply UnFollow(const std::string &username);
    void Timeline(const std::string &username);
};

///////////////////////////////////////////////////////////
// Connect to server and login
//////////////////////////////////////////////////////////
int Client::connectTo() {
    std::string addr = hostname + ":" + port;
    stub_ = SNSService::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));

    IReply login_reply = Login();
    if (!login_reply.grpc_status.ok() || login_reply.comm_status != SUCCESS) {
        std::cerr << "Login failed: " << login_reply.grpc_status.error_message() << std::endl;
        return -1;
    }
    std::cout << "Login successful as " << username << std::endl;
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
        ire.comm_status = SUCCESS;
        ire.grpc_status = grpc::Status::OK;
        //processTimeline();
    } else {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = grpc::Status::CANCELLED;
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
  ire.grpc_status = status;

  if (status.ok()) {
      ire.comm_status = SUCCESS;

      // Fill the vectors instead of printing
      for (int i = 0; i < list_reply.all_users_size(); i++)
          ire.all_users.push_back(list_reply.all_users(i));

      for (int i = 0; i < list_reply.followers_size(); i++)
          ire.followers.push_back(list_reply.followers(i));

  } else {
      ire.comm_status = FAILURE_UNKNOWN;
  }

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
    //std::cout << msg << std::endl;  // Only server message printed

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
    //std::cout << msg << std::endl;  // Only server message printed

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
        std::cerr << reply.msg() << std::endl; // print server message
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

///////////////////////////////////////////////////////////
// TIMELINE Streaming
//////////////////////////////////////////////////////////
void Client::Timeline(const std::string &username) {
  ClientContext context;
  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&context));

  //  Immediately send username to server for identification
  Message init_msg;
  init_msg.set_username(username);
  stream->Write(init_msg);

  // Start a reader thread to continuously read incoming posts
  std::thread reader([stream]() {
      Message server_msg;

      // Read and display messages as soon as they arrive
      while (stream->Read(&server_msg)) {
          std::time_t post_time = server_msg.timestamp().seconds();
          displayPostMessage(server_msg.username(), server_msg.msg(), post_time);
      }
  });

  // Now keep sending user posts (non-blocking read is already running)
  while (true) {
      std::string text = getPostMessage();  // wait for user input
      if (text.empty()) continue;           // ignore empty messages

      Message m = MakeMessage(username, text);
      if (!stream->Write(m)) break;         // exit if server disconnects
  }

  stream->WritesDone();
  reader.join();
}




void Client::processTimeline() {
    Timeline(username);
}

///////////////////////////////////////////////////////////
// MAIN
//////////////////////////////////////////////////////////
int main(int argc, char **argv) {
    std::string hostname = "localhost";
    std::string username = "default";
    std::string port = "3010";

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:p:")) != -1) {
        switch (opt) {
            case 'h': hostname = optarg; break;
            case 'u': username = optarg; break;
            case 'p': port = optarg; break;
            default: std::cout << "Invalid Command Line Argument\n";
        }
    }

    std::cout << "Logging Initialized. Client starting...\n";

    Client myc(hostname, username, port);
    myc.run();

    return 0;
}
