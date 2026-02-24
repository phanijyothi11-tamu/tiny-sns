# 🌐 Tiny SNS – Scalable Distributed Social Network

A **fault-tolerant distributed social networking system** built using **C++, gRPC, and Docker**, designed to simulate large-scale social platforms with high availability, real-time communication, and multi-cluster synchronization.

---

## 🚀 Overview

Tiny SNS is a distributed system that enables users to post updates, follow other users, and receive real-time timeline feeds. The system is designed with a **Coordinator–Server–Client architecture** to support scalability, fault tolerance, and efficient communication across multiple clusters.

---

## 💡 Key Features

* ⚡ **Distributed Architecture**

  * Coordinator–Server–Client model for scalability
* 🔄 **Fault Tolerance**

  * Automatic failure detection and recovery using heartbeat monitoring
* 🌍 **Multi-Cluster Synchronization**

  * RabbitMQ-based inter-cluster communication
* 📡 **Real-Time Timeline Streaming**

  * Live updates using gRPC streaming
* 🧵 **Thread-Safe Message Handling**

  * Efficient concurrency management for high throughput
* 🐳 **Containerized Deployment**

  * Docker-based setup for easy scaling and deployment

---

## 🏗️ System Architecture

```
Client ↔ Server ↔ Coordinator ↔ Other Clusters
```

* **Coordinator**: Manages server registration, failure detection, and load balancing
* **Servers**: Handle user data, posts, and communication
* **Clients**: Provide user interface for interaction
* **RabbitMQ**: Enables cross-cluster message synchronization

---

## ⚙️ Tech Stack

* **Languages**: C++
* **Communication**: gRPC
* **Message Broker**: RabbitMQ
* **Containerization**: Docker
* **Concurrency**: Multi-threading

---

## 📊 Highlights

* Designed for **high availability and low latency**
* Supports **real-time feed updates across clusters**
* Ensures **data consistency in distributed environments**
* Handles **failures gracefully with minimal disruption**

---

## 🛠️ How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/tiny-sns.git
cd tiny-sns
```

### 2. Build the Project

```bash
mkdir build
cd build
cmake ..
make
```

### 3. Run Components

Start Coordinator:

```bash
./coordinator
```

Start Server(s):

```bash
./server
```

Start Client:

```bash
./client
```

### 4. (Optional) Run with Docker

```bash
docker-compose up --build
```

---

## 📁 Project Structure

```
tiny-sns/
│── coordinator/        # Coordinator service
│── server/             # Server logic
│── client/             # Client interface
│── proto/              # gRPC definitions
│── docker/             # Docker configuration
│── README.md
```

---

## 🧪 Future Improvements

* Load balancing strategies across clusters
* Persistent storage integration (e.g., distributed DB)
* Enhanced security (authentication & authorization)
* UI-based frontend dashboard

---

## 👩‍💻 Author

**Phani Jyothi Kurada**

---

## 📌 Note

This project is developed for academic and system design purposes, simulating real-world distributed social networking systems.
