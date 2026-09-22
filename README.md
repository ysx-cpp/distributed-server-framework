# 分布式游戏服务器框架

分布式游戏服务器框架是一个基于 C++ 的分布式游戏服务器集群框架。项目采用进程化微服务架构，将玩家接入、登录认证、消息路由、业务逻辑、场景管理、战斗协调、战斗引擎与数据库访问拆分为独立节点，便于按服务职责扩展和部署。

![架构全景图](docs/architecture/panoramic_architecture_diagram.png)

## 架构设计概述

### 服务节点

| 节点 | 目录 | 主要职责 |
|------|------|----------|
| Router Cluster | `src/router/` | 数据面控制平面 + 配置与路由表中心：服务注册发现、连接心跳、跨 Region 路由转发、故障转移、Region 拓扑管理、远程配置分发、路由表配置下发 |
| Gateway | `src/gateway/` | 玩家 TCP / WebSocket 长连接、心跳检测、连接映射、CS 下行消息拦截器 |
| Login | `src/login/` | HTTP 注册/登录、OAuth 2.0 认证（Google/Facebook/Apple）、会话与 token 创建 |
| Logic | `src/logic/` | 会话验证、玩家在线数据管理、核心业务逻辑、GM 命令、OnBefore 中打开用户共享内存 |
| Scenes | `src/scenes/` | 场景、地图、房间/大厅相关状态与同步逻辑、gRPC 预留跨语言接口 |
| Global | `src/global/` | **战斗服（顺位选举高可用）**：战斗匹配、战斗队列管理、跨战斗实例协调、排行榜、全服广播；选举优先级 本机 rank-1 → 本机 rank-2 → … → 远程 rank-1 → …，状态 SHM→Redis→MySQL 三级恢复 |
| DBAgent | `src/dbagent/` | MySQL 统一访问入口、账号/用户/第三方账号读写、动态建表（基于 protobuf field options） |
| Battle | `src/battle/` | 战斗引擎实例：BattleInstance + BattleRule、结算（SettleManager）、战报（StatsManager）、battlerpc.proto |

### 通信与数据流

客户端登录后通过 `Gateway` 建立 TCP 或 WebSocket 长连接，业务消息经 **CompositeTransport** 决策后走 LocalTransport（同 region，SHMQ 直连目标服务队列）或 RemoteTransport（跨 region，TCP → Router Cluster 一致性哈希转发）。`Router Cluster` 同时承担远程配置分发和路由表管理，各服务启动时从 Router Cluster 拉取 `serverconf` / `redisconf` / `mysqlconf` / `oauthconf`。`Logic` 负责会话与业务处理，需要持久化时通过 `DBAgent` 访问 MySQL，并通过 Redis 缓存会话、账号索引。`Global` 作为战斗服协调匹配与跨战斗调度，`Battle` 作为战斗引擎实例执行具体战斗逻辑。

Logic / Scenes / Battle 预留跨语言 gRPC 接口（Python / Go 活动服接入）。

典型链路：

```text
Client -> HAProxy -> Gateway/Login -> Transport(Local/Remote) -> Logic/Scenes/Global/Battle -> DBAgent -> MySQL/Redis
                                      ↑ TCP→Router Cluster（配置分发 + 路由转发 + 路由表）
```

### 启动模型

所有业务节点（Gateway / Login / Logic / Scenes / Global / DBAgent / Battle）共享同一启动骨架：加载本地配置 → 初始化日志 + Redis → 向 Router Cluster 拉取远程配置（serverconf / redisconf / mysqlconf / oauthconf）→ 构造 Service（继承 INode，内部创建 CompositeTransport + MessageHandler）→ 启动 Boost.Asio 事件循环。

**Router Cluster（Router × N）** 是独立的数据面控制平面集群，不继承 INode：负责服务注册发现、连接心跳、跨 Region 消息路由转发、故障转移、Region 拓扑管理，同时承担远程配置分发和路由表配置下发。

### 核心能力

- **CompositeTransport + INode 中介者**：业务层通过统一接口与底层通信完全解耦，同 region 走 SHM 直连（LocalTransport），跨 region 走 TCP → Router（RemoteTransport），由 IsSameRegion 策略在运行时透明路由
- **MessageHandler 7 步流水线**：OnBefore 钩子 → RequestCorrelator 匹配 → Protobuf 反序列化 → Interceptor 拦截 → cmd 注册表回调 → OnAfter 钩子
- **战斗引擎**：BattleEngine / BattleInstance / BattleRule / SettleManager / StatsManager 完整战斗生命周期
- **WebSocket 接入**：Gateway 同时支持 TCP 和 WebSocket 客户端
- **OAuth 2.0 认证**：Google / Facebook / Apple 三大提供商（授权码模式 + JWT 验证 + JWKS 验签）
- **Kubernetes 部署**：`k8s/` 目录提供 base + overlays dev/prod 的 Kustomize 配置

更完整的架构细节见 [docs/architecture/architecture_design.md](docs/architecture/architecture_design.md)。

## 技术栈

| 层级 | 技术 |
|------|------|
| 语言标准 | C++17 |
| 构建系统 | CMake |
| 异步 IO | Boost.Asio |
| 消息协议 | Protobuf |
| 跨语言 RPC | gRPC（启动配置分发 + Logic/Scenes/Battle 预留跨语言接口） |
| 进程间通信 | Boost.Interprocess / 共享内存消息队列（ShmQueueRpc） |
| 缓存与配置 | Redis / redis-plus-plus |
| 持久化 | MySQL 8.0 / mysql-connector-cpp |
| 日志 | Google glog |
| 容器化 | Docker / Docker Compose |
| 负载均衡 | HAProxy |
| 第三方认证 | OAuth 2.0（Google / Facebook / Apple） |
| 编排 | Kubernetes + Kustomize（k8s/base + overlays dev/prod） |

## 快速开始

### 本地构建

```bash
mkdir -p build
cd build
cmake ..
cmake --build .
```

Release 构建：

```bash
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

### Docker 启停

开发环境（直接调用 Compose）：

```bash
cd docker
bash docker-compose-up.sh
bash docker-compose-down.sh
```

生产环境（Pull GHCR 镜像）：

```bash
cd docker
NIMBUS_IMAGE=ghcr.io/<org>/nimbuscluster:<tag> sh docker-compose-prod-up.sh
sh docker-compose-prod-down.sh
```

部署环境、依赖安装、端口和脚本关系见 [docs/deployment/deployment.md](docs/deployment/deployment.md)。

## 文档索引

### 阅读路径建议

- **第一次接触本项目**：先读本文（README），再读 [架构设计](docs/architecture/architecture_design.md)
- **准备搭本机开发环境**：读 [部署指南](docs/deployment/deployment.md)，以及 [Ubuntu Docker 安装](docs/deployment/ubuntu_docker_setup.md)
- **准备部署到生产或 Kubernetes**：先读 [部署指南](docs/deployment/deployment.md)，再读 [Kubernetes 部署](docs/deployment/kubernetes.md)
- **了解 CI/CD 流水线**：读 [GitLab CI/CD](docs/devops/gitlab-ci-cd.md)

### 架构设计

| 文档 | 重点 |
|------|------|
| [architecture/architecture_design.md](docs/architecture/architecture_design.md) | 服务节点职责、CompositeTransport 通信模型、MessageHandler 7 步流水线、核心业务流程、存储设计、关键设计模式 |

### 部署运维

| 文档 | 重点 |
|------|------|
| [deployment/deployment.md](docs/deployment/deployment.md) | 本地构建、Docker Compose 启停（开发 + 生产）、端口与配置、服务启动顺序 |
| [deployment/kubernetes.md](docs/deployment/kubernetes.md) | Kubernetes + Kustomize 部署：base 与 overlays dev/prod 结构、部署/销毁脚本 |
| [deployment/ubuntu_docker_setup.md](docs/deployment/ubuntu_docker_setup.md) | Ubuntu 上 Docker / Docker Compose / 内核配置（shm_size、core_pattern、ipcmk）一次性安装 |

### 工程流程

| 文档 | 重点 |
|------|------|
| [devops/gitlab-ci-cd.md](docs/devops/gitlab-ci-cd.md) | GitLab CI 4-stage 流水线：build → test → image → deploy |