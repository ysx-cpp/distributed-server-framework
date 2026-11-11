# 分布式游戏服务器框架

分布式游戏服务器框架是一个基于 C++ 的分布式游戏服务器集群框架。项目采用进程化微服务架构，将玩家接入、登录认证、消息路由、业务逻辑、场景管理、战斗协调、战斗引擎与数据库访问拆分为独立节点，便于按服务职责扩展和部署。

![架构全景图](architecture/panoramic_architecture_diagram.png)

## 架构设计概述

### 服务节点

| 节点 | 目录 | 主要职责 |
|------|------|----------|
| ClusterManager | `src/clustermanager/` | **控制面（CM 模式）**：服务注册、连接心跳、Redis 路由表持久化、观察者通知 Router 更新 SHM 路由表；`discovery.mode=etcd` 时被跳过，发现由 etcd 承担 |
| Router | `src/router/` | **转发面 + SHM 路由表维护**：按 `discovery.mode` 双模式——etcd（EtcdRouteInformer 监听 etcd → RouteReconciler 拓扑×在线实例展开 → 写 SHM）或 CM（RouterObserver 收 CM 通知）；跨 Region TCP 转发，同 Region SHM 兜底；本容器业务进程只读该 SHM 路由表 |
| Gateway | `src/gateway/` | 玩家 TCP / WebSocket 长连接、心跳检测、uid↔session 连接映射、CS 下行消息拦截器 |
| Login | `src/login/` | HTTP 注册/登录、OAuth 2.0 认证（Google/Facebook/Apple）、会话与 token 创建 |
| Logic | `src/logic/` | 会话验证、玩家在线数据管理、核心业务逻辑、GM 命令、OnBefore 中打开用户共享内存 |
| Scenes | `src/scenes/` | 场景、地图、房间/大厅相关状态与同步逻辑、gRPC 预留跨语言接口 |
| Global | `src/global/` | **全局协调（Leader-Follower）**：在线统计、排行榜、全服广播、战斗匹配队列、Battle 实例分配；Leader 由 `RouteLookupLeader(GLOBAL)` 取 SHM 路由表中 `instance_id` 最小健康实例确定 |
| DBAgent | `src/dbagent/` | MySQL 统一访问入口、账号/用户/第三方账号读写、动态建表（基于 protobuf field options） |
| Battle | `src/battle/` | 战斗引擎实例：BattleInstance + BattleRule、结算（SettleManager）、战报（StatsManager）、battlerpc.proto |
| Payment | `src/payment/` | **支付服务**：订单状态机（OrderManager）、渠道注册（Google/Paddle/XorPay/Mock）、订单存储（RedisOrderStore）、发货通知到 Logic、HTTP 回调入口 |

### 通信与数据流

客户端登录后通过 `Gateway` 建立 TCP 或 WebSocket 长连接，业务消息经 **CompositeTransport** 决策后走 LocalTransport（同 region，iceoryx2 共享内存 Service 直连目标进程）或 RemoteTransport（跨 region，TCP → Router 转发）。各服务启动时从本地配置（`localconf.pbtxt`）加载本机信息与 `topology.pbtxt` 全集群拓扑（ConfigManage 合并），并依据 `discovery.mode` 完成服务注册：`etcd` 模式经 `EtcdRegistrar`（lease 自注册）上报在线实例，Router 经 `EtcdRouteInformer` 维护 SHM 路由表；`cm` 模式经 CM 注册并通知 Router。`Logic` 负责会话与业务处理，需要持久化时通过 `DBAgent` 访问 MySQL，并通过 Redis 缓存会话、账号索引。`Global` 作为全局协调层处理匹配、排行榜与全服广播，`Battle` 作为战斗引擎实例执行具体战斗逻辑，`Payment` 负责订单支付与发货触发。

Logic / Scenes / Battle 预留跨语言 gRPC 接口（Python / Go 活动服接入）。

典型链路：

```text
Client -> HAProxy -> Gateway/Login/Payment -> Transport(Local/Remote) -> Logic/Scenes/Global/Battle -> DBAgent -> MySQL/Redis
                                      ↑ Router 转发面（跨 Region TCP；同 Region SHM）
```

### 启动模型

所有业务节点（Gateway / Login / Logic / Scenes / Global / DBAgent / Battle / Payment）共享同一启动骨架：加载本地配置（`localconf.pbtxt` + `topology.pbtxt`）→ 初始化日志 + Redis → `InitCMTransport()` 完成服务注册（`discovery.mode=etcd` 时经 `EtcdRegistrar` 自注册；`cm` 时连 CM 发注册包）→ 打开本机 Router 维护的 SHM 路由表 → 构造 Service（继承 INode，内部创建 CompositeTransport + MessageHandler）→ 启动 Boost.Asio 事件循环。

**Router（每 Region ×1，region=0 除外）** 是转发面 + 路由表维护节点，不参与服务注册/心跳（由 etcd 或 CM 承担）：按 `discovery.mode` 从 etcd 或 CM 获取实例集合，用 `RouteReconciler` 将「静态拓扑 × 在线实例」展开为路由条目写入本机 SHM 路由表，本容器业务进程只读。

### 核心能力

- **CompositeTransport + INode 中介者**：业务层通过统一接口与底层通信完全解耦，同 region 走 SHM 直连（LocalTransport），跨 region 走 TCP → Router（RemoteTransport），由 Region 策略在运行时透明路由
- **etcd 服务发现（双模式门控）**：`discovery.mode=etcd` 时业务实例 `EtcdRegistrar` lease 自注册、Router `EtcdRouteInformer` 增量 reconcile 写 SHM；`cm` 模式走 CM + Redis + RouterObserver 观察者通知（默认兼容路径）
- **MessageHandler 6 步流水线**：pkg 空检查 → request_id 回包匹配 → cmd 查表 → Protobuf 反序列化 → OnBefore 钩子 → 业务 handler → OnAfter 钩子
- **战斗引擎**：BattleEngine / BattleInstance / BattleRule / SettleManager / StatsManager 完整战斗生命周期
- **支付服务**：OrderManager 订单状态机 + ProviderRegistry 渠道路由（Google/Paddle/XorPay/Mock）+ RedisOrderStore 存储 + 发货通知 Logic
- **WebSocket 接入**：Gateway 同时支持 TCP 和 WebSocket 客户端
- **OAuth 2.0 认证**：Google / Facebook / Apple 三大提供商（授权码模式 + JWT 验证 + JWKS 验签）
- **容器化部署**：Docker Compose 编排 + Kubernetes（Kustomize）部署，etcd 集群作为发现后端

更完整的架构细节见 [architecture/architecture_design.md](architecture/architecture_design.md)。

## 技术栈

| 层级 | 技术 |
|------|------|
| 语言标准 | C++17 |
| 构建系统 | CMake |
| 异步 IO | Boost.Asio |
| 消息协议 | Protobuf |
| 跨语言 RPC | gRPC（Logic / Scenes / Battle / Global 预留跨语言接口） |
| 进程间通信 | iceoryx2 共享内存 IPC（Publish-Subscribe / Request-Response） |
| 服务发现 | etcd（EtcdClient + EtcdRegistrar + EtcdRouteInformer，`discovery.mode=etcd`）/ ClusterManager（`cm` 模式）双实现 |
| 配置 | 本地 `localconf.pbtxt` + 全集群 `topology.pbtxt`（ConfigManage 合并） |
| 缓存 | Redis / redis-plus-plus |
| 持久化 | MySQL 8.0 / mysql-connector-cpp |
| 日志 | Google glog |
| 容器化 | Docker / Docker Compose（etcd 集群作发现后端） |
| 负载均衡 | HAProxy |
| 第三方认证 | OAuth 2.0（Google / Facebook / Apple） |
| 支付渠道 | Google Play / Paddle / XorPay / Mock（ProviderRegistry） |
| 编排 | Kubernetes + Kustomize（`k8s/base` + `k8s/scripts`） |

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

部署环境、依赖安装、端口和脚本关系见 [deployment/deployment.md](deployment/deployment.md)。

## 文档索引

### 阅读路径建议

- **第一次接触本项目**：先读本文（README），再读 [架构设计](architecture/architecture_design.md)
- **准备搭本机开发环境**：读 [部署指南](deployment/deployment.md)，以及 [Ubuntu Docker 安装](deployment/ubuntu_docker_setup.md)
- **准备部署到生产或 Kubernetes**：先读 [部署指南](deployment/deployment.md)，再读 [Kubernetes 部署](deployment/kubernetes.md)
- **了解 CI/CD 流水线**：读 [GitLab CI/CD](devops/gitlab-ci-cd.md)

### 架构设计

| 文档 | 重点 |
|------|------|
| [architecture/architecture_design.md](architecture/architecture_design.md) | 服务节点职责、通信与发现、生命周期、部署模型、节点模型、生命周期、通信契约和 iceoryx2 数据面 |

### 部署运维

| 文档 | 重点 |
|------|------|
| [deployment/deployment.md](deployment/deployment.md) | 本地构建、Docker Compose 启停（开发 + 生产）、端口与配置、服务启动顺序 |
| [deployment/kubernetes.md](deployment/kubernetes.md) | Kubernetes + Kustomize 部署：base 与 overlays dev/prod 结构、部署/销毁脚本 |
| [deployment/ubuntu_docker_setup.md](deployment/ubuntu_docker_setup.md) | Ubuntu 上 Docker / Docker Compose / 内核配置（shm_size、core_pattern、ipcmk）一次性安装 |

### 工程流程

| 文档 | 重点 |
|------|------|
| [devops/gitlab-ci-cd.md](devops/gitlab-ci-cd.md) | GitLab CI 4-stage 流水线：build → test → image → deploy |




