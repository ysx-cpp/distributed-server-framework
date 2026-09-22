# 分布式游戏服务器框架部署与环境说明

本文档集中记录分布式游戏服务器框架的本地构建、依赖准备、Docker Compose 启停和部署脚本关系。架构设计细节请见 [architecture_design.md](../architecture/architecture_design.md)。

## 1. 本地构建环境

### 1.1 基础要求

- CMake 3.15+
- C++17 编译器
- Boost.Asio / Boost.Interprocess
- Protobuf + gRPC（gRPC 仅用于跨语言接口预留；内部 RPC 走 SHM/TCP）
- glog
- hiredis + redis-plus-plus
- mysql-connector-cpp
- libcurl
- etcd client（etcd 发现模式，`discovery.mode=etcd`）

配置加载方式：各服务从本地 `localconf.pbtxt` 加载本机信息（含 `discovery.mode`），`serverconf.pbtxt` 本地覆盖 `topology.pbtxt` 全集群拓扑（ConfigManage 合并），不再经 Router Cluster 拉取远程配置。

Windows 环境下，预构建库位于 `external/winlib/`，头文件位于 `external/wininc/`。

### 1.2 Debug 构建

```bash
mkdir -p build
cd build
cmake ..
cmake --build .
```

默认构建类型为 Debug。构建产物通常输出到：

- `build/lib/`
- `build/bin/`

### 1.3 Release 构建

```bash
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

构建后通过 `cmake --install build --prefix release` 安装到 `release/` 目录，与生产 Docker runtime 镜像结构一致。

## 2. CI 本地验证

仓库提供 `scripts/local_ci.sh` 用于在 Linux 环境模拟 CI 流程：

```bash
bash scripts/local_ci.sh
```

该脚本会执行：

1. 在 build-server 容器内编译项目（`cmake --build build -- -j4`）
2. 将产物安装到 `release/` 目录（`cmake --install build`）
3. 校验必需二进制是否存在（`router` / `global` / `gateway` / `login` / `logic` / `dbagent` / `scenes`，共 7 个）
4. 基于 `release/` 构建 Docker 运行时镜像

支持 `--host`（宿主机构建）、`--no-image`（跳过镜像打包）等选项。

### 2.1 构建产物校验

Release 构建成功后，`release/bin/` 应包含以下可执行文件：

| 二进制 | 来源目录 |
|--------|---------|
| `router` | `src/router/` |
| `global` | `src/global/` |
| `gateway` | `src/gateway/` |
| `login` | `src/login/` |
| `logic` | `src/logic/` |
| `dbagent` | `src/dbagent/` |
| `scenes` | `src/scenes/` |
| `battle` | `src/battle/` |
| `clustermanager` | `src/clustermanager/` |
| `payment` | `src/payment/` |

## 3. Docker Compose 部署

### 3.1 启动全部服务（开发）

```bash
cd docker
bash docker-compose-up.sh
```

启动顺序由脚本控制：

1. Redis（`container/db/redis`）
2. MySQL（`container/db/mysql`）
3. **etcd ×3**（etcd 发现后端）
4. **ClusterManager**（CM 模式控制面；etcd 模式下仅保留）
5. **Global**（`global`，全局协调 + 匹配/队列，region 3）
6. Login（`login`，region 1）
7. Server（`server`，region 2）
8. Battle（`battle`，region 4）
9. **Payment**（`payment`，region 6，订单 HTTP 回调）
10. HAProxy（`container/haproxy`）

### 3.2 停止全部服务（开发）

```bash
cd docker
bash docker-compose-down.sh
```

### 3.3 生产部署（Pull 镜像）

生产环境不依赖本地编译产物，直接从 **GHCR** 拉取预构建 runtime 镜像：

```bash
cd docker
NIMBUS_IMAGE=ghcr.io/<org>/nimbuscluster:<git_sha_or_tag> sh docker-compose-prod-up.sh
sh docker-compose-prod-down.sh
```

`docker-compose-prod-up.sh` 严格按以下顺序启动：

```text
Shared DB（Redis → MySQL） → etcd → ClusterManager → Global → Login → Server → Battle → Payment → HAProxy
```

每个步骤之间设置 3 秒间隔。etcd 发现后端与 ClusterManager 必须在业务服务注册前就绪；Router 随各业务容器启动，先创建 SHM 路由表供同容器业务进程读取。

### 3.4 Compose 文件分布

| 模块 | Compose 文件 | 启动脚本 | 说明 |
|------|--------------|---------|------|
| Redis | `docker/container/db/redis/docker-compose.yml` | — | 生产独立 Redis（3 节点） |
| MySQL | `docker/container/db/mysql/docker-compose.yml` | — | 生产独立 MySQL |
| etcd | `docker/container/etcd/docker-compose.yml` | — | **etcd ×3**（`container/etcd`，etcd 发现后端，挂 `nimbus-net`，端点 `etcd-0/1/2:2379`） |
| ClusterManager | `docker/clustermanager/docker-compose.yml` | `start_clustermanager.sh` | CM ×4（CM 模式控制面；etcd 模式下仅保留） |
| Global | `docker/global/docker-compose.yml` | `start_global.sh` | Global ×2 + Router ×2（region 3） |
| Login | `docker/login/docker-compose.yml` | `start_login.sh` | Login ×2 + DBAgent ×2 + Router ×2（region 1） |
| **Server** | `docker/server/docker-compose.yml` | `start_server.sh` | **DBAgent×2 + Gateway×2 + Scenes×2 + Logic×2 + Router ×2**（region 2）；开发内嵌 Redis/MySQL |
| **Battle** | `docker/battle/docker-compose.yml` | `start_battle.sh` | **DBAgent×2 + Battle×2 + Router ×2**（region 4） |
| **Payment** | `docker/payment/docker-compose.yml` | `start_payment.sh` | **Payment×2 + Router ×2**（region 6，宿主映射 `30061:16001`） |
| HAProxy | `docker/container/haproxy/docker-compose.yml` | — | 客户端 TCP/HTTP 前置 |

### 3.5 Server 容器启动顺序约束

`release/script/start_server.sh:14` 明确指出：**dbagent 必须最先启动**，避免 SHM Pool 竞争。启动顺序：

```text
dbagent 0 & → sleep 0.5 → dbagent 1 & → sleep → gateway 0 & → gateway 1 & → scenes 0 & → scenes 1 & → logic 0 & → logic 1
```

Battle 容器（`start_battle.sh`）遵循同样顺序：先 dbagent ×2，再 battle ×2。

## 4. 默认容器与端口

### 4.1 基础设施端口

| 服务 | 容器名 | 宿主机端口 | 开发内嵌 vs 生产独立 |
|------|--------|------------|---------------------|
| Redis | `server-1-redis`（内嵌）/ 独立 `redis` | 开发内嵌 `16379` / 生产默认 `6379` | 开发内嵌，生产独立 |
| MySQL | `server-1-mysql`（内嵌）/ 独立 `mysql` | 开发内嵌 `13306` / 生产默认 `3306` | 开发内嵌，生产独立 |

### 4.2 对外入口

HAProxy 默认暴露：

| 入口 | 端口 | 协议 | 用途 |
|------|------|------|------|
| login | `5001` | HTTP | 登录、注册入口 |
| gate | `5000` | TCP | 客户端长连接入口（TCP + WebSocket） |

### 4.3 Server 容器端口（内部服务）

`docker/server/docker-compose.yml` 挂到共享网络 `nimbus-net`（容器 DNS = 容器名），`topology.pbtxt` 声明该 region 内部服务端口（供跨容器经 Router 转发）：

| 服务 | 容器内端口 | 说明 |
|-----------|-----------|------|
| gateway 0/1（TCP） | `8001` / `8002` | 玩家 TCP 长连接 |
| gateway 0/1（WebSocket） | `8003` / `8004` | 玩家 WS 长连接 |

### 4.4 Battle / 各业务容器

业务容器（server / battle / login / global / payment / clustermanager）一律使用共享外部网络 `nimbus-net` + `nimbus-data`（由 `docker-compose-up.sh` / `docker-compose-prod-up.sh` 创建），不通过宿主机暴露业务端口；对外仅 HAProxy 与 Payment（`30061:16001`）需要映射。各容器具有独立 IPC namespace，`NEVER use ipc: host`。

## 5. 共享内存配置

| 容器 | shm_size |
|------|---------|
| server（docker/server） | `512m` |
| battle（docker/battle） | `256m` |

Linux 宿主机若需要手动创建 SHM 文件，可使用 `ipcmk`（参考 `scripts/check_shm.sh`）。

## 6. 配置文件

运行时配置分层：各节点 `localconf.pbtxt` + 本地 `serverconf.pbtxt` 覆盖全集群共享的 `topology.pbtxt`（ConfigManage 合并加载）。

| 文件 | 用途 |
|------|------|
| `localconf.pbtxt` | 本地进程配置：本实例 `server_id`、`discovery.mode`（`etcd`/`cm`/空）、etcd endpoints/认证/TLS、日志目录、本 region CM 地址 |
| `topology.pbtxt` | 全集群共享拓扑：各 region 服务（router / login / dbagent / gateway / logic / scenes / global / battle / payment）的 region/type/index/host/port |
| `serverconf.pbtxt` | 本节点覆盖条目（`discovery.mode` 变更、本 region `127.0.0.1` 服务地址），ConfigManage 合并时覆盖 topology 远程条目 |

生产各业务容器以 `-v ../../config/<svc>:/data/NimbusCluster/release/config/<svc>` 挂载本容器配置目录。

### 6.1 服务发现配置说明

```protobuf
discovery_cfg {
  mode: "etcd"                     # "etcd" / "" / "cm"（空、cm = CM 模式）
  endpoints: ["etcd-0:2379",       # etcd 集群端点列表
              "etcd-1:2379",
              "etcd-2:2379"]
  user: ""                          # 认证（空 = 不认证）
  password: ""
  ca_file: ""  cert_file: ""  key_file: ""   # TLS（ca/cert/key 三文件齐全才启用）
  dial_timeout_ms: 3000             # 建连超时（0 → 默认 3000ms）
  request_timeout_ms: 5000          # 单次 RPC 超时（0 → 默认 5000ms）
  retry_max: 3                      # 可重试错误最大次数（0 → 默认 3）
  watch_prefix: "/nimbus/discovery/instances/"  # 实例注册监听前缀（空 → 上述默认）
  relist_interval_ms: 30000         # 周期性全量兜底周期（0 → 默认 30000ms）
  lease_ttl_sec: 10                 # 注册 lease 时长（0 → 默认 10s）
  keepalive_interval_ms: 3000       # keepalive 周期（0 → 默认 TTL/3）
}
```

`discovery.mode=etcd` 时业务实例经 `EtcdRegistrar` 自注册（LeaseGrant → PutIfAbsent → LeaseKeepAliveLoop，宕机 lease 过期自动摘除），Router 经 `EtcdRouteInformer` 维护 SHM 路由表；`cm`/空时业务连接 CM 注册，Router 经 `RouterObserver` 更新。实例注册 key 编码为 `<watch_prefix><region>/<server_type>/<index>`。

### 6.2 OAuth 配置说明

```protobuf
providers {
  provider: "google"
  client_id: "your-google-client-id.apps.googleusercontent.com"
  client_secret: "your-google-client-secret"
  auth_url: "https://accounts.google.com/o/oauth2/v2/auth"
  token_url: "https://oauth2.googleapis.com/token"
  user_info_url: "https://www.googleapis.com/oauth2/v2/userinfo"
  scopes: "openid email profile"
  jwks_url: "https://www.googleapis.com/oauth2/v3/certs"
  redirect_uri: "https://yourdomain.com/oauth/callback/google"
}
```

**支持的提供商**：Google（OpenID Connect）、Facebook（Graph API v19.0）、Apple（Sign in with Apple）。

### 6.3 数据库初始化

数据库初始化 SQL 位于 `DB/my_database.sql`。核心表：

| 表 | 说明 |
|----|------|
| `user_accounts` | 主账号（username/email → password_hash + salt） |
| `user_profiles` | 玩家资料 |
| `user_heroes` / `user_items` | 游戏数据 |
| `battle_history` | 战报记录 |
| `payment_orders` | 支付订单（渠道、金额、订单状态、回调数据） |

```bash
mysql -u root -p < DB/my_database.sql
```

DBAgent 启动时扫描 protobuf Descriptor，从 `GenerateCreateTableSQL` 自动生成 DDL，自动 `CREATE TABLE IF NOT EXISTS`（`CreateTable` 方法），保证表结构最新。

## 7. Ubuntu Docker 安装

Ubuntu 22.04 的 Docker 安装步骤保留在 [ubuntu_docker_setup.md](ubuntu_docker_setup.md)。包含 Docker / Docker Compose / `ipcmk` SHM 工具、`core_pattern` 内核参数等一次性安装步骤。

## 8. Kubernetes 部署

项目同时提供 `k8s/` 目录的 Kustomize 配置，支持 k3s 轻量集群部署。详见 [kubernetes.md](kubernetes.md)。

## 9. 部署提示

- 业务容器通过共享网络 `nimbus-net` / `nimbus-data` 互连，容器 DNS = 容器名（`topology.pbtxt` 的 `host` 字段），无 `network_mode: host`；各容器独立 IPC namespace。
- Payment 容器以 `-p "${PAYMENT_BIND:-127.0.0.1}:30061:16001"` 暴露公网回调入口，需要对外接收商家回调时设置 `PAYMENT_BIND=0.0.0.0`。
- Battle 容器独立于 Server 容器部署，`docker-compose-up.sh` 中它在 Server 之后、Payment/HAProxy 之前启动。
- 各容器通过 `-v ../../config/<svc>:...` 挂载本地配置，勿以宿主机源码覆盖运行时镜像（`docker-compose-prod-up.sh` 用镜像内二进制）。