# 分布式游戏服务器框架部署与环境说明

本文档集中记录分布式游戏服务器框架的本地构建、依赖准备、Docker Compose 启停和部署脚本关系。架构设计细节请见 [architecture_design.md](../architecture/architecture_design.md)。

## 1. 本地构建环境

### 1.1 基础要求

- CMake 3.15+
- C++17 编译器
- Boost.Asio / Boost.Interprocess
- Protobuf + gRPC（gRPC 仅用于跨语言接口预留；远程配置由 **Router Cluster** 统一分发，内部 RPC 走 SHM/TCP）
- glog
- hiredis + redis-plus-plus
- mysql-connector-cpp
- libcurl

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

仓库提供 `scripts/ci-local.sh` 用于在 Linux 环境模拟 CI 流程：

```bash
bash scripts/ci-local.sh
```

该脚本会执行：

1. 检查 Git 仓库状态
2. 初始化 Git 子模块
3. 安装构建依赖
4. 以 Release 模式配置 CMake
5. 编译项目（`cmake --build build -- -j$(nproc)`）
6. 运行 `ctest --output-on-failure -j$(nproc)`

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

## 3. Docker Compose 部署

### 3.1 启动全部服务（开发）

```bash
cd docker
bash docker-compose-up.sh
```

启动顺序由脚本控制：

1. Redis（`container/db/redis`）
2. MySQL（`container/db/mysql`）
3. **Router Cluster**（`router` ×4，控制平面 + 路由表中心）
4. **Global（战斗服，顺位集群）**（`global`，多 host 多 rank 顺位选举，匹配/队列/协调）
5. Login（`login`）
6. Server（`server`）
7. Battle（`battle`）
8. HAProxy（`container/haproxy`）

### 3.2 停止全部服务（开发）

```bash
cd docker
bash docker-compose-down.sh
```

### 3.3 生产部署（Pull 镜像）

生产环境不依赖本地编译产物，直接从 **GHCR** 拉取预构建 runtime 镜像：

```bash
cd docker
DGSF_IMAGE=ghcr.io/<org>/dgsf:<git_sha_or_tag> sh docker-compose-prod-up.sh
sh docker-compose-prod-down.sh
```

`docker-compose-prod-up.sh` 严格按以下顺序启动：

```text
Shared DB → Router Cluster → Global（战斗服） → Login → Server → Battle → HAProxy
```

每个步骤之间设置 3 秒间隔。**Router Cluster 必须最先就绪**，因为所有其他服务启动时要从 Redis 全量拉取 `router:servers` 路由表，同时 Router Cluster 也承担服务注册发现和路由表管理。

### 3.4 Compose 文件分布

| 模块 | Compose 文件 | 启动脚本 | 说明 |
|------|--------------|---------|------|
| Redis | `docker/container/db/redis/docker-compose.yml` | — | 生产独立 Redis |
| MySQL | `docker/container/db/mysql/docker-compose.yml` | — | 生产独立 MySQL |
| Router Cluster | `docker/router/docker-compose.yml` | `start_router1.sh` + `start_router2.sh` | Router ×4，控制平面 + 路由表中心 |
| Global（战斗服） | `docker/global/docker-compose.yml` | `start_global.sh` | 多 host + 多 rank 顺位集群 |
| Login | `docker/login/docker-compose.yml` | `start_login.sh` | Login ×2 |
| **Server** | `docker/server/docker-compose.yml` | `start_server.sh` | **DBAgent×2 + Gateway×2 + Scenes×2 + Logic×2**；内嵌 Redis/MySQL 用于开发 |
| **Battle** | `docker/battle/docker-compose.yml` | `start_battle.sh` | **DBAgent×2 + Battle×2**；独立容器 |
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

### 4.3 Server 容器端口映射（开发内嵌）

`docker/server/docker-compose.yml` 将以下端口暴露到宿主机：

| 宿主机端口 | 容器内端口 | 说明 |
|-----------|-----------|------|
| `8001` | — | Router 相关 |
| `8002` | — | Router 相关 |
| `8003` | — | Router 相关 |
| `8004` | — | Router 相关 |

### 4.4 Battle 容器

Battle 容器使用 `network_mode: host`，共享宿主机网络栈，不额外做端口映射。

## 5. 共享内存配置

| 容器 | shm_size |
|------|---------|
| server（docker/server） | `512m` |
| battle（docker/battle） | `256m` |

Linux 宿主机若需要手动创建 SHM 文件，可使用 `ipcmk`（参考 `scripts/check_shm.sh`）。

## 6. 配置文件

运行时配置位于 `config/`，各服务从 `localconf.pbtxt` 加载本地路径和 server_id：

| 文件 | 用途 |
|------|------|
| `localconf.pbtxt` | 本地进程配置（包含本实例 `server_id`、端口、日志目录） |

### 6.1 OAuth 配置说明

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

### 6.2 数据库初始化

数据库初始化 SQL 位于 `DB/my_database.sql`。核心表：

| 表 | 说明 |
|----|------|
| `user_accounts` | 主账号（username/email → password_hash + salt） |
| `user_profiles` | 玩家资料 |
| `oauth_accounts` | **新增** OAuth provider + provider_user_id → user_id 映射 |
| `user_heroes` / `user_items` | 游戏数据 |
| `battle_history` | 战报记录 |

```bash
mysql -u root -p < DB/my_database.sql
```

DBAgent 启动时会扫描 protobuf `field_options(dgsf.db_table)` 自动 `CREATE TABLE IF NOT EXISTS`，保证表结构最新。

## 7. Ubuntu Docker 安装

Ubuntu 22.04 的 Docker 安装步骤保留在 [ubuntu_docker_setup.md](ubuntu_docker_setup.md)。包含 Docker / Docker Compose / `ipcmk` SHM 工具、`core_pattern` 内核参数等一次性安装步骤。

## 8. Kubernetes 部署

项目同时提供 `k8s/` 目录的 Kustomize 配置，支持 k3s 轻量集群部署。详见 [kubernetes.md](kubernetes.md)。

## 9. 部署提示

- `docker/server/docker-compose.yml` 挂载路径默认指向 `../../../DGSF`，若仓库目录结构不同需要同步调整 volume mount。
- `docker/battle/docker-compose.yml` 使用 `network_mode: host`，共享宿主机网络。
- Battle 容器独立于 Server 容器部署，`docker-compose-prod-up.sh` 中它在 Server 之后、HAProxy 之前启动。