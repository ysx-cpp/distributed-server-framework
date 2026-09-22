# Kubernetes 部署

本文档描述分布式游戏服务器框架的 Kubernetes 部署方案。项目提供 `k8s/` 目录的 **Kustomize** 配置，支持 **k3s** 轻量集群单节点部署。`k3s` 即本地模拟的生产环境。

## 目录

1. [架构与布局](#1-架构与布局)
2. [快速开始](#2-快速开始)
3. [base 目录详解](#3-base-目录详解)
4. [部署脚本](#4-部署脚本)
5. [生产化建议](#5-生产化建议)

---

## 1. 架构与布局

```text
k8s/
├── base/
│   ├── namespace.yaml              # namespace: nimbus
│   ├── configmap.yaml              # nimbus-config（仅 battle 的 wait-redis/wait-mysql init 容器消费）
│   ├── kustomization.yaml          # 聚合全部 base 资源
│   ├── image-tag.env               # IMAGE_REF（CI 写入的唯一镜像坐标）
│   ├── redis-cluster/              # 独立 Redis Cluster
│   ├── mysql-cluster/              # 独立 MySQL Cluster
│   ├── clustermanager/             # CM 控制面 ×2（clustermanager-1 / -2）
│   ├── etcd/                       # etcd StatefulSet ×3（etcd 发现后端）
│   ├── global/                     # Global ×1（region 3，含 Router 进程）
│   ├── login/                      # Login ×1（region 1，含 Router 进程）
│   ├── server/                     # Server ×1（region 2，含 Router 进程）
│   ├── battle/                     # Battle ×1（region 4，含 Router 进程）
│   ├── payment/                    # Payment ×1（region 6，含 Router 进程）
│   └── haproxy/                    # HAProxy 统一入口（NodePort 30050-30052）
├── addons/
│   └── dashboard.yaml              # 可选：k3s dashboard
├── scripts/
│   ├── deploy.sh                   # CD 部署入口（apply + 切镜像 + 滚动等待）
│   ├── nimbus-bootguard.sh/.service # 开机拉起（systemd）
│   ├── k3s-dashboard-proxy.service  # dashboard 代理
│   └── port-forward-db.sh           # DB 开发端口转发
├── setup-k3s.sh                    # 一键安装 k3s（systemd 服务）
├── k8s-deploy.sh                   # 部署入口（apply k8s/base → nimbus）
├── k8s-destroy.sh                  # 销毁入口
└── K3S-TUTORIAL.md                 # K3s 环境入门
```

### 1.1 Namespace

所有资源位于 `namespace: nimbus`（`kustomization.yaml` 顶部声明）。

### 1.2 ConfigMap：nimbus-config

`base/configmap.yaml` 定义 DB 连接参数。**仅 Battle 的 `wait-redis` / `wait-mysql` init 容器消费这些键**；业务进程的 Redis / MySQL / Router 地址全部来自 `release/config/*/*.pbtxt`（拓扑配置）。

| Key | Value | 说明 |
|-----|-------|------|
| REDIS_HOST / REDIS_PORT | `redis-master` / `6379` | 独立 Redis |
| MYSQL_HOST / MYSQL_PORT | `mysql-master` / `3306` | 独立 MySQL |
| MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE / MYSQL_ROOT_PASSWORD | `nimbus_*` | MySQL 账号与库 |

### 1.3 单容器 = 单 region 模型

每个业务 Pod 通过 `start_<svc>.sh` 在**同一进程组**内先拉起本 region 的 Router，再拉起业务进程；Router 创建按 region 命名的 SHM 路由表，业务进程通过 SHM 读取路由。Pod 内 `/dev/shm` 挂载 `emptyDir { medium: Memory }` 提供共享内存。**没有独立的 router / overlays 资源**——Router 是各业务 Pod 内进程，overlay 已并入 base（k3s 即生产模拟）。

---

## 2. 快速开始

### 2.1 安装 k3s

```bash
cd k8s
sudo bash setup-k3s.sh
```

脚本会：写入 `/etc/systemd/system/k3s.service`（`--disable=traefik`，kubeconfig mode 644）→ `systemctl enable --now k3s` → 等待节点 Ready → 拷贝 kubeconfig 到 `~/.kube/config`。

### 2.2 部署

```bash
bash k8s-deploy.sh
```

等价于：

```bash
kubectl apply -k k8s/base
kubectl -n nimbus wait pods --all --for=condition=Ready --timeout=60s
kubectl -n nimbus get pods -o wide
```

### 2.3 CD 部署（生成配置 + 切镜像 + 滚动）

```bash
./k8s/scripts/deploy.sh                  # 完整流程
SKIP_IMAGE=1 ./k8s/scripts/deploy.sh     # 只 apply，不动镜像
NAMESPACE=nimbus ./k8s/scripts/deploy.sh # 覆盖命名空间
```

`deploy.sh` 职责边界：**只部署，不构建**。构建/打包/推送由 `scripts/local_ci.sh` 完成，两者唯一交接物是不可变镜像坐标 `k8s/base/image-tag.env`（`IMAGE_REF`）。流程：`gen_release_config.sh` 生成 `release/config` → `kubectl apply -k k8s/base` → 校验镜像已推送后把 7 个业务 Deployment（`clustermanager-1` `clustermanager-2` `login` `global` `server` `battle` `payment`）切到该 tag → 逐个 `rollout status` 等待完成。

### 2.4 销毁

```bash
bash k8s-destroy.sh
```

等价于 `kubectl delete -k k8s/base --ignore-not-found=true`。

---

## 3. base 目录详解

所有业务 Deployment 使用 `127.0.0.1:5500/nimbuscluster:<tag>` 本地 registry 镜像（`image-tag.env` 提供 tag），`workingDir: /data/NimbusCluster`，`nimbus-data` hostPath 挂载源码/产物，`shm`（`emptyDir` medium: Memory）挂载 `/dev/shm`。

### 3.1 server.yaml

单容器 Deployment，`command: start_server.sh`；`readinessProbe` 探测 `8001`。其 Service `server-1` 暴露各进程端口：

| Service 端口 | 用途 |
|------|------|
| 8001 / 8002 | Gateway（玩家 TCP） |
| 8003 / 8004 | Gateway（WebSocket） |
| 7103 | Router |
| 10001 / 10002 | DBAgent |
| 11001 / 11002 | Logic |
| 12001 / 12002 | Scenes |

### 3.2 battle.yaml / global.yaml / login.yaml / payment.yaml

结构对称：各自独立 Deployment + Service，均含 Router 端口与本 region 业务端口，`/dev/shm` 提供 SHM。Battle/Payment 额外挂 `core-dumps`：

| 资源 | command | Service 端口 |
|------|---------|------|
| battle | `start_battle.sh`（init: `wait-redis` + `wait-mysql`） | `15001`/`15002`（battle）+ `7105`（router） |
| global | `start_global.sh` | `13001`/`13002`（global）+ `7104`（router） |
| login | `start_login.sh` | `9001`/`9002`（login）+ `7102`（router） |
| payment | `start_payment.sh` | `16001`（payment）+ `7106`（router） |

readinessProbe：global 用 `grep -q ":32C9" /proc/net/tcp6`，battle 用 `:3A99`。

### 3.3 etcd.yaml（etcd 发现后端）

`StatefulSet` ×3（`podManagementPolicy: Parallel`），类型为 headless `Service etcd-headless`。`quay.io/coreos/etcd:v3.5.18`，`--initial-cluster=etcd-0/1/2.etcd-headless.<ns>.svc:2380`，client 2379 / peer 2380，带健康探针与 1Gi PVC。业务以 `discovery_cfg.endpoints`（如 `etcd-0.etcd-headless.nimbus.svc:2379`）接入。

### 3.4 clustermanager.yaml（CM 控制面）

两个 Deployment：`clustermanager-1`（`start_clustermanager1.sh`，端口 7001/7002）、`clustermanager-2`（`start_clustermanager2.sh`，端口 7003/7004），readinessProbe 探测对应首发端口。etcd 模式下仅保留兜底。

### 3.5 haproxy.yaml（统一入口）

`ConfigMap haproxy-config` 内嵌 `haproxy.cfg` + Deployment（`haproxy:2.9`）+ NodePort Service。配置与 `docker/container/haproxy/config/haproxy.cfg` 逐行一致：容器内 5000（gate TCP→gateway 8001/8002）、5001（login HTTP→9001/9002）、5002（WebSocket→8003/8004）。NodePort 暴露：

| NodePort | 容器内 | 用途 |
|---------|--------|------|
| 30050 | 5000 | 客户端 TCP 长连接 |
| 30051 | 5001 | HTTP 登录 |
| 30052 | 5002 | WebSocket |

### 3.6 payment-public.yaml（公网回调）

NodePort `30061` → payment `16001`，`externalTrafficPolicy: Local`，与 Compose 的 `30061:16001` 一致；单独建 Service 避免 NodePort 类型把 7106（Router）也自动分配外部端口。

---

## 4. 部署脚本

| 脚本 | 用途 |
|------|------|
| `setup-k3s.sh` | 一键安装 k3s（systemd，`--disable=traefik`） |
| `k8s-deploy.sh` | 部署：`kubectl apply -k k8s/base` + 等待 Ready |
| `k8s-destroy.sh` | 销毁：`kubectl delete -k k8s/base --ignore-not-found=true` |
| `scripts/deploy.sh` | CD 部署：生成配置 + apply + 切镜像 + 滚动等待 |
| `scripts/port-forward-db.sh` | 开发时端口转发 DB |
| `scripts/nimbus-bootguard.sh` (systemd) | 节点重启后自动拉起 |
| `addons/dashboard.yaml` + `scripts/k3s-dashboard-proxy.service` | 可选 dashboard |

## 5. 生产化建议

- 业务进程的 Redis / MySQL / Router 地址均来自拓扑配置而非 ConfigMap，生产只需按拓扑下发正确的 `release/config/*/*.pbtxt`。
- 各业务 Pod `/dev/shm` 为 `emptyDir (Memory)`，容量即 Pod 内存上限内的一部分；SHM 路由表为按 region 命名段，Pod 独享，无需 `hostIPC`。
- Router 是各业务 Pod 内进程，无独立 Pod；跨 Pod 调用统一走 Service DNS 短名（HAProxy 后端即为此）。
- 对外仅 `haproxy`（30050-30052）与 `payment-public`（30061）需要暴露；其余 Service 仅集群内部访问。
- 单节点 k3s 即本地生产模拟，无需 dev/prod 两套 overlay；需要独立集群时直接复用 `k8s/base`。