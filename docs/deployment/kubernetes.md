# Kubernetes 部署

本文档描述分布式游戏服务器框架的 Kubernetes 部署方案。项目提供 `k8s/` 目录的 **Kustomize** 配置，支持 **k3s** 轻量集群单节点快速部署。部署脚本：`k8s-deploy.sh` / `k8s-destroy.sh` / `setup-k3s.sh`。

## 目录

1. [架构与布局](#1-架构与布局)
2. [快速开始](#2-快速开始)
3. [base 目录详解](#3-base-目录详解)
4. [overlays（dev / prod）](#4-overlaysdev--prod)
5. [部署脚本](#5-部署脚本)
6. [生产化建议](#6-生产化建议)

---

## 1. 架构与布局

```text
k8s/
├── base/
│   ├── namespace.yaml              # namespace: dgsf
│   ├── configmap.yaml              # dgsf-config（DB + Router 连接参数）
│   ├── kustomization.yaml          # 聚合所有 base 资源
│   ├── router/
│   │   └── router.yaml             # **Router Cluster** Deployment + Service
│   ├── global/
│   │   └── global.yaml             # **Global（战斗服）顺位集群** Deployment + Service
│   ├── login/
│   │   └── login.yaml              # Login Deployment + Service
│   ├── server/
│   │   └── server.yaml             # Server Deployment + Service（**内嵌 Redis + MySQL initContainers**）
│   ├── battle/
│   │   └── battle.yaml             # Battle Deployment + Service（hostIPC + wait-redis/wait-mysql）
│   ├── redis-cluster/
│   │   └── redis-cluster.yaml      # 独立 Redis Cluster
│   └── mysql-cluster/
│       └── mysql-cluster.yaml      # 独立 MySQL Cluster
├── overlays/
│   ├── dev/
│   │   ├── kustomization.yaml
│   │   └── patch.yaml              # 覆盖 replicas、清空 nodeSelector（本地单节点）
│   └── prod/
│       └── kustomization.yaml      # 引用 base（生产通过 CI/CD 独立 overlay）
├── setup-k3s.sh                    # 一键安装 k3s（systemd 服务）
├── k8s-deploy.sh [dev|prod]        # 部署入口
└── k8s-destroy.sh [dev|prod]       # 销毁入口
```

### 1.1 Namespace

所有资源位于 `namespace: dgsf`。

### 1.2 ConfigMap：dgsf-config

`base/configmap.yaml` 定义所有服务共享的连接参数：

| Key | Value | 说明 |
|-----|-------|------|
| ROUTER_HOST | `router-service.dgsf.svc.cluster.local` | K8s DNS Service FQDN |
| ROUTER_PEER_HOST | 同上 | Router 间 peer 通信 |
| REDIS_HOST | `redis-master.dgsf.svc.cluster.local` | 独立 Redis（server.yaml 内嵌 Redis 时 localhost 覆盖） |
| REDIS_PORT | `6379` | — |
| MYSQL_HOST | `mysql-master.dgsf.svc.cluster.local` | 独立 MySQL（server.yaml 内嵌 MySQL 时 localhost 覆盖） |
| MYSQL_PORT | `3306` | — |
| MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE / MYSQL_ROOT_PASSWORD | `dgsf_*` | — |

---

## 2. 快速开始

### 2.1 安装 k3s

```bash
cd k8s
sudo bash setup-k3s.sh
```

脚本会：
1. 写入 `/etc/systemd/system/k3s.service`（`--disable=traefik`，写入 kubeconfig mode 644）
2. `systemctl enable --now k3s`
3. 等待 30s，检查节点状态
4. 拷贝 kubeconfig 到 `~/.kube/config`

### 2.2 部署 dev 环境

```bash
bash k8s-deploy.sh dev
```

等价于：

```bash
kubectl apply -k k8s/overlays/dev
kubectl -n dgsf wait pods --all --for=condition=Ready --timeout=120s
kubectl -n dgsf get pods -o wide
```

### 2.3 部署 prod 环境

```bash
bash k8s-deploy.sh prod
```

prod overlay 当前直接引用 base，未做额外覆盖。

### 2.4 销毁

```bash
bash k8s-destroy.sh dev
bash k8s-destroy.sh prod
```

等价于 `kubectl delete -k ... --ignore-not-found=true`。

---

## 3. base 目录详解

### 3.1 server.yaml（server Deployment）

这是最复杂的资源：

```yaml
spec:
  hostIPC: true                        # 关键：共享 IPC namespace，同 Pod 内容访 SHM
  nodeSelector:
    dgsf-role: server                # 目标节点标签（dev patch 清空）
  initContainers:
    - wait-redis                       # init 阶段等待内嵌 Redis 就绪
    - wait-mysql                       # init 阶段等待内嵌 MySQL 就绪
  containers:
    - name: server                     # 主容器：执行 start_server.sh
      command: ["/data/DGSF/release/script/start_server.sh"]
      image: ghcr.io/dgsf/server:latest
      env:
        - ROUTER_HOST: configMapKeyRef → ROUTER_HOST
        - REDIS_HOST: localhost        # 指向同 Pod 的 redis sidecar
        - MYSQL_HOST: localhost        # 指向同 Pod 的 mysql sidecar
      volumes:
        - dgsf-data: hostPath /data/DGSF   # 二进制 + config 挂载
        - core-dumps: hostPath /data/core_dumps       # 内核转储
        - redis-data: emptyDir
        - mysql-data: emptyDir
    - name: redis                      # sidecar Redis（开发内嵌模式）
      livenessProbe: redis-cli ping
      readinessProbe: redis-cli ping
    - name: mysql                      # sidecar MySQL（开发内嵌模式）
      livenessProbe: mysqladmin ping -h localhost
      readinessProbe: mysqladmin ping -h localhost
```

**关键点**：`hostIPC: true` 让 Server 主进程 + sidecar 共享同一 IPC namespace，保证 SHM 文件（通过 `ipc::shm::MpscDescriptorRing` 创建）在 Pod 内各容器间可见。Docker Compose 部署依赖同一容器的 `shm_size`，K8s 通过 `hostIPC` 实现等价效果。

### 3.2 server.yaml（Server Service 端口）

| Port | 用途 |
|------|------|
| 8001 / 8002 | Gateway |
| 8003 / 8004 | Gateway WebSocket |
| 9001 / 9002 | Login |
| 10001 / 10002 | DBAgent |
| 11001 / 11002 | Logic |
| 12001 / 12002 | Scenes |
| 14001 / 14002 | Battle |

### 3.3 router.yaml / global.yaml / login.yaml

结构相似：各自独立 Deployment + Service，从 `dgsf-config` ConfigMap 注入连接参数。

### 3.4 battle.yaml

独立 Battle Deployment，与 server.yaml 设计对称：

```yaml
spec:
  hostIPC: true
  nodeSelector:
    dgsf-role: battle
  initContainers:
    - wait-redis
    - wait-mysql
  containers:
    - name: battle
      command: ["/data/DGSF/release/script/start_battle.sh"]
      env:
        - ROUTER_HOST / REDIS_HOST / MYSQL_HOST: configMapKeyRef
        - MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE: configMapKeyRef
      volumes:
        - dgsf-data: hostPath /data/DGSF
        - core-dumps: hostPath /data/core_dumps
```

Battle 进程内部通过 `start_battle.sh` 先拉起 dbagent×2，再拉起 battle×2，两者通过 SHM 通信，因此需要 `hostIPC: true`。

---

## 4. overlays（dev / prod）

### 4.1 dev overlay

`overlays/dev/patch.yaml` 覆盖两个资源：

```yaml
# router
spec.replicas: 1

# server
spec.replicas: 1
spec.template.spec.nodeSelector: {}   # 清空：本地单节点无标签
```

本地 k3s 单节点环境通常不打 `dgsf-role: server` 标签，patch 清空 nodeSelector 让调度器可以将 Pod 放到任意节点。

### 4.2 prod overlay

当前仅引用 base，未额外 override。生产部署通常需要覆盖：

- `spec.replicas`（放大副本数）
- `nodeSelector`（按 role 标签分节点）
- Ingress / LoadBalancer Service 暴露 HAProxy
- Resource limits 调优
- 独立 Redis/MySQL Cluster 而非 sidecar 内嵌模式

---

## 5. 部署脚本

### 5.1 setup-k3s.sh

一键部署 k3s 到当前主机。注意 `--disable=traefik` 避免额外网关组件。

```bash
sudo bash setup-k3s.sh
```

### 5.2 k8s-deploy.sh

```bash
bash k8s-deploy.sh dev   # 或 prod
```

执行 `kubectl apply -k k8s/overlays/<env>`，等待所有 Pod Ready（120s 超时），输出 Pod 状态。

### 5.3 k8s-destroy.sh

```bash
bash k8s-destroy.sh dev   # 或 prod
```

执行 `kubectl delete -k k8s/overlays/<env> --ignore-not-found=true`。

---

## 6. 生产化建议

### 6.1 sidecar 模式与独立集群

server.yaml 使用 sidecar 模式内嵌 Redis + MySQL（开发内嵌风格），同时 base 目录提供 `redis-cluster/` 和 `mysql-cluster/` 独立集群资源。建议：
- dev：沿用 sidecar（快速启动，不依赖外部存储）
- prod：使用独立 `redis-cluster/` + `mysql-cluster/`，server.yaml / battle.yaml 的 `REDIS_HOST` / `MYSQL_HOST` 通过 ConfigMap 切换为集群 Service FQDN

### 6.2 prod overlay 具体化

`overlays/prod/kustomization.yaml` 当前仅引用 base，建议补充完整的 prod patch：覆盖 replicas、nodeSelector（按 role 标签分节点）、resource limits、以及 sidecar → 独立集群的 ConfigMap key 切换。