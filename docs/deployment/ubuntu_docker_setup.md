# Ubuntu 环境一次性安装

本章覆盖 DGSF 开发/生产环境在 Ubuntu 22.04 上的前置依赖安装与内核参数配置，一次性完成即可。

---

## 1. Docker 与 Docker Compose 安装

### 1.1 卸载旧版本（如有）

```bash
sudo apt remove docker docker-engine docker.io containerd runc
```

### 1.2 添加 Docker 官方仓库

```bash
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

### 1.3 安装 Docker Engine

```bash
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### 1.4 免 sudo 运行（可选）

```bash
sudo usermod -aG docker $USER
```

执行后重新登录生效。

---

## 2. 共享内存（SHM）配置

DGSF 的同 Region 进程间通信基于 **SHM 消息队列**（`/dev/shm/dgsf/{server_id}_{role}`），对共享内存容量有较高要求。

### 2.1 调整 /dev/shm 大小

Docker 容器默认 `/dev/shm` 仅 **64MB**，不足以支撑多服务实例的 SHM 队列。在 `docker-compose.yml` 中为每个服务显式指定：

```yaml
services:
  logic:
    shm_size: "512m"
```

生产环境建议 **512MB ~ 2GB**，视同 Region 内服务实例数量与消息吞吐量调整。

### 2.2 宿主机 SHM 上限

若直接在宿主机运行（非容器），需确保 `/dev/shm` 容量充足：

```bash
# 查看当前大小
df -h /dev/shm

# 临时调整为 2GB（重启失效）
sudo mount -o remount,size=2G /dev/shm

# 永久生效：写入 /etc/fstab
tmpfs /dev/shm tmpfs defaults,size=2G 0 0
```

### 2.3 ipcmk 工具

`ipcmk` 用于手动创建共享内存段，常用于调试 SHM 通道是否正常：

```bash
sudo apt install util-linux
```

验证共享内存可用：

```bash
ipcmk -M 4096   # 创建 4KB 共享内存段
ipcs -m         # 查看当前共享内存段
```

---

## 3. Core Dump 配置

生产环境需要捕获崩溃现场，配置 `core_pattern` 将 core dump 输出到固定目录：

```bash
# 创建 core dump 目录
sudo mkdir -p /var/coredumps
sudo chmod 777 /var/coredumps

# 设置 core_pattern（临时，重启失效）
sudo sysctl -w kernel.core_pattern=/var/coredumps/core.%e.%p.%t

# 永久生效：写入 /etc/sysctl.d/99-coredump.conf
echo "kernel.core_pattern=/var/coredumps/core.%e.%p.%t" | sudo tee /etc/sysctl.d/99-coredump.conf
sudo sysctl -p /etc/sysctl.d/99-coredump.conf
```

Docker 容器内需额外在 `docker-compose.yml` 中传递宿主机配置：

```yaml
services:
  logic:
    volumes:
      - /var/coredumps:/var/coredumps
    ulimits:
      core: -1            # unlimited
```

---

## 4. 验证

```bash
docker --version
docker compose version
df -h /dev/shm | grep -v tmpfs || echo "SHM OK"
```

全部通过即可进入 [部署指南](deployment.md) 启动服务集群。