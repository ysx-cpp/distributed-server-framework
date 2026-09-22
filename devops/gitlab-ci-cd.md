# GitLab CI/CD 流水线

本文档描述分布式游戏服务器框架的 **GitLab CI/CD** 流水线配置。

GitLab CI 配置文件：仓库根目录 `.gitlab-ci.yml`。

## 目录

1. [流水线总览](#1-流水线总览)
2. [Stage: build](#2-stage-build)
3. [Stage: test](#3-stage-test)
4. [Stage: image](#4-stage-image)
5. [Stage: deploy](#5-stage-deploy)
6. [所需 CI/CD Variables](#6-所需-cicd-variables)
7. [Docker 构建分层](#7-docker-构建分层)

---

## 1. 流水线总览

```text
                push / tag
                     │
                     ▼
              ┌──────────────┐
              │    build     │  (Builder 镜像编译 Release + 产物归档)
              └──────┬───────┘
                     │ needs
              ┌──────▼───────┐
              │ unit-test    │  (ctest via builder 容器)
              └──────┬───────┘
              ┌──────▼───────┐
              │  lint-test   │  (TODO/FIXME 扫描)
              └──────┬───────┘
                     │ needs (build + unit-test + lint-test)
              ┌──────▼───────┐
              │ docker-image │  (Runtime 镜像推送 GitLab Registry)
              └──────┬───────┘
                     │ needs
              ┌──────▼───────┐
              │   deploy     │  (SSH → docker pull → docker-compose-prod-up.sh)
              └──────────────┘
```

| Stage | Job | 触发 | 核心动作 |
|-------|-----|------|---------|
| build | build | branch / tag | builder 镜像编译 Release → `cmake --install --prefix release` → 校验 8 个可执行文件 → artifacts 归档 |
| test | unit-test | branch / tag | `needs: build` → builder 容器内 `ctest --output-on-failure` |
| test | lint-test | branch / tag | Alpine grep 扫描 `src/` 下 TODO/FIXME/HACK |
| image | docker-image | branch / tag | `needs: build+unit-test+lint-test` → build runtime 镜像 → push GitLab Registry → 主分支自动打 `:latest` |
| deploy | deploy | 主分支 / tag（**manual**） | `needs: docker-image` → SSH → docker pull → docker-compose 滚动更新 |

所有 job 运行在 GitLab Runner（Docker executor，`docker:27` + `docker:27-dind`）。

---

## 2. Stage: build

编译源码并归档 Release 产物：

1. 从 `docker/builder/Dockerfile` 构建 builder 镜像（预装 C++17 / Boost / Protobuf / glog / redis-plus-plus / mysql-connector-cpp）
2. `docker run` builder 容器内执行 CMake Release 编译：`cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -- -j$(nproc) && cmake --install build --prefix release`
3. 校验 8 个可执行文件：`test -x release/bin/{router,global,gateway,login,logic,dbagent,scenes,battle}`
4. artifacts 归档 `release/` + `build/bin/` + `build/CTestTestfile.cmake`（保留 1 周，供下游 test/image stage 使用）

---

## 3. Stage: test

### 3.1 unit-test

`needs: build` 依赖 build 产物。用同一个 builder 容器在 `build/` 目录执行 `ctest --output-on-failure -j$(nproc)`。junit XML 报告写入 `build/test-results/`，通过 `artifacts.reports.junit` 上报 GitLab MR 页面。

### 3.2 lint-test

Alpine grep 扫描 `src/**/*.{cpp,h}` 下 TODO/FIXME/HACK 标记，仅告警不阻塞构建。

---

## 4. Stage: image

`needs: build + unit-test + lint-test`，依赖 build 产物和单元测试通过。

1. `docker login "$CI_REGISTRY"`（GitLab 内置 Registry，免配 token）
2. 从 `docker/runtime/Dockerfile` 构建 runtime 镜像（仅含 Release 二进制 + 运行时依赖，无编译器）
3. 推送到 `$CI_REGISTRY_IMAGE:$CI_COMMIT_SHORT_SHA`（SHA 标签可追溯）
4. **主分支**自动额外打 `:latest` 标签
5. **git tag** 额外打 tag 版本号（如 `v1.2.0`）

| 标签 | 生成条件 |
|------|---------|
| `$CI_COMMIT_SHORT_SHA` | 所有 push |
| `latest` | 主分支 push |
| `$CI_COMMIT_TAG` | git tag 触发 |

---

## 5. Stage: deploy

**手动触发**（`when: manual`），两种触发条件：
- 主分支 push 完成后，在 GitLab Pipeline 页面手动点击 deploy job 的 ▶ 按钮
- git tag push 后，手动触发部署（用于版本发布）

部署流程（SSH 到目标机执行）：

```bash
# 在目标机执行
cd "$DEPLOY_PATH"
echo "$CI_REGISTRY_PASSWORD" | docker login "$CI_REGISTRY" -u "$CI_REGISTRY_USER" --password-stdin

export DGSF_IMAGE="$IMAGE_TAG"
docker pull "$IMAGE_TAG"
sh docker/docker-compose-prod-down.sh || true
sh docker/docker-compose-prod-up.sh
```

`environment: production` 声明使 GitLab 追踪该环境的部署历史（GitLab UI → 部署 → Environments → production）。

### 5.1 远端部署前置条件

- 目标机已安装 Docker + Docker Compose
- 宿主机文件系统支持 `shm_size: 512m` / `256m`
- 网络可达 GitLab Registry（自建 GitLab 需开放 Registry 端口）
- 端口：5000（TCP）、5001（HTTP）、Redis/MySQL 对应端口开放

---

## 6. 所需 CI/CD Variables

在 GitLab 项目 Settings → CI/CD → Variables 中配置：

| Variable | 说明 | 使用位置 |
|----------|------|---------|
| `DEPLOY_HOST` | 目标机 IP 或域名 | deploy: SSH 连接 |
| `DEPLOY_PORT` | SSH 端口（默认 22） | deploy: SSH 连接 |
| `DEPLOY_USER` | SSH 用户名 | deploy: SSH 连接 |
| `DEPLOY_SSH_PRIVATE_KEY` | SSH 私钥（base64 或 PEM 内容） | deploy: ssh-agent |
| `DEPLOY_PATH` | 目标机项目根路径 | deploy: SSH 后工作目录 |

以下由 GitLab Runner 自动注入，无需手动配置：

| Variable | 来源 | 说明 |
|----------|------|------|
| `CI_REGISTRY` | GitLab 内置 | Registry 地址，如 `registry.gitlab.com` |
| `CI_REGISTRY_IMAGE` | GitLab 内置 | 当前项目镜像路径，如 `registry.gitlab.com/group/project` |
| `CI_REGISTRY_USER` | GitLab 内置 | Registry 登录用户名 |
| `CI_REGISTRY_PASSWORD` | GitLab 内置 | Registry 登录密码 |
| `CI_COMMIT_SHORT_SHA` | GitLab 内置 | commit SHA 前 8 位，作为镜像标签 |
| `CI_DEFAULT_BRANCH` | GitLab 内置 | 主分支名（main / master） |

---

## 7. Docker 构建分层

### 7.1 Builder 镜像（`docker/builder/Dockerfile`）

- 基于 Debian / Ubuntu 基础镜像
- 预装 C++17 编译工具链、Protobuf/gRPC、Boost、glog、redis-plus-plus、mysql-connector-cpp
- 仅用于 CI 中编译源码（build + unit-test stage 复用同一 builder 镜像加速）

### 7.2 Runtime 镜像（`docker/runtime/Dockerfile`）

- 基于精简运行时镜像（Ubuntu / distroless）
- 仅拷贝 `cmake --install --prefix release` 产物（bin + lib + config）
- **不再包含编译器或源码**，生产安全且镜像小
- 所有 Compose 文件（开发和生产）引用此 runtime 镜像：`${DGSF_IMAGE:-$CI_REGISTRY_IMAGE:latest}`

---

## 多平台同构

本项目同时维护 GitHub Actions 流水线（`.github/workflows/ci.yml` + `cd.yml`），两套配置完全同构：

| 对比项 | GitLab CI | GitHub Actions |
|--------|-----------|----------------|
| 配置文件 | `.gitlab-ci.yml` | `.github/workflows/ci.yml` + `cd.yml` |
| Registry | GitLab Container Registry（`$CI_REGISTRY`） | GitHub Container Registry（`ghcr.io`） |
| Deploy 触发 | deploy stage `when: manual` | `workflow_run` 监听 CI 完成 + `workflow_dispatch` 手动 |
| 核心变量 | `CI_COMMIT_SHORT_SHA` / `CI_REGISTRY_*` | `github.sha` / `GHCR_TOKEN` |

详见 [github-actions-ci-cd.md](github-actions-ci-cd.md)。