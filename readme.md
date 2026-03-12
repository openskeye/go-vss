# go-vss

`go-vss` 是一款采用 **Go 语言** 开发的**高性能视频汇聚流媒体平台**，全面支持 **GB/T 28181、ONVIF、RTSP、RTMP、WebRTC** 等主流协议。平台专注于解决异构设备接入、多协议并发等行业痛点，实现海康、大华、宇视等品牌监控设备的**统一接入与管理**。

## 一、项目概述

本项目是一套基于**GB28181**国标协议的视频安全监控平台，采用Go语言与go-zero微服务框架，包含国标信令（SIP）、流媒体、管理后台、定时任务、数据服务等模块。

### 核心能力

- **国标 GB28181**：设备注册、目录订阅、实时视频、回放、云台控制、语音对讲
- **级联分发**：支持平台级联（如上级平台调度），视频流可转码为 RTSP/RTMP/FLV/HLS/WebRTC 格式全网分发（GBC）
- **多协议设备兼容**：支持 GB/T 28181、RTSP、RTMP、ONVIF 等主流协议，兼容 95% 以上主流硬件设备
- **流媒体**：对接媒体服务器（SkeyesMS），接收/拉取/转发 RTP 流
- **管理后台**：设备、通道、用户、录像计划、系统配置等
- **全流程视频管理**：多分屏直播、支持云端录像与设备本地录像回放、云台控制、语音对讲
- **智能分析**：集成烟火检测、区域入侵、口罩识别等算法，输出告警快照与可视化报表
- **AI 算法融合**：插件化集成：以插件形式集成第三方 AI 服务（如客流统计、违停检测），用户按需安装算法插件，避免多平台配置碎片化
- **开放生态与集成**：提供设备管理、直播控制等 API，JWT鉴权
- **全链路工具链**：配套 APP（移动端推流）、SkeyeWEBPlayer.js（无插件 H5 播放器）等免费工具，覆盖采集到播放全流程
- **灵活部署**：支持 Windows/Linux 系统、Docker 容器化部署，适配 X86/ARM 架构，覆盖本地服务器、私有云及边缘计算节点

---

## 二、技术栈与架构

| 类别   | 技术                                      |
|------|-----------------------------------------|
| 语言   | Go                                      |
| 微服务  | go-zero（REST API、gRPC、goctl 代码生成）       |
| 数据库  | MySQL / SQLite                          |
| 缓存   | Redis                                   |
| 服务发现 | etcd                                    |
| 协议   | GB28181 SIP（TCP/UDP）、HTTP、WebSocket、SSE |
| 媒体   | 自研媒体服务 SkeyesMS（RTMP/RTSP/RTC 等）        |
| 部署   | Docker Compose、二进制 + Guard 守护进程         |

### 服务依赖关系（简图）

```
                    ┌─────────────┐
                    │   Web 代理   │  (静态资源 + 反向代理 API)
                    └──────┬──────┘
                           │
    ┌──────────────────────┼──────────────────────┐
    │                      │                      │
    ▼                      ▼                      ▼
┌─────────┐          ┌─────────────┐        ┌──────────┐
│ Backend │◄────────►│  DB (gRPC)  │◄───────│  Redis   │
│   API   │          │  Config/    │        │  etcd    │
└────┬────┘          │  Device/    │        └──────────┘
     │               │  Backend    │
     │               └──────┬─────┘
     │                      │
     │               ┌──────┴─────┐
     │               │    MySQL   │
     │               └────────────┘
     │
     ▼
┌──────────────────────────┐     ┌─────────────┐     ┌───────────────┐
│             VSS          │◄───►│ MediaServer │     │    Cron       │
│ (SIP/HTTP/Websocket/SSE) │     │ (SkeyesMS)  │     │ (定时/录像...) │
└──────────────────────────┘     └─────────────┘     └───────────────┘
```

- **Backend API**：对外 REST 接口，调用 DB RPC、VSS、Redis 等。
- **VSS**：国标信令（GBS/GBC SIP）、HTTP 回调、WebSocket、SSE，并请求媒体服务收流/拉流。
- **DB (dbrpc)**：统一数据访问层，提供 BackendService、ConfigService、DeviceService 等 gRPC。
- **Cron**：定时任务、录像计划、与 DB 和媒体服务联动。
- **Guard**：可选守护进程，用于在 Windows/Linux 上按顺序启动/停止上述服务（含 MySQL/Redis 等）。

---

## 三、目录结构

```
skeyevss/
├── .env*                      # 环境变量（.env.local.default / .env.prod 等）
├── bin/                       # 二进制依赖（如 sql2gorm、goctl）
├── core/                      # 核心代码
│   ├── app/
│   │   ├── sev/               # 主业务服务
│   │   │   ├── backend/       # 管理后台 REST API（main.go）
│   │   │   ├── cron/          # 定时任务服务
│   │   │   ├── db/            # 数据层 gRPC 服务
│   │   │   ├── vss/           # 国标 VSS（SIP + HTTP + WS + SSE）
│   │   │   └── guard/         # 守护进程（一键启停所有服务）
│   │   ├── sk/                # 官网相关
│   │   │   ├── backend/       # 官网后台 API
│   │   │   └── frontend/      # 官网前台 API
│   │   └── tools/             # 构建/激活码等工具（main.go）
│   ├── common/                # 公共逻辑（如 skeyevssSev 启动/停止）
│   ├── constants/             # 全局常量
│   ├── localization/          # 国际化
│   ├── pkg/                   # 通用包（orm、redis、pubsub、imgcaptcha 等）
│   ├── repositories/          # 数据仓库
│   │   ├── models/            # 表模型（*/*.go）
│   │   └── redis/             # Redis 封装
│   └── tps/                   # 全局类型与配置结构
├── docker/                    # 各服务 Dockerfile
├── docker-compose.yml         # 编排（mysql/redis/etcd/jaeger + 各应用）
├── etc/                       # 服务配置文件（.backend-api.yaml 等）
├── logs/                      # 运行日志
├── scripts/                   # 脚本
│   ├── dev/                   # 开发脚本（sev-api.sh、sev-db.sh、sev-rpc.sh 等）
│   ├── docker/                # 镜像构建与启动
│   └── jenkins/               # CI 构建
├── source/                    # 静态资源与文档
│   └── doc/                   # API 说明、GB28181 流程等
├── templates/                 # 代码生成模板（sql、go-zero、proto）
└── docs/                      # 项目文档（本文档所在）
```

---

## 四、服务说明

### 4.1 端口与配置（以 .env.local.default 为参考）

| 服务              | 默认端口                                              | 配置文件                      | 说明                            |
|-----------------|---------------------------------------------------|---------------------------|-------------------------------|
| MySQL           | 11001                                             | -                         | 数据库(也可使用Sqlite)               |
| Redis           | 11002                                             | -                         | 缓存与队列                         |
| etcd            | 11003(Client) / 11016(Peer)                       | -                         | 服务发现                          |
| Web Proxy       | 11004                                             | etc/.web-sev.yaml         | 前端静态 + /api-backend 等反向代理     |
| Media Server    | 11005...(HTTP)                                    | etc/skeyesms.conf         | SkeyesMS，RTMP/RTSP/RTC 等      |
| VSS             | 11008(SIP) / 11013(HTTP) / 11014(SSE) / 11018(WS) | etc/.vss.yaml             | Video Security System,国标信令与回调 |
| Cron            | 11009                                             | etc/.cron.yaml            | 定时任务, 消息队列                    |
| DB RPC          | 11010                                             | etc/.db-rpc.yaml          | 数据管理 gRPC                     |
| Backend API     | 11011                                             | etc/.backend-api.yaml     | 管理后台 REST                     |
| Guard           | 11012                                             | etc/.guard.yaml           | 守护进程                          |
| SK Backend API  | 11030                                             | etc/.sk-backend-api.yaml  | 官网后台                          |
| SK Frontend API | 11031                                             | etc/.sk-frontend-api.yaml | 官网前台                          |

### 4.2 各服务职责简述

- **dbrpc**：对 MySQL/Redis 的封装，提供配置、设备、后台等 gRPC；需先启动 etcd、MySQL、Redis。
- **vss**：
    - SIP：GBS（TCP/UDP）、GBC 级联（TCP/UDP）；
    - 业务：设备注册、目录、邀请收流、心跳、语音对讲、SIP 日志等；
    - HTTP：媒体服务器回调（on_pub_start/stop、on_sub_start 等）；SSE/WebSocket 供前端实时数据。
- **backendapi**：管理后台所有 REST 接口（设备、通道、用户、录像、配置等），依赖 etcd、Redis、dbrpc、vss。
- **cron**：拉取配置与定时项、录像计划（如 VideoProjectLogic），与 DB、媒体服务配合。
- **webproxy**：对外提供前端页面和 API 代理，需先有 Backend 等后端服务。
- **skeyesms**：独立媒体服务进程，需单独部署/启动；VSS 通过 HTTP 调用其 open/close 流接口。
- **guard**：读取 `.guard.yaml` 与 env，按顺序启动 MySQL/Redis/etcd、媒体、DB、VSS、Backend、Cron、Web 等，用于生产一键启停。

---

## 五、项目流程

### 5.1 国标设备实时流播放（GB28181）

（与 `source/doc/GB28181.md` 对应）

1. **用户侧**：在前端点击「播放」国标设备视频。
2. **获取流媒体地址**：后端解析出需要使用的流媒体信息。
3. **若是回放**：先按流名称请求媒体服务停止该流，再进入邀请流程。
4. **发送 Invite**：
    - 调用 GBS 服务，生成 `streamName` 并加锁。
    - 通过媒体服务查询流信息 `streamRes`；若已存在且有效则直接复用退出。
    - 校验防止流被占用（结合 PubStreamExistsState）。
    - 将 Invite 请求投递到 `inviteChannel`。
5. **后台 goroutine 处理 inviteChannel**：
    - 调用媒体服务 **start_rtp_pub**（开始接收国标 RTP 推流）。
    - 向设备发送 **Invite**。
    - 向设备发送 **ACK**。
    - 向媒体服务发送 **ack_rtp_pub**（拉流/关联）。
    - 记录 **PubStreamExistsState**。
6. **媒体服务回调**：如 **on_pub_stop** 时清除 PubStreamExistsState。
7. **快照**：可并行走快照接口。
8. ...

整体上，VSS 负责 SIP 信令与流生命周期，媒体服务负责实际收流、转码、分发。

### 5.2 数据流与调用关系

- 前端 → **Web 代理** → **Backend API**（鉴权、参数校验）。
- Backend API → **DB RPC**（配置、设备、用户等 CRUD）与 **VSS**（国标控制、流信息）。
- VSS → **媒体服务**（创建/关闭收流、拉流）与 **DB RPC**（设备状态等）。
- Cron → **DB RPC**（配置、录像计划）与 **媒体服务**（录像启停）。
- 配置与发现：各服务通过 **etcd** 发现 DB、VSS 等；密钥、数据库等来自 **.env** 与 **etc/*.yaml**。

---

## 六、环境配置与使用方式

### 6.1 环境变量

- 复制 `.env.local.default` 为 `.env` 或 `.env.local`，按本机修改：
    - `SKEYEVSS_ROOT`、数据库/Redis/etcd 地址与端口、媒体服务/VSS/Backend 等端口。
    - 修改 **`SKEYEVSS_INTERNAL_IP`** 内网ip, **`SKEYEVSS_EXTERNAL_IP`** 外网ip。
    - 前端路径 `SKEYEVSS_BACKEND_WEB_CODE_PATH`、媒体服务代码路径 `SKEYEVSS_MEDIA_SERVER_CODE_PATH`（若本地开发）。
- 生产使用 `.env.prod` 或 `.env.prod.d`，与 docker-compose 或 Guard 使用的路径一致。

重要项示例：

- `SKEYEVSS_DATABASE_TYPE`：mysql / sqlite
- `SKEYEVSS_MYSQL_*` / `SKEYEVSS_SQLITE_*`
- `SKEYEVSS_REDIS_*`、`SKEYEVSS_ETCD_*`
- `SKEYEVSS_VSS_*`、`SKEYEVSS_BACKEND_*`、`SKEYEVSS_MEDIA_SERVER_*`
- `SKEYEVSS_BACKEND_ADMIN_USERNAME` / `PASSWORD`（管理后台默认账号）

### 6.2 本地开发启动顺序

1. **基础依赖**（若不用 Docker 则本地起）：  
   MySQL、Redis、etcd（及可选 Jaeger）。

2. **加载环境变量**（以下均需先执行）：  
   `source .env` 或脚本里 `functions.OverloadEnvFile(*envFilePath)` 等价。

3. **启动服务**（严格顺序）：
    - dbrpc：  
      `-env .env.local -f etc/.db-rpc.yaml`
    - vss：  
      `-env .env.local -f etc/.vss.yaml`
    - backendapi：  
      `-env .env.local -f etc/.backend-api.yaml`
    - cron：  
      `-env .env.local -f etc/.cron.yaml`
    - webproxy（如需完整前后端）：  
      `-env .env.local -web-static-dir <前端构建目录> -f etc/.web-sev.yaml`
    - 媒体服务 SkeyesMS 需单独按自身文档启动，并保证 VSS 中配置的媒体 HTTP 地址可达，如果不指定配置将使用默认配置。
      `-c skeyesms.conf`

4. **开发脚本**（在 `scripts/dev/` 下）：
    - `constants.sh`：项目路径、goctl、模板路径配置等。
    - `sev-db.sh`：按 SQL 生成 Model（repositories/models/xxx）。
    - `sev-rpc.sh`：生成 db 的 gRPC。
    - `sev-api.sh`：生成 backend 的 REST API（需修改脚本内 server_name 等）。

### 6.3 Docker Compose 启动

- 使用 `docker-compose` 时必须先有 env 文件（如 `.env.prod.d`），其中 `SKEYEVSS_*` 与 compose 中变量一致。
- 常用 profile：`conf`、`core`、`needed-update` 等，例如：  
  `docker-compose --profile xxxx up -d`
    - `core` 核心服务
    - `needed-update` 基础服务更新
    - `conf` docker-composer 配置内容替换更新
      会启动 mysql、redis、etcd、jaeger、webproxy、skeyesms、dbrpc、vss、backendapi、cron 等。
- 数据卷、日志路径见 `docker-compose.yml` 中 `volumes`（如 `SKEYEVSS_SEV_VOLUMES_DIR`）。

### 6.4 Guard 守护进程 启动

- 以**管理员权限**运行 Guard(main) 可执行文件，会按配置启动 MySQL/Redis/etcd、媒体、DB、VSS、Backend、Cron、Web Proxy 等。
- 配置文件：`etc/.guard.yaml`，配合 env（如 `.env.prod`）。
- 子命令：如 `start`、`stop`、`restart`、`help`（见 guard main 中 `service.Control`）。
- 日志：如 `journalctl -u SkeyevssSevGuard -f` 或安装目录下 `skeyevss-sev/logs/...`。

---

## 七、构建与部署

### 7.1 工具入口（core/app/tools）

运行：

```bash
go run core/app/tools/main.go -f etc/.base.yaml -env .env.build.sev -index <序号>
```

可选序号：

- 1：生成激活码
- 2：构建服务（当前平台）
- 3：获取本机序列号
- 4：构建所有平台并上传（含基础依赖）
- 5：构建所有平台并上传（不含基础依赖）
- 6：构建所有平台并上传（基础依赖不含 MySQL）
- 7：执行 4、5、6

构建结果输出在 tmp/Skeyevss.*.zip。<br>
**simplified**不包含基础依赖, **full** 包含所有服务依赖, **full-1** 基础依赖不包含mysql。

### 7.2 Jenkins / 脚本构建

- `scripts/jenkins/build.sh`：
    - `-d` / `--docker`：构建 Docker 镜像并推送到 Harbor。
    - `-b` / `--bin-package`：构建二进制安装包并上传（内部调用 `go run core/app/tools/main.go ... -index 7`）。
    - `-a`：全部。
- Docker 镜像构建：`scripts/docker/build.docker.images.sh` 等。

### 7.2 单机/现场部署

软件包下载地址 `https://frontend.openskeye.cn/releases`

- 通过官网下载对应的软件包。
- 注意区分不同系统、架构
- 官网提供的是不带数据库版本
- **修改env.prod** `SKEYEVSS_INTERNAL_IP`、`SKEYEVSS_EXTERNAL_IP`等配置（注意端口信息是否冲突）

**解压后的目录**

```
skeyevss/
├── .env.prod                       # 环境变量
├── Skeyevss.bat                    # windows 启动脚本
├── Skeyevss.sh                     # linux 启动脚本
├── skeyevss-sev/              
│   ├── backend-web/                # 管理后台网页打包代码
│   ├── sev/                        # 主业务服务
│   |   ├── etc/                    # 服务依赖配置
│   |   ├── ms-conf/                # 媒体服务依赖配置
│   |   ├── source/                 # 服务运行产生的文件与项目静态文件
│   |   ├── SkeyevssSevBackendApi*  # 后台接口服务
│   |   ├── SkeyevssSevCron*        # 任务服务
│   |   ├── SkeyevssSevDB*          # 数据服务
│   |   ├── SkeyevssSevGuard*       # 守护进程
│   |   ├── SkeyevssSevMediaServer* # 媒体服务
│   |   ├── SkeyevssSevVss*         # 视频安全监控系统服务
│   |   ├── SkeyevssWebServer*      # web代理服务
```

**windows**: 进入目录以管理员身份运行`Skeyevss.bat`会自动注册服务并运行

**linux**: 进入目录以管理员身份运行`Skeyevss.sh`会自动注册服务并运行

### 7.3 Docker 部署

脚本下载地址 `https://frontend.openskeye.cn/releases`

- 下载docker版本
- 解压后得到 `start.sh`, `docker-compose.yml`

启动运行`sh start.sh`。<br>
脚本会自动拉取镜像并启动所有对应服务


---

## 八、API 使用说明

### 8.1 通用约定（见 source/doc/api/common.md）

- **鉴权**：请求头 `Authorization: <token>`。
- **Content-Type**：`application/json`。
- **统一响应**：如 `timestamp`、`node`、`version`、`data`、`code`、`message`、`token`、`logout`、`reset-pwd`、`license` 等，错误时常用
  HTTP 400/401/403。

### 8.2 通用请求体（ReqParams）

- **列表**：`limit`、`page`、`orders`、`conditions`、`keyword`、`uniqueId`、`all` 等。
- **排序**：`orders[]` 含 `column`、`value`（asc/desc）。
- **条件**：`conditions[]` 含 `column`、`value`/`values`、`operator`（=、!=、>、<、like、IN、notin、match 等）、`logicalOperator`
  （AND/OR）。
- **更新**：`data[]`（column+value）、`bulkUpdates[]`、`ignoreUpdateColumns` 等。
- **删除**：`conditions` 指定要删除的记录。

示例：list、add、update、delete 的 JSON 示例见 `source/doc/api/common.md`。

---

## 九、开发指南

### 9.1 开发前准备

这是一个开发测试版本，如果需要完整版本请联系作者：

- 1003275805@qq.com
- 295222688@qq.com

完整版本包含 `设备控制`、`预置位`、`录像`、`sms`等，功能更强大、更稳定，长期技术支持。

- 开发环境
    1. 后端代码使用`Go`语言开发，需要配置**go**语言开发环境 go版本 >= 1.23.10 [https://go.dev](https://go.dev)
    2. 前端代码使用`React(18.2.0)` `Typescript`，需要配置`node`开发环境 [https://nodejs.org](https://nodejs.org/zh-cn)
    3. 手机端App使用`Flutter`开发，dart版本 >= 3.10.7，flutter版本 >= 3.38.7 [https://docs.flutter.cn](https://docs.flutter.cn)

- 服务依赖
    1. **流媒体服务**：下载对应系统的流媒体二进制文件 [https://go.dev](https://go.dev)
    2. **redis**：[https://redis.io](https://redis.io)
    3. **etcd**：[https://github.com/etcd-io/etcd](https://github.com/etcd-io/etcd)
    4. **mysql**：默认数据库为**sqlite**，如果需要使用`mysql`记得修改env配置 [https://www.mysql.com](https://www.mysql.com)

- env配置（cp .env.default .env）
    1. `SKEYEVSS_INTERNAL_IP=内网ip`，`SKEYEVSS_EXTERNAL_IP=公网ip`，请不要填写127.0.0.1，如果为正确配置视频流可能不会正常播放。
    2. `SKEYEVSS_MYSQL_*` mysql配置如果需要
    3. `SKEYEVSS_REDIS_*` redis
    4. `SKEYEVSS_ETCD_*` etcd

以上内容准备完毕后，进入 `core/app/sev/*`，首先启动 `core/app/sev/sev`，`core/app/sev/vss`，`core/app/sev/backend`，`core/app/sev/cron`<br>
启动参数 `go run main -f 配置文件路径 -env 环境变量`，如果不指定参数将使用默认值 `-f etc/.xx.yaml -env .env.local`，详细请参考`core/app/sev/*/main.go`


### 9.2 新增数据表与 Model

1. 在 `templates/sql/` 下新增 `表名.sql`（建表语句）。
2. 在 `scripts/dev/` 下执行 `sev-db.sh`（需先改脚本内 `name` 为表名），会生成：
    - `core/repositories/models/<name>/model.go`
    - `variables.go`、`data.go`、`db.go`
3. 按需在 `data.go` 中补全转换逻辑，在 `db.go` 中补全查询封装。

### 9.3 新增/修改 Backend API

1. 修改 `templates/apis/backend-api.api`（或对应 .api 文件）。
2. 在 `scripts/dev` 下执行 `sev-api.sh`（注意脚本内 `server_name`、`use_orm_params`），会生成/覆盖 handler、logic，配置文件会放到
   `etc/.backend-api.yaml` 等。
3. 若使用自定义模板，脚本会替换 `api-handler.tpl`、`api-logic.tpl` 等（见 sev-api.sh）。

### 9.4 修改 DB RPC

1. 修改 `core/app/sev/db/*.proto`。
2. 在 `scripts/dev` 下执行 `sev-rpc.sh`，会生成 gRPC 与 zrpc 代码，配置移动到 `etc/.db-rpc.yaml`。

### 9.5 配置说明

- 所有 `etc/.xxx.yaml` 中大量使用环境变量占位符，例如 `Mode: "${SKEYEVSS_SERVER_ENV_MODE}"`、
  `Port: ${SKEYEVSS_BACKEND_API_PORT}`。
- 实际值来自启动时加载的 .env 文件；Docker/Guard 使用同一套 env 保证端口与路径一致。

---

## 十、协作与优缺点

- 在团队协作开发中，需要严格遵守代码生成规则。
- 请求尽量使用通用参数。
- 尽量使用公共函数、package避免代码冗余。
- 检查已存在的接口，预防重复工作。

---

### 10.1 架构清晰、职责划分明确

- **数据层集中**：所有持久化通过 DB RPC（dbrpc）统一暴露，Backend API 不直连数据库，便于做连接池、监控与权限控制。
- **领域边界清楚**：VSS 专注国标信令与流生命周期，Cron 专注定时与录像计划，Backend 专注 REST 与鉴权，媒体服务独立进程，符合单一职责。
- **服务发现统一**：etcd 做 RPC 发现，配置与密钥通过 .env + yaml 注入，同一套配置可支撑本地、Docker、Guard 多种部署。

### 10.2 技术栈与工程化

- **go-zero 规范**：Handler → Logic → ServiceContext 分层明确，REST/ gRPC 由 goctl 生成，目录结构一致，新人容易上手。
- **代码生成闭环**：`scripts/dev` 下 sev-db.sh / sev-rpc.sh / sev-api.sh 覆盖 Model、RPC、API 生成，配合 templates 可保持风格统一。
- **ORM 抽象**：`core/pkg/orm` 封装 GORM，统一连接、日志、事务；CachePlugin 支持内存/Redis 缓存，模型实现 `orm.Model`
  接口，便于扩展（如 Correction、UseCache）。

### 10.3 数据与模型规范

- **Repository 分层**：每个表对应 `repositories/models/<name>/` 下 model.go、variables.go、data.go、db.go，表结构、列常量、转换、DB
  操作集中，便于复用与重构。
- **通用请求体 ReqParams**：list/add/update/delete 使用统一的 conditions、orders、data、bulkUpdates 等，前端与后端约定清晰，见
  `source/doc/api/common.md`。

### 10.4 部署与运维

- **多形态部署**：Docker Compose 适合多机/云环境；Guard 一键启停 MySQL/Redis/etcd 及业务进程，适合单机/现场部署。
- **可观测性**：集成 pprof、Jaeger（可选），日志路径与级别可配置，便于排障。
- **配置与环境解耦**：yaml 中大量使用 `${SKEYEVSS_*}`，同一份 yaml 通过不同 .env 适配开发/测试/生产，减少配置漂移。

### 10.5 业务适配

- **国标与媒体解耦**：VSS 只做信令与流控制，收流/转码/分发交给 SkeyesMS，符合中平台与媒体分离。
- **级联与多协议**：GBC/GBS、TCP/UDP、HTTP/WS/SSE 在 VSS 内分层处理，扩展新协议或新厂商时边界清晰。

---

### 10.6 全局状态与可测试性

- **Backend ServiceContext 包级变量**：`servicecontext.go` 中 `healthCache`、`settingRow`、`authRes`、`dictRes`、`msRes` 等为
  package-level 全局变量，被多个 goroutine 读写（health、auth、dict、setting、ms、deviceStatistics）。依赖 channel，逻辑相对分散，后期有待优化。

### 10.7 配置复杂度与一致性

- **.env 体量大**：`.env.local.default`容易漏改或改错。
- **yaml 与 env 强绑定**：yaml 中大量占位符依赖 env，若某 key 未导出或拼写错误，表现可能是空字符串或异常值。

### 10.8 基础框架版本兼容

- go-zero版本较低，很多框架特性与新功能需要更新框架，开发测试成本较高

---

## 十一、常见问题

1. **端口冲突**：检查 .env 与各 `etc/.xxx.yaml` 中端口是否与现有服务冲突。
2. **DB 连接失败**：确认 MySQL 已启动、账号密码与 `SKEYEVSS_MYSQL_*` 一致、库已创建。
3. **etcd 连接失败**：确认 etcd 已启动，且 `SKEYEVSS_ETCD_HOST`、`SKEYEVSS_ETCD_CLIENT_PORT` 正确。
4. **VSS 无法拉流**：确认媒体服务已启动，且 VSS 配置的媒体 HTTP 地址可访问；防火墙放行 SIP/RTP 端口。
5. **Guard 需管理员权限**：Windows/Linux 上以管理员运行，否则可能无法启动子进程或绑定端口。
