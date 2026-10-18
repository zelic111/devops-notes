# Linux 运维实验记录

> 分布式中间件的三节点集群搭建实录。按实际执行顺序写，包括中途卡住的地方和解决办法——不只是"命令能跑通"，而是"为什么这么配、跳过会出什么事"。

## 文档

| 文档 | 内容 | 环境 |
| :--- | :--- | :--- |
| [Elasticsearch 7.17 三节点集群](./Elasticsearch-7.17.22-学习笔记.md) | 部署原理、集群发现、索引与文档 API、批量操作、查询 DSL、映射、排错 | Rocky Linux 9.8 · ES 7.17.22 · JDK 随包 |
| [ZooKeeper 3.8 三节点集群](./Zookeeper-3.8.4-集群搭建实录.md) | 角色与选主、zoo.cfg 参数、znode 操作、watch、四字命令、故障演练、调优 | Rocky Linux 9 · ZK 3.8.4 · JDK 17 |

---

## Elasticsearch 7.17 三节点集群

从零搭建 ES 7.17 三节点集群的完整记录：部署原理、配置要点、索引与文档 API、批量操作、查询 DSL 与排错手册。

不满足于命令速查——每个配置项都说明它解决什么问题、跳过会出什么事。

### 目录

| 章节 | 内容 |
| :--- | :--- |
| 一 ~ 二 | 核心概念（索引 / 分片 / 副本 / 分配）、架构决策与集群发现机制 |
| 三 ~ 四 | 集群部署（安装、内核参数、配置、堆内存）、四层验证方法 |
| 五 ~ 七 | 索引管理、文档管理、`_bulk` / `_mget` 批量操作 |
| 八 ~ 十 | 查询 DSL、映射与字段类型、别名切换 |
| 十一 ~ 十二 | 排错手册（集群层 / 索引层 / 环境层）、卸载 |
| 十三 ~ 十五 | 部署清单、验收清单、实战练习 |

### 已验证环境

| 项 | 版本 |
| :--- | :--- |
| OS | Rocky Linux 9.8 |
| Elasticsearch | 7.17.22（RPM） |
| 节点 | 3 台，全角色（master-eligible + data） |
| JVM | 随包自带 JDK，堆固定 1 GB |

Debian / Ubuntu 环境仅需替换包管理命令（`dpkg -i` / `dpkg -r`），其余配置一致。

### 四个实证结论

部署过程中遇到的问题，根因都不是"配置写错了"，而是对机制理解有偏差。

**1. `network.publish_host` 在 overlay 网络下必须显式指定**

只写 `network.host: 0.0.0.0` 时，ES 会自行挑选一个地址向其他节点宣告，且**不区分业务网卡与虚拟网卡**。宿主机上存在 flannel 时，可能选中 `10.244.1.0` 这类子网地址。集群仍能达到 `green`（overlay 内部互通），但集群外客户端无法连接、排障时容易误判、更换网络插件后可能直接起不来。

**2. 7.17 的堆内存并非默认 1 GB**

`jvm.options` 里没有固定堆大小（只有一行注释 `## -Xms4g`），ES 会按**物理内存的 50%** 推算。4 GB 内存的机器推算接近 2 GB，加上堆外开销必然 OOM。必须显式设置：

```bash
printf -- '-Xms1g\n-Xmx1g\n' > /etc/elasticsearch/jvm.options.d/heap.options
```

经验法则：物理内存 ≈ 堆 × 2，另一半留给 page cache。

**3. `/etc/sysctl.d/` 的加载顺序由文件名排序决定**

`systemd-sysctl` 按文件名排序加载三个目录下的 `*.conf`。自定义配置若命名为 `99-elasticsearch.conf`，会排在 `99-sysctl.conf` 之前（`e` < `s`），可能被后者的同名参数覆盖。应使用排序靠后的名字，如 `99-zz-elasticsearch.conf`。

**4. 配置文件不允许重复键**

向 `elasticsearch.yml` **追加**而非**修改原行**，会出现两行 `network.host`，触发 `JsonParseException: Duplicate field` 并直接导致进程退出。

---

## ZooKeeper 3.8 三节点集群

ZooKeeper 是分布式协调服务，为 Kafka、HBase、Dubbo 等系统提供配置管理、命名服务、分布式锁与选主能力。三节点是最小的高可用集群：容忍一台故障，多数票（2/3）依然成立。

文章覆盖从目录规划到集群成形、八项功能验证、故障演练与调优的完整过程。

### 目录

| 章节 | 内容 |
| :--- | :--- |
| 一 | 核心概念：角色分工、myid 与 Zxid、三个端口、节点类型、数据模型 |
| 二 | 环境准备：目录规划、下载（`-bin` 包）、JDK、环境变量 |
| 三 | 配置：zoo.cfg 参数详解、myid、systemd unit |
| 四 | 启动与集群成形、角色确认 |
| 五 | 八项功能验证：端口、读写、节点类型、删除限制、跨节点一致、临时节点回收、watch、四字命令、故障演练 |
| 六 | 调优：堆内存、日志轮转、磁盘 |
| 七 ~ 十一 | 图形化管理、排错、卸载、验收清单、参考 |

### 已验证环境

| 项 | 值 |
| :--- | :--- |
| OS | Rocky Linux 9 |
| ZooKeeper | 3.8.4 |
| JDK | openjdk 17 |
| 节点 | 3 台，全部 participant |
| myid | 1 / 2 / 3 |

### 几个关键点

**`Error contacting service` 不等于启动失败**

第一台启动后必然看到这句报错。此时集群只有 1/3 节点在线，凑不齐多数票，无法完成选主。三台全部启动后自愈——这是最容易误判的一步。

**必须关闭 AdminServer**

ZK 3.5+ 默认开启 AdminServer 监听 8080。若该端口已被占用，启动直接报 `Address already in use`。不需要 Web 管理界面时配置 `admin.enableServer=false`。

**四字命令跨机无输出是安全默认值**

3.5+ 仅允许本机访问四字命令。远程监控需显式配置 `4lw.commands.whitelist`，生产环境建议只放开 `srvr,mntr,ruok` 而非 `*`。

**重装前必须清空 dataDir**

残留的 `myid` 与 `version-2/` 会带着旧 Zxid 与新集群混合，导致事务 ID 错乱、反复选主。分布式系统的元数据带着"历史"，不能跨实例复用。

---

## 排错速查

### Elasticsearch

判断哪条错误才是致命的——查看 `journalctl` 输出末尾：

```bash
journalctl -u elasticsearch -n 30 --no-pager
```

进程退出的标志是 `Main process exited, code=exited, status=1/FAILURE`，**紧邻其上方那条 `Caused by:` 才是根因**。`[ERROR]` 级别条目（如 GeoIP 的 `PKIX path building failed`）多数不致命。

### 集群颜色

| 颜色 | 含义 |
| :--- | :--- |
| `green` | 全部主分片与副本均已分配 |
| `yellow` | 主分片齐全，**部分副本未分配**（数据完整，冗余缺失） |
| `red` | **至少一个主分片未分配**（对应数据不可读写） |

两节点集群出现 `yellow` 属正常现象：副本不能与主分片位于同一节点。

### ZooKeeper 角色

```bash
zkServer.sh status
```

期望 1 台 `Mode: leader` + 2 台 `Mode: follower`。若三台均为 `Mode: standalone`，检查 myid 与 `server.N` 配置。

---

## 参考

- [Elasticsearch 7.17 官方文档](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/index.html)
- [ZooKeeper 3.8 官方文档](https://zookeeper.apache.org/doc/r3.8.4/index.html)

## 许可

本文档采用 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) 许可。
