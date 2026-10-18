# ZooKeeper

ZooKeeper 3.8.4 三节点集群的搭建与验证记录。

| 文档 | 内容 | 环境 |
| :--- | :--- | :--- |
| [ZooKeeper 3.8 三节点集群](./Zookeeper-3.8.4-集群搭建实录.md) | 角色与选主、zoo.cfg 参数、znode 操作、watch、四字命令、故障演练、调优 | Rocky Linux 9 · ZK 3.8.4 · JDK 17 |

---

## 目录

| 章节 | 内容 |
| :--- | :--- |
| 一 | 核心概念：角色分工、myid 与 Zxid、三个端口、节点类型、数据模型 |
| 二 | 环境准备：目录规划、下载（`-bin` 包）、JDK、环境变量 |
| 三 | 配置：zoo.cfg 参数详解、myid、systemd unit |
| 四 | 启动与集群成形、角色确认 |
| 五 | 八项功能验证：端口、读写、节点类型、删除限制、跨节点一致、临时节点回收、watch、四字命令、故障演练 |
| 六 | 调优：堆内存、日志轮转、磁盘 |
| 七 ~ 十一 | 图形化管理、排错、卸载、验收清单、参考 |

## 已验证环境

| 项 | 值 |
| :--- | :--- |
| OS | Rocky Linux 9 |
| ZooKeeper | 3.8.4 |
| JDK | openjdk 17 |
| 节点 | 3 台，全部 participant |
| myid | 1 / 2 / 3 |

## 几个关键点

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

确认角色分布：

```bash
zkServer.sh status
```

期望 1 台 `Mode: leader` + 2 台 `Mode: follower`。若三台均为 `Mode: standalone`，检查 myid 与 `server.N` 配置。

---

## 参考

- [ZooKeeper 3.8 官方文档](https://zookeeper.apache.org/doc/r3.8.4/index.html)
