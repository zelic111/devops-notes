# 从零搭建 ZooKeeper 3.8 三节点集群：配置、验证与排错

## 开篇

ZooKeeper 是分布式协调服务，为 Kafka、HBase、Dubbo 等系统提供配置管理、命名服务、分布式锁与选主能力。它本身不存业务数据，但几乎所有分布式中间件都依赖它来达成"谁是主、谁在线、配置是什么"这类共识。

三节点是最小的高可用集群：容忍一台故障，多数票（2/3）依然成立。本文记录一套完整的搭建过程——从目录准备到集群成形，再到读写验证、故障演练与调优。

文中命令在两台以上 Rocky Linux 9 + ZooKeeper 3.8.4 + JDK 17 上验证。示例使用 `192.168.5.104 / .105 / .106`，替换为自己的地址即可。

---

## 一、核心概念

搭建前需要理解四个概念，否则配置项只是照抄。

### 1.1 集群角色

| 角色 | 职责 |
| --- | --- |
| **Leader** | 唯一处理写请求的节点，负责发起投票与事务提交 |
| **Follower** | 处理读请求，参与投票，同步 Leader 的数据 |
| **Observer** | 同 Follower，但**不参与投票**，仅扩容读能力（3.5+ 支持） |

三节点集群中通常为 1 Leader + 2 Follower。

### 1.2 myid 与 Zxid

| 标识 | 含义 |
| --- | --- |
| **myid** | 每个节点的唯一编号（1~255），写在 `dataDir/myid` 文件中，纯文本单数字 |
| **Zxid** | 事务 ID，全局单调递增的 64 位整数：高 32 位为 Leader 纪元（epoch），低 32 位为事务计数 |

Zxid 是理解"为什么重装必须清空数据目录"的关键——新旧数据混在一起会导致 Zxid 错乱。

### 1.3 三个端口

| 端口 | 用途 |
| --- | --- |
| **2181** | 客户端连接端口，`zkCli.sh`、Kafka、各类 SDK 都连这里 |
| **2888** | 节点间数据同步（Follower → Leader） |
| **3888** | 选主投票通信 |

另有 **8080**：ZK 3.5+ 内置 AdminServer 的 HTTP 端口，默认开启。它常与 Tomcat、其他 Web 服务冲突，是搭建阶段最容易踩的坑。

### 1.4 会话与节点类型

| 节点类型 | 命令 | 特性 |
| --- | --- | --- |
| 永久节点 | `create /path value` | 客户端断开后仍存在 |
| 临时节点 | `create -e /path` | **随会话结束自动删除**，用于服务注册与存活检测 |
| 顺序节点 | `create -s /path` | 名称自动追加单调递增序号，用于队列与分布式锁 |
| 临时顺序节点 | `create -e -s /path` | 两者组合，分布式锁的标准实现 |

节点用 `stat` 查看时，`ephemeralOwner = 0x0` 表示永久节点，非 0 表示临时节点（该值即会话 ID）。

### 1.5 数据模型

ZooKeeper 是一棵树，每个节点称为 znode：

```
/
├── zookeeper            # 系统保留
│   ├── config
│   └── quota
└── app
    ├── service-a
    └── service-b
```

限制：每个 znode 存储数据上限 **1 MB**（可调但不应调），节点数理论上无上限。因此 ZK 适合存元数据，不适合存业务数据。

---

## 二、环境准备

### 2.1 目录规划

三台节点统一执行：

```bash
mkdir -p /data/softwares /data/data/zookeeper/txn
ls -d /data/softwares /data/data/zookeeper
```

`ls` 必须输出两条路径。若目录未创建就写 myid，会报 `没有那个文件或目录`——这是最常见的第一步失误。

目录职责：

| 路径 | 用途 |
| --- | --- |
| `/data/softwares` | 安装包与解压后的二进制 |
| `/data/data/zookeeper` | `dataDir`：快照文件 + myid |
| `/data/data/zookeeper/txn` | `dataLogDir`：事务日志 |

**快照与事务日志分目录存放**是有意为之：事务日志是顺序写，快照写入时会产生大量磁盘 IO，分开可避免互相干扰。

### 2.2 下载

```bash
cd /data/softwares
curl -L -O https://archive.apache.org/dist/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz
tar xf apache-zookeeper-3.8.4-bin.tar.gz
```

**必须下载 `-bin` 包。** 源码包（`apache-zookeeper-3.8.4.tar.gz`）不含 `lib/*.jar`，启动会直接报 `ClassNotFoundException`。

### 2.3 JDK

```bash
java -version
```

无输出或报 `no java in (...)` 说明未安装。ZK 3.8 需要 JDK 8 或以上，推荐 17：

```bash
dnf install -y java-17-openjdk java-17-openjdk-devel
java -version
```

dnf 源不可用时可离线安装：

```bash
cd /data/softwares
curl -L -O https://mirrors.tuna.tsinghua.edu.cn/Adoptium/17/jdk/x64/linux/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz
tar xf OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz
ls -d /data/softwares/jdk-17*

ln -sfn /data/softwares/jdk-17.0.13+11/bin/java /usr/local/bin/java
ln -sfn /data/softwares/jdk-17.0.13+11/bin/javac /usr/local/bin/javac
```

软链中的版本号按 `ls` 的实际输出调整。

### 2.4 环境变量

```bash
printf 'export JAVA_HOME=%s\n' "$(dirname $(dirname $(readlink -f $(which java))))" > /etc/profile.d/zk.sh
printf 'export ZK_HOME=/data/softwares/apache-zookeeper-3.8.4-bin\n' >> /etc/profile.d/zk.sh
printf 'export PATH=$PATH:${ZK_HOME}/bin\n' >> /etc/profile.d/zk.sh

source /etc/profile.d/zk.sh
echo "JAVA_HOME = $JAVA_HOME"
zkServer.sh version
```

预期输出 `JAVA_HOME` 有值，随后打印 `ZooKeeper version: 3.8.4-...`。

**用 `printf` 而非 `cat <<EOF` 的原因**：从终端或聊天窗口粘贴 heredoc 时，`EOF` 前的换行容易丢失，shell 会卡在 `>` 续行提示符。`printf` 不受此影响。

记下 `JAVA_HOME` 的值，写入 systemd unit 时要用。

---

## 三、配置

### 3.1 zoo.cfg

三台内容完全相同：

```ini
tickTime=2000
initLimit=10
syncLimit=5

dataDir=/data/data/zookeeper
dataLogDir=/data/data/zookeeper/txn
clientPort=2181

server.1=192.168.5.104:2888:3888
server.2=192.168.5.105:2888:3888
server.3=192.168.5.106:2888:3888

admin.enableServer=false
maxClientCnxns=200
autopurge.snapRetainCount=5
autopurge.purgeInterval=24
```

```bash
cat > $ZK_HOME/conf/zoo.cfg <<'EOF'
tickTime=2000
initLimit=10
syncLimit=5

dataDir=/data/data/zookeeper
dataLogDir=/data/data/zookeeper/txn
clientPort=2181

server.1=192.168.5.104:2888:3888
server.2=192.168.5.105:2888:3888
server.3=192.168.5.106:2888:3888

admin.enableServer=false
maxClientCnxns=200
autopurge.snapRetainCount=5
autopurge.purgeInterval=24
EOF

mkdir -p /data/data/zookeeper/txn $ZK_HOME/logs
```

### 3.2 参数说明

| 参数 | 说明 |
| --- | --- |
| `tickTime` | 心跳与超时的基本单位（毫秒），其余超时参数都是它的倍数 |
| `initLimit` | Follower 与 Leader 初始连接的超时：`initLimit × tickTime` = 20 秒 |
| `syncLimit` | Follower 与 Leader 同步的超时：`syncLimit × tickTime` = 10 秒 |
| `dataDir` | 快照目录，也是 myid 所在位置 |
| `dataLogDir` | 事务日志目录，**强烈建议与 dataDir 分离** |
| `clientPort` | 客户端端口，默认 2181 |
| `server.N` | N 为 myid；`2888` 为数据同步端口，`3888` 为选主端口 |

**三个必要调整**：

- `admin.enableServer=false` —— ZK 3.5+ 默认开启 AdminServer 监听 8080。若 8080 已被占用，ZK 启动会失败报 `Address already in use`。不需要 Web 管理界面时应关闭
- `autopurge.*` —— 不配置这两项，快照与事务日志会持续累积，最终占满磁盘。`snapRetainCount=5` 保留 5 份快照，`purgeInterval=24` 每 24 小时清理一次
- `maxClientCnxns=200` —— 默认 60，Kafka 等客户端连接较多时需上调

### 3.3 myid

三台各写不同数字，与 `server.N` 中的 N 对应：

```bash
# node1
echo 1 > /data/data/zookeeper/myid
cat /data/data/zookeeper/myid

# node2
echo 2 > /data/data/zookeeper/myid

# node3
echo 3 > /data/data/zookeeper/myid
```

**myid 放在 `dataDir` 根目录，不是 `txn` 子目录。** 内容为纯数字，末尾可以有换行但不能有其他字符。

### 3.4 systemd unit

```bash
cat > /etc/systemd/system/zk.service <<'EOF'
[Unit]
Description=Apache ZooKeeper Server
After=network.target
RequiresMountsFor=/data

[Service]
Type=forking
User=root
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-17.0.x.x-x.el9.x86_64
Environment=ZOO_LOG_DIR=/data/softwares/apache-zookeeper-3.8.4-bin/logs
ExecStart=/data/softwares/apache-zookeeper-3.8.4-bin/bin/zkServer.sh start
ExecStop=/data/softwares/apache-zookeeper-3.8.4-bin/bin/zkServer.sh stop
ExecReload=/data/softwares/apache-zookeeper-3.8.4-bin/bin/zkServer.sh restart
PIDFile=/data/data/zookeeper/zookeeper_server.pid
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable zk
grep JAVA_HOME /etc/systemd/system/zk.service
```

**`Environment=JAVA_HOME` 是必需的。** systemd 不读取 `/etc/profile.d/` 下的环境变量，缺失时会报 `JAVA_HOME is not set` 并启动失败。值为实际路径，不加引号。

`RequiresMountsFor=/data` 确保数据盘挂载完成后再启动，避免在未挂载时写入根分区。

---

## 四、启动与集群成形

### 4.1 启动

三台都要启动：

```bash
systemctl start zk
sleep 3
zkServer.sh status
```

第一台启动后必然看到：

```
ZooKeeper JMX enabled by default
Using config: /data/softwares/apache-zookeeper-3.8.4-bin/conf/zoo.cfg
Client port found: 2181. Client address: localhost. Client SSL: false.
Error contacting service. It is probably not running.
```

**这句报错不代表启动失败。** 此时集群只有 1/3 节点在线，凑不齐多数票，无法完成选主，因此 `status` 查询不到身份。三台全部启动后自愈。

确认进程确实在运行：

```bash
ss -lntp | grep -E '2181|2888|3888'
ps -ef | grep -v grep | grep zookeeper
tail -30 $ZK_HOME/logs/zookeeper.out
```

日志末尾出现 `LOOKING` 表示正在寻找 Leader，属正常中间状态。

### 4.2 确认角色

三台都启动后再查：

```bash
zkServer.sh status
```

预期：

```
Mode: leader       # 一台
Mode: follower     # 两台
```

若三台都显示 `Mode: standalone`，说明它们没有组成集群，检查两项：

1. myid 文件是否存在且三台不重复
2. `zoo.cfg` 中三行 `server.N` 是否齐全，IP 是否正确

---

## 五、功能验证

集群能启动不等于功能正常。以下八项从不同维度验证。

### 5.1 端口

```bash
ss -lntp | grep -E '2181|2888|3888'
```

三个端口均在监听，且**没有 ZK 占用的 8080**。

### 5.2 基本读写

```bash
zkCli.sh -server 192.168.5.104:2181
```

```
create /demo hello
get /demo                        # hello

create /demo/node1
get /demo/node1                  # null（未赋值）

set /demo/node1 world
get /demo/node1                  # world

create -e /demo/node2            # 临时节点
ls /demo                         # [node1, node2]
```

### 5.3 节点类型确认

```bash
stat /demo/node1
# ephemeralOwner = 0x0          永久节点

stat /demo/node2
# ephemeralOwner = 0x5d...      临时节点
```

### 5.4 删除限制

```bash
create /demo/node1/1
create /demo/node1/2
delete /demo/node1
# Node not empty: /demo/node1   非空节点不能直接删

deleteall /demo/node1            # 递归删除
```

`delete` 只能删空节点，这是防止误删的保护机制。`deleteall` 是 3.5+ 新增的递归删除命令，4.0 起 `rmr` 被移除。

### 5.5 跨节点一致性

换一台机器连接：

```bash
zkCli.sh -server 192.168.5.105:2181
get /demo
```

能读到在 `.104` 写入的数据，才能证明集群成形。**前面几步只能证明单个 ZK 进程能跑。**

### 5.6 临时节点自动清理

在 `.104` 的客户端执行 `quit` 断开，等待 40~60 秒（取决于 session timeout），从另一台连接：

```bash
zkCli.sh -server 192.168.5.106:2181
ls /demo
# node2 已消失
```

这个机制是服务注册与心跳检测的基础：服务实例创建临时节点表示存活，宕机后会话超时，节点自动清除。

### 5.7 Watch 机制

终端 A 设置监听：

```bash
get -w /demo
ls -w /demo
```

终端 B 修改数据：

```bash
set /demo hello
```

终端 A 收到：

```
WATCHER::
WatchedEvent state:SyncConnected type:NodeDataChanged path:/demo
```

**watch 只触发一次**，需要持续监听必须在回调中重新注册。这是 ZK 的经典设计——避免大量 watch 堆积导致服务端内存压力。

### 5.8 四字命令

```bash
dnf install -y nmap-ncat

echo ruok | nc 192.168.5.104 2181     # imok
echo srvr | nc 192.168.5.104 2181     # 服务器状态
echo conf | nc 192.168.5.104 2181     # 当前配置
echo mntr | nc 192.168.5.104 2181     # 监控指标
```

**跨机执行无输出（空白而非报错）** 是 3.5+ 的安全默认值：四字命令仅允许本机访问。需要远程监控时显式放开：

```bash
echo '4lw.commands.whitelist=*' >> $ZK_HOME/conf/zoo.cfg
systemctl restart zk
```

生产环境建议只放开需要的命令，如 `srvr,mntr,ruok`，而非 `*`。

`mntr` 输出的关键指标：

| 指标 | 含义 |
| --- | --- |
| `zk_avg_latency` | 平均请求延迟（ms） |
| `zk_max_latency` | 最大延迟，突增说明有慢请求 |
| `zk_outstanding_requests` | 堆积请求数，持续非 0 说明处理不过来 |
| `zk_followers` | Follower 数量 |
| `zk_synced_followers` | 已同步的 Follower 数量，**小于总数说明有节点掉队** |
| `zk_pending_syncs` | 待同步数 |

### 5.9 故障演练

停一台：

```bash
ssh root@192.168.5.105 'systemctl stop zk'
```

在 `.104` 确认：

```bash
zkServer.sh status
zkCli.sh -server 192.168.5.104:2181    # 仍可读写
```

恢复：

```bash
ssh root@192.168.5.105 'systemctl start zk'
```

**停两台（剩 1/3）时集群拒绝写入**——写操作需要多数票，1 票不足 2/3。此时客户端报 `ConnectionLoss` 或 `SessionExpired`，恢复一台后自动重新选主。

这正是三节点集群的意义与边界：容忍 1 台故障，不容忍 2 台。

---

## 六、调优

### 6.1 堆内存

ZK 默认堆为 1000 MB，对小规模集群过大：

```bash
grep ^ZK_SERVER_HEAP $ZK_HOME/bin/zkEnv.sh
# ZK_SERVER_HEAP="${ZK_SERVER_HEAP:-1000}"

sed -i '/^ZK_SERVER_HEAP/s#1000#512#' $ZK_HOME/bin/zkEnv.sh

systemctl restart zk
ps -ef | grep zookeeper | grep -i xmx     # -Xmx512m
```

经验值：ZK 只存元数据，512 MB 足以支撑数万客户端；节点数与 znode 数量很大时再提到 1024~2048。物理内存有限且同机运行其他中间件时，512 MB 是合理选择。

### 6.2 日志轮转

`zookeeper.out` 会持续增长，需配置 logrotate：

```bash
cat > /etc/logrotate.d/zookeeper <<'EOF'
/data/softwares/apache-zookeeper-3.8.4-bin/logs/zookeeper.out {
    daily
    rotate 7
    compress
    missingok
    notifempty
    copytruncate
}
EOF
```

`copytruncate` 是关键：ZK 进程持有文件句柄，直接 rename 会导致日志写到已删除的文件里，磁盘空间不释放。

### 6.3 磁盘

| 建议 | 原因 |
| --- | --- |
| 事务日志放独立磁盘 | 顺序写性能敏感，与快照 IO 分离 |
| 使用 SSD | ZK 对写入延迟极敏感，磁盘延迟直接影响事务提交耗时 |
| 关闭磁盘写缓存 | 数据安全优先于性能，`fsync` 必须真正落盘 |

---

## 七、图形化管理

ZK 3.5+ 自带 AdminServer，开启后通过 HTTP 查看集群状态：

```bash
sed -i 's/^admin.enableServer=false/admin.enableServer=true\nadmin.serverPort=8099/' $ZK_HOME/conf/zoo.cfg
systemctl restart zk
```

访问 `http://192.168.5.104:8099/commands`。

> 端口改成 8099 是为了避开 8080。若保持默认 8080，需先确认端口未被占用。

第三方 Web 工具 zkWeb 在新版 JDK 上兼容性较差（需要 JDK 8），JDK 17 环境建议直接用 AdminServer 或 `zkCli.sh`。

---

## 八、排错

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `which java` 报 `no java in (...)` | 未安装 JDK | 安装 `java-17-openjdk` |
| `myid: 没有那个文件或目录` | `dataDir` 未创建 | 先 `mkdir -p` 数据目录 |
| heredoc 卡在 `>` 提示符 | 粘贴时 `EOF` 前的换行丢失 | `Ctrl+C` 退出，改用 `printf` 写文件 |
| `Error contacting service. It is probably not running.` | 只启动一台，仍在选主 | 三台全部启动后再查询 |
| `Address already in use`（8080） | AdminServer 抢占 8080 | `admin.enableServer=false` |
| 三台均为 `Mode: standalone` | myid 或 `server.N` 配置错误 | 核对 myid 与三行 server 配置 |
| 四字命令跨机无输出 | 3.5+ 默认限制本机访问 | 配置 `4lw.commands.whitelist` |
| `ClassNotFoundException` | 下载了源码包而非 `-bin` 包 | 重新下载带 `-bin` 的包 |
| `JAVA_HOME is not set` | systemd 不读 `/etc/profile.d` | unit 中显式声明 `Environment=JAVA_HOME=` |
| 启动后磁盘持续增长 | 未配置 autopurge | 设置 `autopurge.purgeInterval` 与 `snapRetainCount` |
| 重启后 Zxid 错乱、反复选主 | 旧数据未清理 | 停止全部节点，清空 `dataDir` 后重启 |

**判断启动失败与选主等待的区别**：`zkServer.sh status` 的 `Error contacting service` 在多节点未全部启动时属正常；若三台都启动后仍报此错，去看 `zookeeper.out` 末尾，真正的异常堆栈在那里。

---

## 九、卸载

```bash
systemctl stop zk
systemctl disable zk
rm -f /etc/systemd/system/zk.service
systemctl daemon-reload
rm -rf /data/softwares/apache-zookeeper-3.8.4-bin /data/data/zookeeper
rm -f /etc/profile.d/zk.sh
```

**重装前必须彻底删除 `dataDir`。** 残留的 `myid` 与 `version-2/` 目录会带着旧 Zxid 与新集群混合，导致事务 ID 错乱、反复选主。这与 Elasticsearch 重装前必须清空 data 目录是同一类问题——分布式系统的元数据带着"历史"，不能跨实例复用。

---

## 十、验收清单

**集群层**

- [ ] 三台 `ss -lntp` 均有 2181 / 2888 / 3888，无 ZK 占用的 8080
- [ ] 角色分布为 1 Leader + 2 Follower
- [ ] 三台 myid 互不相同，与 `server.N` 对应
- [ ] `systemctl is-enabled zk` = `enabled`

**功能层**

- [ ] 在节点 A 写入的数据，节点 B 能读到
- [ ] `create -e` 创建的临时节点，客户端断开 40~60 秒后自动消失
- [ ] `stat` 显示临时节点 `ephemeralOwner` 非 `0x0`，永久节点为 `0x0`
- [ ] 跨节点能收到 `NodeDataChanged` watch 事件
- [ ] `delete` 非空节点返回 `Node not empty`
- [ ] `echo ruok | nc <ip> 2181` 返回 `imok`

**可用性**

- [ ] 停止 1 台，集群仍可读写
- [ ] 停止 2 台，集群拒绝写入
- [ ] 恢复节点后自动重新选主

**调优**

- [ ] `ps -ef | grep Xmx` 显示 `-Xmx512m`
- [ ] `/etc/logrotate.d/zookeeper` 已配置

---

## 十一、参考

- [ZooKeeper 3.8 官方文档](https://zookeeper.apache.org/doc/r3.8.4/index.html)
- [ZooKeeper 配置参数](https://zookeeper.apache.org/doc/r3.8.4/zookeeperAdmin.html#sc_configuration)
- [四字命令与监控](https://zookeeper.apache.org/doc/r3.8.4/zookeeperAdmin.html#sc_4lw)
