# ZooKeeper 3.8.4 三节点集群搭建记录

> 2026-10-18 实际操作记录。三台 Rocky Linux 9 虚拟机，从零到集群成形。
> 按实际执行顺序写，包括中途卡住的地方和解决办法。

## 环境

| 项 | 值 |
| :--- | :--- |
| OS | Rocky Linux 9 |
| ZooKeeper | 3.8.4 |
| JDK | openjdk 17（dnf 安装） |
| 节点 | 3 台，全部 participant |
| myid | 1 / 2 / 3 |
| 软件目录 | `/data/softwares` |
| 数据目录 | `/data/data/zookeeper` |

---

## 1. 建目录

三台都执行：

```bash
mkdir -p /data/softwares /data/data/zookeeper/txn
ls -d /data/softwares /data/data/zookeeper
```

`ls` 必须有两条输出。这里我第一次漏了，后面写 myid 报 `没有那个文件或目录`。

腾内存（这台机器上跑着 grafana、prometheus、netbox 等残留服务）：

```bash
systemctl stop netbox netbox-rq grafana-server prometheus zabbix-mcp \
               php-fpm postgresql redis nginx
free -m
```

只 stop 不 disable，重启会自己回来。

---

## 2. 下载解压

```bash
cd /data/softwares
curl -L -O https://archive.apache.org/dist/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz
tar xf apache-zookeeper-3.8.4-bin.tar.gz
```

必须下 `-bin` 包。源码包里没有 `lib/*.jar`，启动报 `ClassNotFoundException`。

---

## 3. 装 JDK

先确认有没有：

```bash
java -version
```

这台机器上执行报 `no java in (...)`，等于没装。之前我以为有 Java，是看了另一份记录里的 `openjdk 22`，那是别的环境的。

```bash
dnf install -y java-17-openjdk java-17-openjdk-devel
java -version
```

dnf 源不通的话用离线包：

```bash
cd /data/softwares
curl -L -O https://mirrors.tuna.tsinghua.edu.cn/Adoptium/17/jdk/x64/linux/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz
tar xf OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz
ls -d /data/softwares/jdk-17*

ln -sfn /data/softwares/jdk-17.0.13+11/bin/java /usr/local/bin/java
ln -sfn /data/softwares/jdk-17.0.13+11/bin/javac /usr/local/bin/javac
```

软链名字按 `ls` 实际输出改。

---

## 4. 环境变量

```bash
printf 'export JAVA_HOME=%s\n' "$(dirname $(dirname $(readlink -f $(which java))))" > /etc/profile.d/zk.sh
printf 'export ZK_HOME=/data/softwares/apache-zookeeper-3.8.4-bin\n' >> /etc/profile.d/zk.sh
printf 'export PATH=$PATH:${ZK_HOME}/bin\n' >> /etc/profile.d/zk.sh

source /etc/profile.d/zk.sh
echo "JAVA_HOME = $JAVA_HOME"
zkServer.sh version
```

`JAVA_HOME` 应该有值，然后打印出 `ZooKeeper version: 3.8.4-...`。

用 `printf` 而不是 `cat <<EOF`，是因为从聊天窗口粘贴 heredoc 时 `EOF` 前的换行经常丢，shell 会卡在 `>` 提示符。

记下 `JAVA_HOME` 的值，第 6 步要用。

---

## 5. 写 zoo.cfg

三台内容完全一样：

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

改动说明：

- `admin.enableServer=false` —— ZK 3.5+ 默认开 AdminServer 占 8080，这台 8080 已被别的服务占用，不改起不来
- `autopurge.*` —— 不配这两行磁盘会被旧快照吃满
- `server.N` 里的 N 就是该节点的 myid

---

## 6. 写 myid

三台不同：

```bash
# .104
mkdir -p /data/data/zookeeper/txn
echo 1 > /data/data/zookeeper/myid
cat /data/data/zookeeper/myid

# .105
mkdir -p /data/data/zookeeper/txn
echo 2 > /data/data/zookeeper/myid
cat /data/data/zookeeper/myid

# .106
mkdir -p /data/data/zookeeper/txn
echo 3 > /data/data/zookeeper/myid
cat /data/data/zookeeper/myid
```

myid 放在 `dataDir` 根下，不是 `txn` 子目录。三台不能重复。

---

## 7. 写 systemd unit

`JAVA_HOME` 换成第 4 步得到的值：

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

`Environment=JAVA_HOME` 不能少，systemd 不读 `/etc/profile.d`，缺了报 `JAVA_HOME is not set`。值不加引号。

---

## 8. 启动

三台都要起：

```bash
systemctl start zk
sleep 3
zkServer.sh status
```

第一台执行后输出：

```
ZooKeeper JMX enabled by default
Using config: /data/softwares/apache-zookeeper-3.8.4-bin/bin/../conf/zoo.cfg
Client port found: 2181. Client address: localhost. Client SSL: false.
Error contacting service. It is probably not running.
```

**这句不是启动失败**，是在等选主。只起一台凑不齐多数票，`status` 拿不到身份。

确认进程在跑：

```bash
ss -lntp | grep -E '2181|2888|3888'
ps -ef | grep -v grep | grep zookeeper
tail -30 $ZK_HOME/logs/zookeeper.out
```

日志末尾是 `LOOKING` 就正常。

三台都起完再查：

```bash
zkServer.sh status
```

结果应该是：

```
Mode: leader       # 一台
Mode: follower     # 两台
```

如果三台都是 `Mode: standalone`，检查 myid 和三行 `server.N`。

---

## 9. 验证

### 9.1 端口

```bash
ss -lntp | grep -E '2181|2888|3888'
```

三个都在，且没有 ZK 占的 8080。

### 9.2 基本读写

```bash
zkCli.sh -server 192.168.5.104:2181
```

```bash
ls /
# [zookeeper]

create /zzz xixi
get /zzz                          # xixi

create /zzz/node1
get /zzz/node1                    # null

set /zzz/node1 haha
get /zzz/node1                    # haha

create -e /zzz/node2              # 临时节点
ls /zzz                           # [node1, node2]
```

### 9.3 临时节点与永久节点

```bash
stat /zzz/node1
# ephemeralOwner = 0x0          永久

stat /zzz/node2
# ephemeralOwner = 0x5d...      临时
```

### 9.4 删除

```bash
create /zzz/node1/1
create /zzz/node1/2
delete /zzz/node1
# Node not empty: /zzz/node1     非空删不掉

deleteall /zzz/node1              # 递归删除
```

### 9.5 跨节点一致

换到 .105：

```bash
zkCli.sh -server 192.168.5.105:2181
get /zzz        # 能读到 .104 写的数据
```

能读到才算集群成形。前面几步只能证明单个 ZK 能跑。

### 9.6 临时节点自动消失

在 .104 的客户端 `quit`，等 40~60 秒，从 .106 连：

```bash
zkCli.sh -server 192.168.5.106:2181
ls /zzz         # node2 已消失
```

### 9.7 watch

终端 A（.104）：

```bash
get -w /zzz
ls -w /zzz
```

终端 B（.105）：

```bash
set /zzz hello
```

终端 A 输出：

```
WATCHER::
WatchedEvent state:SyncConnected type:NodeDataChanged path:/zzz
```

watch 只触发一次，要继续监听得重新 `-w`。

### 9.8 四字命令

```bash
dnf install -y nmap-ncat

echo ruok | nc 192.168.5.104 2181     # imok
echo srvr | nc 192.168.5.104 2181
echo conf | nc 192.168.5.104 2181
echo mntr | nc 192.168.5.104 2181
```

跨机执行没输出（不是报错，是空白）：3.5+ 默认只允许 127.0.0.1 访问。

```bash
echo '4lw.commands.whitelist=*' >> $ZK_HOME/conf/zoo.cfg
systemctl restart zk
```

### 9.9 停一台

```bash
ssh root@192.168.5.105 'systemctl stop zk'

# 在 .104 确认
zkServer.sh status
zkCli.sh -server 192.168.5.104:2181    # 仍能读写

ssh root@192.168.5.105 'systemctl start zk'
```

停两台（剩 1/3）时集群拒绝写入，恢复一台后自动重新选主。

---

## 10. 调优

```bash
grep ^ZK_SERVER_HEAP $ZK_HOME/bin/zkEnv.sh
# ZK_SERVER_HEAP="${ZK_SERVER_HEAP:-1000}"

sed -i '/^ZK_SERVER_HEAP/s#1000#512#' $ZK_HOME/bin/zkEnv.sh
grep ^ZK_SERVER_HEAP $ZK_HOME/bin/zkEnv.sh

systemctl restart zk
ps -ef | grep zookeeper | grep -i xmx     # -Xmx512m
```

这台机器 3.6 G 内存还要跑 ES，所以给 512。内存提到 8 G 再改 1024~2048。

日志轮转：

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

---

## 11. 图形化（可选）

用 ZK 自带的 AdminServer，端口改 8099 避开 8080：

```bash
sed -i 's/^admin.enableServer=false/admin.enableServer=true\nadmin.serverPort=8099/' $ZK_HOME/conf/zoo.cfg
systemctl restart zk
```

访问 `http://192.168.5.104:8099/commands`。

zkWeb 那个 jar 没跑起来，JDK 17 起不来它，要装 JDK 8。没继续折腾。

---

## 遇到的问题

| 现象 | 原因 | 处理 |
| :--- | :--- | :--- |
| `which java` 报 `no java in (...)` | 机器没装 JDK | 装 java-17-openjdk |
| `myid: 没有那个文件或目录` | `dataDir` 没建 | 补 `mkdir -p /data/data/zookeeper/txn` |
| heredoc 卡在 `>` 提示符 | 粘贴时 `EOF` 前换行丢了 | `Ctrl+C` 退出，改用 `printf` 写文件 |
| `Error contacting service. It is probably not running.` | 只起一台，还在选主 | 把三台都起起来再查 |
| `Address already in use`（8080） | AdminServer 抢 8080，已被占用 | `admin.enableServer=false` |
| 三台都是 `Mode: standalone` | myid 或 `server.N` 配错 | 核对 myid 和三行 server 配置 |
| 四字命令跨机无输出 | 3.5+ 默认只允许本机 | 加 `4lw.commands.whitelist` |
| `ClassNotFoundException` | 下成了源码包 | 重下 `-bin` 包 |

---

## 验收清单

- [ ] 三台 `ss -lntp` 有 2181 / 2888 / 3888，无 ZK 占的 8080
- [ ] 1 台 leader + 2 台 follower
- [ ] .104 写的数据 .105 能读到
- [ ] `create -e` 断开后 40~60 秒自动消失
- [ ] 临时节点 `ephemeralOwner` 非 0，永久节点为 `0x0`
- [ ] 跨节点能收到 `NodeDataChanged`
- [ ] `echo ruok | nc <ip> 2181` 返回 `imok`
- [ ] 停 1 台仍可读写，停 2 台拒绝写入
- [ ] `ps -ef | grep Xmx` 看到 `-Xmx512m`

---

## 卸载

```bash
systemctl stop zk
systemctl disable zk
rm -f /etc/systemd/system/zk.service
systemctl daemon-reload
rm -rf /data/softwares/apache-zookeeper-3.8.4-bin /data/data/zookeeper
rm -f /etc/profile.d/zk.sh
```

重装前必须删干净 `dataDir`，留着旧 `myid` 和 `version-2/` 会出 Zxid 错乱。

---

## 关键配置

```ini
tickTime=2000               # 心跳单位 ms，下面两个是它的倍数
initLimit=10                # 初始化超时 10 × 2s = 20s
syncLimit=5                 # 同步超时 5 × 2s = 10s

dataDir=/data/data/zookeeper        # 快照 + myid
dataLogDir=/data/data/zookeeper/txn # 事务日志
clientPort=2181

server.1=192.168.5.104:2888:3888    # N=myid，2888 同步，3888 选主
server.2=192.168.5.105:2888:3888
server.3=192.168.5.106:2888:3888

admin.enableServer=false
maxClientCnxns=200
autopurge.snapRetainCount=5
autopurge.purgeInterval=24
```

---

## 下一步

- Kafka 3.7.1 三节点，`zookeeper.connect` 指到这四台 ZK 的 2181
- Zabbix 接 `mntr` 指标监控集群

> 环境：Rocky Linux 9，ZooKeeper 3.8.4，JDK 17，3 节点全 participant
