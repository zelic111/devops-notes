# Kafka 3.7.1 Topic 管理实操

> 前提：ZooKeeper 三节点集群已就绪（2181 可连）。Kafka 三节点 broker 已部署，`broker.id` = 1 / 2 / 3。
> 本文只讲 topic 的增删改查，从零到能熟练管理。
>
> 示例地址用 `192.168.5.101 / .102 / .103`（对应 `kafka1 / kafka2 / kafka3`），替换为自己的地址即可。

---

## 环境约定

| 项 | 值 |
| :--- | :--- |
| Kafka | 3.7.1（`kafka_2.13-3.7.1`） |
| ZooKeeper | 3.8.4，三节点 `:2181` |
| broker | 3 台，`broker.id` = 1 / 2 / 3 |
| broker 端口 | 9092 |
| ZK chroot | `/kafka-3.7.1` |
| 软件路径 | `/data/softwares/kafka_2.13-3.7.1` |

先确认 Kafka 环境变量已生效：

```bash
echo $KAFKA_HOME
kafka-topics.sh --version
```

第一条要输出路径，第二条输出 `3.7.1`。都没有就补环境变量：

```bash
printf 'export KAFKA_HOME=/data/softwares/kafka_2.13-3.7.1\n' > /etc/profile.d/kafka.sh
printf 'export PATH=$PATH:${KAFKA_HOME}/bin\n' >> /etc/profile.d/kafka.sh
source /etc/profile.d/kafka.sh
```

---

## 0. 动手前先确认集群活着

topic 操作的命令本身很简单，但**如果 broker 没全起来，后面的副本数、分区分布全会和预期不一样**，容易得出错误结论。先花三十秒确认：

```bash
kafka-broker-api-versions.sh --bootstrap-server 192.168.5.101:9092 2>/dev/null | grep -c "id:"
```

预期输出 `3`。少于 3 台就先别往下做。

看集群元数据是否正常：

```bash
kafka-metadata-quorum.sh --bootstrap-server 192.168.5.101:9092 describe --status 2>/dev/null \
  || zookeeper-shell.sh 192.168.5.101:2181 ls /kafka-3.7.1/brokers/ids
```

ZK 里能看到 `[1, 2, 3]` 说明三个 broker 都注册上了。

---

## 1. 查看 topic 列表

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --list
```

刚装好的集群是空的，什么都不输出。也可以看到 Kafka 自建的内部 topic：

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --list | grep '^__'
```

`__consumer_offsets`（记录消费位置）和 `__transaction_state`（事务）是系统 topic，**不要动**。

---

## 2. 创建 topic

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --create \
  --topic zzz-topic \
  --partitions 3 \
  --replication-factor 2
```

输出 `Created topic zzz-topic.` 即成功。

### 两个参数怎么定

| 参数 | 含义 | 怎么定 |
| :--- | :--- | :--- |
| `--partitions` | 分区数，决定并行度 | 取 **下游消费者最大并发数** 的整数倍。**只能调大不能调小**，宁少勿多 |
| `--replication-factor` | 副本数，决定容错 | 生产用 `3`（容忍 1 台 broker 挂）；学习环境 3 台可用 `2` |

**副本数不能大于存活的 broker 数量。** 三台 broker 建 `--replication-factor 5` 会直接失败：

```
Error while executing topic command : Replication factor: 5 larger than available brokers: 3.
ERROR org.apache.kafka.common.errors.InvalidReplicationFactorException:
  Replication factor: 5 larger than available brokers: 3.
```

原因是同一分区的多个副本**不能落在同一个 broker 上**，否则那台机器一挂，整个分区就没了，副本就失去意义。

### 创建时就指定参数（避免事后改）

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --create \
  --topic zzz-topic-log \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=604800000 \
  --config max.message.bytes=2097152
```

`retention.ms=604800000` 是 7 天（默认也是 7 天）；`max.message.bytes` 默认 1 MB，日志类主题经常需要调大。

---

## 3. 查看 topic 详情

### 全部 topic

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe
```

输出：

```
Topic: zzz-topic	TopicId: v3h4QOctS7G-bI5NB_n5zA	PartitionCount: 3	ReplicationFactor: 2	Configs:
	Topic: zzz-topic	Partition: 0	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: zzz-topic	Partition: 1	Leader: 2	Replicas: 2,3	Isr: 2,3
	Topic: zzz-topic	Partition: 2	Leader: 3	Replicas: 3,1	Isr: 3,1
```

### 指定 topic

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --topic zzz-topic
```

### 四列的含义

| 列 | 含义 |
| :--- | :--- |
| `Leader` | 该分区当前的主副本所在 broker id，**读写都走它** |
| `Replicas` | 该分区的全部副本（含 leader），第一个是优先副本 |
| `Isr` | **In-Sync Replicas**，与 leader 保持同步的副本集合 |
| `Configs` | topic 级配置，没设就是空 |

**`Isr` 是判断健康的核心**。`Replicas` 和 `Isr` 一致才算全部正常。如果出现：

```
Replicas: 1,2	Isr: 1
```

说明 broker 2 上的副本掉队了（进程挂了、网络不通、或者同步超时被踢出）。生产环境这种情况要立刻查——一旦 leader 也挂，数据就丢了。

### 看分区分布是否均衡

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --topic zzz-topic \
  | awk 'NR>1 {print $4}' | sort | uniq -c
```

统计每个 broker 当了几个分区的 leader。三个 broker 应该接近均分，如果全压在一台上，说明分区数设得太少或者分配不均。

---

## 4. 修改 topic

### 增加分区

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --alter --topic zzz-topic --partitions 5
```

成功时**没有任何输出**，再 `--describe` 就能看到变成 5 个分区，新增的 3、4 号分区会重新分配到各 broker：

```
PartitionCount: 5
	Partition: 3	Leader: 1	Replicas: 1,3
	Partition: 4	Leader: 2	Replicas: 2,1
```

### 分区只能加不能减

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --alter --topic zzz-topic --partitions 3
```

```
Error while executing topic command : Topic currently has 5 partitions, which is higher than the requested 3.
ERROR org.apache.kafka.common.errors.InvalidPartitionsException:
  Topic currently has 5 partitions, which is higher than the requested 3.
```

**这不是 bug，是设计约束。** 分区数减少会导致原本按 `hash(key) % 分区数` 落位的消息**换了归属分区**，同一 key 的历史消息和新消息落在不同分区上，"同一 key 的消息保证有序"这个前提就崩了。

所以分区数宁少勿多，估不准就先给个保守值，后面再加。

### 修改 topic 级配置

```bash
kafka-configs.sh --bootstrap-server 192.168.5.101:9092 \
  --entity-type topics --entity-name zzz-topic \
  --alter --add-config retention.ms=86400000
```

改成 1 天。查当前配置：

```bash
kafka-configs.sh --bootstrap-server 192.168.5.101:9092 \
  --entity-type topics --entity-name zzz-topic --describe
```

删除某项配置（回落到 broker 默认值）：

```bash
kafka-configs.sh --bootstrap-server 192.168.5.101:9092 \
  --entity-type topics --entity-name zzz-topic \
  --alter --delete-config retention.ms
```

### 修改副本数

`kafka-topics.sh` 的 `--alter` **不支持改副本数**，必须走重分配：

```bash
# 1. 生成重分配方案，把副本数提到 3
cat > /tmp/zzz-topic-reassign.json <<'EOF'
{"version":1,"partitions":[
  {"topic":"zzz-topic","partition":0,"replicas":[1,2,3]},
  {"topic":"zzz-topic","partition":1,"replicas":[2,3,1]},
  {"topic":"zzz-topic","partition":2,"replicas":[3,1,2]}
]}
EOF

# 2. 执行
kafka-reassign-partitions.sh --bootstrap-server 192.168.5.101:9092 \
  --reassignment-json-file /tmp/zzz-topic-reassign.json --execute

# 3. 查看进度
kafka-reassign-partitions.sh --bootstrap-server 192.168.5.101:9092 \
  --reassignment-json-file /tmp/zzz-topic-reassign.json --verify
```

`--verify` 输出 `completed` 才算完成。数据量大的话要等一会儿。

---

## 5. 删除 topic

```bash
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --delete --topic zzz-topic
```

**没有输出就是成功**。再 `--list` 确认已消失。

### 数据不是立刻删的

topic 标记删除后，磁盘上的数据文件还要等一段时间才真正清理：

```
log.segment.delete.delay.ms   默认 60000（1 分钟）
file.delete.delay.ms          默认 60000
```

所以刚删完 `du -sh` 看数据目录，文件还在，这是正常的。想立刻腾空间可以临时把 `log.segment.delete.delay.ms` 调小再删。

### 如果删了还在

检查 `delete.topic.enable` 是否为 `true`（3.x 默认就是 true，早版本默认 false）：

```bash
kafka-configs.sh --bootstrap-server 192.168.5.101:9092 \
  --entity-type brokers --entity-name 1 --describe --all | grep delete.topic
```

某些情况下 topic 会卡在 `marked for deletion`，需要重启对应 broker 才能完成清理。

---

## 6. 常用检查命令

| 目的 | 命令 |
| :--- | :--- |
| 每个分区的消息条数 | `kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list 192.168.5.101:9092 --topic zzz-topic` |
| 磁盘占用 | `du -sh /data/data/kafka-logs/zzz-topic-*` |
| broker 上所有 topic 的磁盘占用 | `kafka-log-dirs.sh --bootstrap-server 192.168.5.101:9092 --describe --topic-list zzz-topic` |
| 集群下所有 topic 的分布总览 | `kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --under-replicated-partitions` |
| 有问题的分区（副本不齐） | `kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --under-replicated-partitions` |
| 无 leader 的分区（严重） | `kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --unavailable-partitions` |

`--under-replicated-partitions` 和 `--unavailable-partitions` 这两条是日常巡检最该用的，**输出为空就说明集群健康**。有输出就要立刻查。

---

## 7. 完整练一遍

按顺序执行，观察每一步的变化：

```bash
# 建立两个 topic
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --create --topic zzz-topic --partitions 3 --replication-factor 2
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --create --topic zzz-topic-001 --partitions 3 --replication-factor 2

# 看列表
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --list

# 看详情
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --topic zzz-topic

# 副本数超限，预期报错
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --create --topic zzz-topic-002 --partitions 3 --replication-factor 5

# 分区 3 → 5
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --alter --topic zzz-topic --partitions 5
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --topic zzz-topic

# 分区 5 → 3，预期报错
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 \
  --alter --topic zzz-topic --partitions 3

# 删除
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --delete --topic zzz-topic
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --list
```

做完应该只剩 `zzz-topic-001`。

---

## 遇到的问题

| 现象 | 原因 | 处理 |
| :--- | :--- | :--- |
| `Replication factor: N larger than available brokers: M` | 副本数大于存活 broker 数 | 减小副本数；或先把没起的 broker 拉起来 |
| `Topic currently has N partitions, which is higher than the requested M` | 想把分区改小 | 不支持。重建 topic 并迁移数据 |
| `--alter` 无输出 | 正常，成功时不打印 | 用 `--describe` 确认 |
| `--delete` 后 `--list` 还在 | 卡在 marked for deletion | 查 `delete.topic.enable`；必要时重启 broker |
| 删了 topic 但磁盘没降 | `log.segment.delete.delay.ms` 默认 60 秒 | 等一会儿，或调小该参数 |
| `--bootstrap-server` 连不上 | broker 没起 / 端口不对 / 防火墙 | 先 `kafka-broker-api-versions.sh` 验证 |
| 副本数改不了 | `kafka-topics.sh` 不支持 | 用 `kafka-reassign-partitions.sh` |
| topic 创建成功但客户端收发不到 | 客户端连的是 `localhost`，broker 宣告的地址不可达 | 检查 `advertised.listeners` |

---

## 参数速查

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `num.partitions` | 1（broker 默认） | 创建时不指定 `--partitions` 就用这个 |
| `default.replication.factor` | 1 | 创建时不指定副本数就用这个 |
| `retention.ms` | 604800000（7 天） | 消息保留时间 |
| `retention.bytes` | -1（不限） | 按容量保留，超过删最旧的 |
| `max.message.bytes` | 1048588（约 1 MB） | 单条消息上限 |
| `segment.bytes` | 1073741824（1 GB） | 单个日志段大小 |
| `cleanup.policy` | delete | 可改 `compact`（按 key 保留最新值） |
| `min.insync.replicas` | 1 | 写入最少要几个副本确认，生产建议设为副本数 - 1 |

> `min.insync.replicas` 值得单独说：副本数 3、这个值 1 时，写入只要 leader 确认就算成功，另外两个副本挂了照样能写，但**数据实际只有一份**。生产环境应设为 2，配合生产端 `acks=all`，才能真正做到"至少两份落盘"。

---

## 下一步

- 生产者和消费者命令行（`kafka-console-producer.sh` / `kafka-console-consumer.sh`）
- 消费者组与 offset（`kafka-consumer-groups.sh`）
- Rebalance 机制

> 环境：Kafka 3.7.1 + ZooKeeper 3.8.4 三节点，Rocky Linux 9
