# Kafka

Kafka 3.7.1 集群的操作记录，依赖 ZooKeeper 提供元数据存储。

| 文档 | 内容 | 环境 |
| :--- | :--- | :--- |
| [Topic 管理实操](./Kafka-3.7.1-Topic管理实操.md) | 创建、查看、改分区、改配置、改副本数、删除、巡检 | Kafka 3.7.1 · ZK 3.8.4 · 3 broker |

---

## 前提

本文假设：

- ZooKeeper 三节点已就绪，`2181` 可连
- 三个 broker 已注册，`broker.id` = 1 / 2 / 3
- ZK 上使用了 chroot 路径 `/kafka-3.7.1`

确认 broker 全部在线：

```bash
kafka-broker-api-versions.sh --bootstrap-server 192.168.5.101:9092 2>/dev/null | grep -c "id:"
```

输出 `3` 才继续。**broker 没全起来时做 topic 操作，副本数和分区分布会和预期不符，容易误判。**

---

## 几个容易踩的点

**副本数不能超过存活 broker 数**

三台 broker 建 `--replication-factor 5` 直接报错。因为同一分区的副本不能落在同一台机器上，否则那台一挂分区就没了。

**分区只能加不能减**

想把分区从 5 改回 3 会报 `InvalidPartitionsException`。减少分区会让 `hash(key) % 分区数` 的结果改变，同一 key 的消息跑到别的分区，有序性前提就崩了。所以分区数宁少勿多。

**`Isr` 少一个就是有问题**

`--describe` 里 `Replicas` 和 `Isr` 应当完全一致。出现 `Replicas: 1,2 Isr: 1` 说明 broker 2 上的副本掉队了，此时 leader 再挂就丢数据。

**改副本数不能用 `kafka-topics.sh`**

`--alter` 不支持改副本数，要用 `kafka-reassign-partitions.sh` 走重分配流程。

**删了 topic 磁盘空间不会立刻释放**

`log.segment.delete.delay.ms` 默认 60 秒，数据文件延迟清理。

---

## 巡检两条命令

```bash
# 副本不齐的分区，输出为空即健康
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --under-replicated-partitions

# 无 leader 的分区，出现就是严重故障
kafka-topics.sh --bootstrap-server 192.168.5.101:9092 --describe --unavailable-partitions
```

---

## 参考

- [Kafka 3.7 官方文档](https://kafka.apache.org/documentation/#operations)
- [Topic 配置项完整列表](https://kafka.apache.org/documentation/#topicconfigs)
