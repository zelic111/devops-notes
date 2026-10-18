# 从零搭建 Elasticsearch 7.17 三节点集群：原理、配置与 API 实战

## 开篇

日志分散在几十台机器的本地磁盘上，排查问题只能逐台 ssh 上去 grep，日志一多既存不住也留不久——这是 ElasticStack 要解决的问题。完整的日志平台链路是：

```
Filebeat（采集） → Kafka + ZK（缓冲削峰） → Logstash（解析加工） → Elasticsearch（存储检索） → Kibana（展示）
```

按规模分为三种架构：

| 架构 | 组成 | 适用场景 |
| --- | --- | --- |
| EFK | Filebeat + ES + Kibana | 小规模，日志无需复杂清洗 |
| ELFK | Filebeat + Logstash + ES + Kibana | 需要解析、格式化日志 |
| ELFK + Kafka | 追加消息队列 | 大规模、写入峰值高，需要缓冲 |

本文只聚焦最底层的 **Elasticsearch**——它是存储与查询的底座，上游向它写入，下游从它读取。底座不稳，后续接入采集与展示都是空谈。

本文覆盖的内容：三节点集群的部署原理与配置、索引与文档 API、批量操作、映射与查询入门，以及实际踩过的坑。文中命令已在 Rocky Linux 9.8 + ES 7.17.22 上验证，Debian 系只需替换包管理命令。

---

## 一、核心概念

### 1.1 术语

| 术语 | 类比 | 说明 |
| --- | --- | --- |
| **索引 index** | 数据库的库 | 一类文档的集合，名称必须小写 |
| **分片 shard** | 数据切块 | 索引被切成的份数，主分片数建库时定死，不可修改 |
| **副本 replica** | 分片的备份 | 可随时调整，**不能与主分片位于同一节点** |
| **文档 document** | 一行记录 | JSON 格式，ES 中最小数据单元 |
| **映射 mapping** | 表结构 | 字段类型定义，可动态推断也可显式指定 |
| **倒排索引** | 书的目录 | ES 检索快的根因：存"词 → 文档"的映射，而非逐行扫描 |

### 1.2 分片与副本

| 角色 | 职责 |
| --- | --- |
| **primary shard**（主分片） | 数据的读写入口，写操作先落主分片 |
| **replica shard**（副本分片） | 从主分片同步数据，承担读负载均衡，主分片故障时顶上 |
| **allocation**（分配） | 将索引的分片分配到集群各节点的过程，由 master 统一决策 |

一个主分片的数据只存在一份，副本可以有多个（`number_of_replicas: 2` 表示每个主分片有两个副本）。

### 1.3 文档的构成

| 部分 | 说明 |
| --- | --- |
| **源数据 `_source`** | 用户写入的原始 JSON 全文 |
| **元数据** | 描述数据的数据：`_index`、`_id`、`_version`、`_seq_no`、`_source` 等 |

`_source` 保存原始 JSON 全文，同时 ES 为其建立倒排索引用于检索。这是存储放大系数通常在 1.5~3 倍的原因。

### 1.4 两个端口

| 端口 | 协议 | 用途 | 调用方 |
| --- | --- | --- | --- |
| **9200** | HTTP / HTTPS | 对外提供 REST API | 客户端、curl、Kibana、es-head |
| **9300** | TCP（ES 私有二进制协议） | 集群内部数据传输、选主、分片同步 | 仅 ES 节点之间 |

HTTP 客户端只能连接 9200，连接 9300 会得到乱码或直接断开。

---

## 二、架构设计：动手前的决策

### 2.1 目标

对外只暴露一个 `9200` 入口，对内只走 `9300` 通道。客户端将请求发送给任意节点，无需关心数据实际所在位置。

因此本次任务不是"在三台机器上各装一个 ES"，而是**组建一个集群**——安装是手段，成形才是目的。

### 2.2 三个前置决策

| 决策 | 备选 | 选择 | 理由 |
| --- | --- | --- | --- |
| **版本** | 7.17.x / 8.x / 9.x | 7.17.x | 7.x 默认关闭 security，无需证书与 enroll 流程，学习集群机制时干扰最小 |
| **拓扑** | 全角色 / 拆分专用 master | 全角色（master-eligible + data） | 仅三台节点时，拆分专用 master 至少需三台专职，剩余节点无法承载数据 |
| **路径** | 先单点后转集群 / 直接建集群 | 直接建集群 | 单点模式使用 `discovery.type: single-node`，转集群必须清空数据目录，否则将复现脑裂 |

### 2.3 集群发现四参数

| 参数 | 回答的问题 |
| --- | --- |
| `cluster.name` | 是否属于同一集群（集群名不同则彼此隔离） |
| `node.name` | 我是谁（各节点必须唯一） |
| `discovery.seed_hosts` | 启动时向谁询问当前成员列表 |
| `cluster.initial_master_nodes` | 首次选主时谁的投票有效（填 `node.name`，非 IP） |

前两者决定"是否同一家族"，后两者决定"如何找到彼此、首次由谁主导"。**四个参数全部正确才构成一个集群，任一错误都会退化为三个独立单点。**

### 2.4 执行顺序

```
备条件 → 安装 → 配置 → 启动 → 验证 → 扩容
```

- **配置必须在启动前**：ES 启动时一次性读取配置，修改后必须重启，先启动再改配置无法生效
- **验证必须在扩容前**：先确认现有节点已成形再加入新节点，否则故障归因困难
- **扩容置于最后**：新增节点不影响已上线节点的可用性

### 2.5 验收标准

| 层次 | 判定依据 |
| --- | --- |
| 端口监听 | `9200`、`9300` 均处于 LISTEN |
| 身份正确 | `curl :9200` 返回的 `cluster_name`、`node.name` 符合预期 |
| 成员完整 | `_cat/nodes` 包含全部节点，且有一个 `*` 标识当前 master |
| 分片健康 | `_cluster/health` 为 `green`，或为 `yellow` 且原因可解释 |

端口监听只能证明进程存活，无法证明节点同属一个集群——这是最易误判的一环。

### 2.6 两个预期现象

1. **两节点时，带副本的索引为 `yellow`**：副本不能与主分片同节点，无处分配。这不是故障，而是副本机制的正常反馈
2. **第三节点未上线时集群仍可用**：三节点存活两台，选主所需多数票（2/3）依然满足

---

## 三、集群部署

### 3.1 安装

两种发行版的安装路径完全一致（`/etc/elasticsearch`、`/var/lib/elasticsearch`、`/var/log/elasticsearch`），仅包管理命令不同：

| 发行版 | 包格式 | 安装 | 卸载 |
| --- | --- | --- | --- |
| Ubuntu / Debian | `.deb` | `dpkg -i` | `dpkg -r elasticsearch` |
| RHEL / Rocky / CentOS | `.rpm` | `rpm -ivh` | `rpm -e elasticsearch` |

```bash
curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.22-x86_64.rpm
rpm -ivh elasticsearch-7.17.22-x86_64.rpm
```

安装完成后的提示明确说明**不会自动启动**：

```
### NOT starting on installation, please execute the following statements to
### configure elasticsearch service to start automatically using systemd
 sudo systemctl daemon-reload
 sudo systemctl enable elasticsearch.service
```

RPM 包自带 JDK，无需额外安装 Java：

```bash
/usr/share/elasticsearch/jdk/bin/java -version
```

### 3.2 内核参数：先检查，后修改

ES 依赖 Lucene 的 mmap 读取文件，`vm.max_map_count` 不足时 bootstrap check 会拒绝启动。但**安装包通常已自动完成配置**：

```bash
sysctl vm.max_map_count
```

| 返回值 | 处理 |
| --- | --- |
| `262144` | 已满足要求，跳过 |
| `65530` | 需要修改，见下文 |

确认安装包是否已下发配置：

```bash
cat /usr/lib/sysctl.d/elasticsearch.conf          # 内容为 vm.max_map_count=262144
grep Limit /usr/lib/systemd/system/elasticsearch.service   # LimitNOFILE=65535 / LimitNPROC=4096
```

关于 `sysctl.d` 的两个要点：

- `/etc/sysctl.d/` 是 **drop-in 目录**，默认为空。系统启动时 `systemd-sysctl` 按文件名排序依次加载 `/usr/lib/sysctl.d/`、`/run/sysctl.d/`、`/etc/sysctl.d/` 下的 `*.conf`，同名参数以 `/etc` 下的为准
- 因此标准做法是**新增独立配置文件**，而非向现有文件追加内容，卸载时删除即可恢复干净

若需自行配置，**不要使用 `99-elasticsearch.conf`**——它排序在 `99-sysctl.conf` 之前（`e` < `s`），若后者包含同名参数将覆盖你的设置。应使用排序靠后的文件名：

```bash
cat > /etc/sysctl.d/99-zz-elasticsearch.conf <<'EOF'
vm.max_map_count = 262144
EOF
sysctl --system
```

同理，systemd unit 已包含 `LimitNOFILE=65535`，无需额外创建 `limits.d` 配置。

### 3.3 配置文件

三台节点各一份，**`node.name` 与 `network.publish_host` 各不相同**：

| 主机 | IP | `node.name` | `network.publish_host` |
| --- | --- | --- | --- |
| node1 | 192.168.5.104 | `elk1` | `192.168.5.104` |
| node2 | 192.168.5.105 | `elk2` | `192.168.5.105` |
| node3 | 192.168.5.106 | `elk3` | `192.168.5.106` |

```bash
cp /etc/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml.bak
cat > /etc/elasticsearch/elasticsearch.yml <<'EOF'
cluster.name: my-es
node.name: elk1

path.data: /var/lib/elasticsearch
path.logs: /var/log/elasticsearch

network.host: 0.0.0.0
network.publish_host: 192.168.5.104
http.port: 9200
transport.port: 9300

discovery.seed_hosts: ["192.168.5.104:9300", "192.168.5.105:9300", "192.168.5.106:9300"]
cluster.initial_master_nodes: ["elk1", "elk2", "elk3"]
EOF
```

### 3.4 必须显式配置 publish_host

`network.host: 0.0.0.0` 仅表示"监听所有网卡"，ES 仍需**自行选择一个地址向其他节点宣告**。选择过程中不区分业务网卡与 overlay 网卡。当宿主机存在 flannel、docker 等虚拟网卡时：

```bash
ip -4 -o addr show | awk '{print $2, $4}'
# ens34      192.168.5.104/24
# flannel.1  10.244.1.0/32      ← 子网网络地址，flannel 按此方式分配
```

未配置 `publish_host` 时，ES 可能选中 `10.244.x.0`。此时集群仍可达到 `green`（overlay 网络内部互通），但会带来以下问题：

- 该地址无法 ssh 登录，排障时容易误判
- Pod 网段仅在集群内可路由，集群外客户端无法连接
- 节点间复制流量额外绕行一层 VXLAN
- 更换网络插件（如 flannel → calico）后地址变化，集群可能无法启动

因此每台节点都需显式指定业务地址。

### 3.5 堆内存必须显式设置

7.17 的 `jvm.options` **未固定堆大小**（仅有注释 `## -Xms4g`），ES 会按物理内存自动推算，**约为内存总量的 50%**。4 GB 内存的机器会推算接近 2 GB，加上堆外开销必然 OOM。

```bash
printf -- '-Xms1g\n-Xmx1g\n' > /etc/elasticsearch/jvm.options.d/heap.options
```

经验法则：**物理内存 ≈ 堆 × 2**。另一半留给文件系统缓存，ES 严重依赖 page cache。`Xms` 与 `Xmx` 必须相等，否则 JVM 会反复做堆伸缩，反而降低性能。

### 3.6 启动

```bash
systemctl daemon-reload
systemctl enable --now elasticsearch
systemctl status elasticsearch --no-pager -l | head -15
```

`enable` 与 `start` 缺一不可——缺少 `enable`，主机重启后服务不会自动恢复。

查看日志：

```bash
journalctl -u elasticsearch -f
tail -100f /var/log/elasticsearch/my-es.log
```

日志文件名格式为 `<cluster.name>.log`。

首次启动正常输出（20~40 秒内）：

```
[INFO ][o.e.n.Node                ] [elk1] version[7.17.22], pid[...], build[default/rpm/...], JVM[...]
[INFO ][o.e.c.r.a.AllocationService] [elk1] current.health="GREEN"
```

---

## 四、集群验证

### 4.1 端口

```bash
ss -ntl | grep -E '9[23]00'
```

```
LISTEN 0  4096  *:9200  *:*
LISTEN 0  4096  *:9300  *:*
```

### 4.2 身份

```bash
curl http://192.168.5.104:9200/
```

```json
{
  "name" : "elk1",
  "cluster_name" : "my-es",
  "version" : { "number" : "7.17.22", "build_type" : "rpm" },
  "tagline" : "You Know, for Search"
}
```

### 4.3 成员

```bash
curl -s 192.168.5.104:9200/_cat/nodes?v
```

在线节点数量决定返回行数，`master` 列带 `*` 的为当前主节点。**`ip` 列必须是业务地址**，若出现 `10.244.x.0` 则说明遗漏了 `network.publish_host`。

### 4.4 分片健康

```bash
curl -s "192.168.5.104:9200/_cluster/health?pretty"
```

创建一个带副本的索引观察颜色变化：

```bash
curl -XPUT "192.168.5.104:9200/t1" -H 'Content-Type: application/json' -d'
{ "settings": { "number_of_shards": 1, "number_of_replicas": 1 } }'

curl -s "192.168.5.104:9200/_cluster/health?pretty"
curl -s "192.168.5.104:9200/_cat/shards/t1?v"
```

| 在线节点数 | 集群颜色 | 原因 |
| --- | --- | --- |
| 1 | `red` 或 `yellow` | 副本无处分配；单节点可能存在主分片未分配 |
| 2 | `yellow`，`unassigned_shards: 1` | 副本不能与主分片同节点 |
| 3 | `green` | 主副分片各占一个节点 |

### 4.5 集群颜色语义

| 颜色 | 含义 |
| --- | --- |
| **green** | 全部主分片与副本均已分配 |
| **yellow** | 全部主分片已分配，**部分副本未分配**（数据完整，冗余缺失） |
| **red** | **至少一个主分片未分配**（对应数据不可读写） |

关键区别：`yellow` 缺失的是副本（可用性下降），`red` 缺失的是主分片（数据不在线）。

### 4.6 集群成形后的收尾

`cluster.initial_master_nodes` **仅在集群首次成形时有效**。保留该配置时，多台节点同时重启可能出现"各自认为自己是初始集群"的脑裂。

确认全部节点出现在 `_cat/nodes` 后，逐台执行（**不可同时重启**）：

```bash
sed -i '/cluster.initial_master_nodes/d' /etc/elasticsearch/elasticsearch.yml
systemctl restart elasticsearch
sleep 30
curl -s 192.168.5.104:9200/_cat/nodes?v
```

**同时建议关闭 GeoIP 下载器**。集群启动后日志会反复出现：

```
[ERROR][o.e.i.g.GeoIpDownloader] exception during geoip databases update
javax.net.ssl.SSLHandshakeException: PKIX path building failed ...
```

这是 ES 尝试从 `https://geoip.elastic.co` 下载 GeoIP 数据库（用于标注日志中 IP 的地理位置），经过代理的 TLS 中间人时 JVM 无法验证证书。**不影响集群运行**，但每次启动会刷满 ERROR 日志，掩盖真正的错误。不使用 GeoIP 可关闭：

```bash
echo 'ingest.geoip.downloader.enabled: false' >> /etc/elasticsearch/elasticsearch.yml
systemctl restart elasticsearch
```

---

## 五、索引管理

以下示例通过 `curl` 调用，设置环境变量简化命令：

```bash
export ES=http://192.168.5.104:9200
```

若使用 Kibana DevTools，可直接粘贴去掉 `curl` 部分的请求。

### 5.1 创建索引

```bash
# 最简形式：默认 1 主分片 + 1 副本
curl -XPUT "$ES/demo"

# 自定义分片数（未指定副本时沿用默认值 1）
curl -XPUT "$ES/demo-001" -H 'Content-Type: application/json' \
  -d '{ "settings": { "number_of_shards": 3 } }'

# 同时指定分片与副本
curl -XPUT "$ES/demo-002" -H 'Content-Type: application/json' \
  -d '{ "settings": { "number_of_shards": 3, "number_of_replicas": 1 } }'
```

返回：

```json
{"acknowledged":true,"shards_acknowledged":true,"index":"demo"}
```

默认配置为 1 主分片 1 副本。副本数在节点充足时可随时调整，而分片数创建后**不可修改**——这是建索引时唯一需要慎重考虑的参数。

### 5.2 查看索引

```bash
# 单个索引（含 settings / mappings / aliases）
curl -s "$ES/demo?pretty"

# 全部索引
curl -s "$ES/_cat/indices?v"

# 指定列并按大小排序
curl -s "$ES/_cat/indices?v&h=health,status,index,docs.count,store.size&s=store.size:desc"

# 字段结构
curl -s "$ES/demo/_mapping?pretty"
```

`_cat/indices` 关键列说明：

| 列 | 含义 |
| --- | --- |
| `health` | `green` / `yellow` / `red` |
| `status` | `open`（可读写）/ `close`（关闭，不可访问） |
| `pri` / `rep` | 主分片数 / 每个主分片的副本数 |
| `docs.count` | 文档数 |
| `store.size` | 主分片与副本的总占用 |
| `pri.store.size` | 仅主分片占用（**真实数据量以此为准**） |

### 5.3 修改索引

仅副本数与部分 settings 可修改，`number_of_shards` 不可变：

```bash
curl -XPUT "$ES/demo-001/_settings" -H 'Content-Type: application/json' \
  -d '{ "number_of_replicas": 0 }'
```

若确需改变分片数，只有一条路径：**新建索引 → `_reindex` 迁移数据 → 切换别名**。因此分片数需在创建时按数据量规划，官方建议单分片控制在 10~50 GB。

### 5.4 删除索引

```bash
curl -XDELETE "$ES/demo-002"      # 删除指定索引
curl -XDELETE "$ES/demo-*"        # 通配符批量删除（危险操作）
```

返回 `{"acknowledged":true}`。

**索引一旦删除，其下所有文档同时消失，且无回收机制。** 生产环境建议关闭通配符删除能力：

```bash
curl -XPUT "$ES/_cluster/settings" -H 'Content-Type: application/json' -d'
{ "persistent": { "action.destructive_requires_name": true } }'
```

开启后 `DELETE demo-*` 会被拒绝，必须使用完整索引名。

### 5.5 索引规划要点

| 参数 | 建议 |
| --- | --- |
| `number_of_shards` | 按数据总量估算，单分片 10~50 GB；节点数 × 1~3 倍为宜 |
| `number_of_replicas` | 生产环境至少 1；实验环境可设 0 减少 `yellow` 干扰 |
| 索引名 | 全小写，避免大写与特殊字符 |
| mapping | 关键索引应显式声明，避免自动推断 |

---

## 六、文档管理

7.x 已废弃 `doc` 类型，统一使用 `_doc`。写作 `/<index>/doc` 会返回 deprecation 警告但仍可执行，8.x 将直接报错。

### 6.1 创建文档

**不指定 id（推荐）**——由 ES 自动生成，不会冲突：

```bash
curl -XPOST "$ES/demo/_doc" -H 'Content-Type: application/json' \
  -d '{"name":"张三","hobby":["读书","跑步"]}'
```

返回：

```json
{
  "_index": "demo", "_type": "_doc", "_id": "Ix0EnJABsv5jddKIjbRG",
  "_version": 1, "result": "created",
  "_shards": { "total": 2, "successful": 1, "failed": 0 },
  "_seq_no": 0, "_primary_term": 1
}
```

**指定 id**——便于定位，但重复提交会覆盖：

```bash
curl -XPUT "$ES/demo/_doc/202407101700001" -H 'Content-Type: application/json' \
  -d '{"name":"李四","age":20}'
```

三种写入方式的语义差异：

| 请求 | 语义 | 重复 id 时的行为 |
| --- | --- | --- |
| `POST /<index>/_doc` | 自动生成 id | 不会冲突 |
| `PUT /<index>/_doc/<id>` | 指定 id | **覆盖**原文档，`_version` 递增 |
| `POST /<index>/_create/<id>` | 指定 id 且仅允许新建 | 返回 409 `version_conflict` |

需要"仅新建、不许覆盖"的语义时，使用 `_create`。

### 6.2 查看文档

```bash
# 全部文档（默认返回前 10 条）
curl -s "$ES/demo/_search?pretty"

# 按 id 查询
curl -s "$ES/demo/_doc/202407101700001?pretty"

# 仅返回源数据
curl -s "$ES/demo/_search?pretty&filter_path=hits.hits._source"

# 统计条数
curl -s "$ES/demo/_count?pretty"
```

### 6.3 修改文档

**局部更新 `_update`**——仅修改指定字段，其余保留：

```bash
curl -XPOST "$ES/demo/_doc/202407101700001/_update" -H 'Content-Type: application/json' \
  -d '{ "doc": { "age": 21 } }'
```

返回 `"result":"updated"`。注意 `_update` 的请求体需再包一层 `"doc"`，这是它与 `POST /<index>/_doc/<id>` 的差异。

**全量覆盖**——使用 `PUT` 或 `POST` 直接提交新文档，未包含的字段会被丢弃：

```bash
curl -XPUT "$ES/demo/_doc/202407101700001" -H 'Content-Type: application/json' \
  -d '{"name":"李四"}'
```

执行后 `age` 字段即消失。这是"数据莫名丢失"的常见原因。

### 6.4 删除文档

```bash
curl -XDELETE "$ES/demo/_doc/202407101700001"
```

返回 `"result":"deleted"`。**删除为逻辑删除**：文档标记为 deleted，待段合并（merge）时才从磁盘回收。因此大量删除后磁盘占用不会立即下降，`_forcemerge` 可加速回收。

---

## 七、批量操作

### 7.1 `_bulk` 的 NDJSON 格式

`_bulk` 使用 **NDJSON**（换行分隔 JSON），规则与普通 JSON 完全不同：

```
动作行（声明操作类型与目标索引）
数据行（具体内容）
动作行
数据行
...
```

三条强制规则：

1. **请求体必须以换行符结尾**，否则报 `The bulk request must be terminated by a newline [\n]`
2. **每个 JSON 必须在单行内完成**，不可格式化换行
3. 动作行只能为 `index` / `create` / `update` / `delete`

| 动作 | 含义 | 是否需要数据行 |
| --- | --- | --- |
| `index` | 写入，id 存在则覆盖 | 需要 |
| `create` | 写入，id 存在则报错 | 需要 |
| `update` | 局部更新 | 需要 |
| `delete` | 删除 | **不需要** |

### 7.2 批量创建

```bash
curl -XPOST "$ES/_bulk" -H 'Content-Type: application/json' --data-binary @- <<'EOF'
{ "index" : { "_index" : "demo" } }
{"name": "王五","age": 20}
{ "index" : { "_index" : "demo" } }
{"name": "赵六","age": 22}
{ "index" : { "_index" : "demo" } }
{"name": "孙七","age": 24}

EOF
```

`<<'EOF'` 后必须保留空行，以确保请求体以换行符结尾。必须使用 `--data-binary` 而非 `-d`，后者会吞掉换行符。

返回结构：

```json
{
  "took": 15, "errors": false,
  "items": [
    { "index": { "_index": "demo", "_id": "Lx0EnJAB...", "status": 201 } },
    ...
  ]
}
```

判断是否全部成功只需检查 `errors` 字段。为 `true` 时逐个查看 `items[].*.status` 与 `error`。

### 7.3 批量读取 `_mget`

```bash
curl -XPOST "$ES/_mget" -H 'Content-Type: application/json' -d'
{
  "docs": [
    { "_index": "demo", "_id": "202407101700001" },
    { "_index": "demo", "_id": "202407101700002" }
  ]
}'
```

同一索引下可简写：

```json
{ "ids": ["202407101700001", "202407101700002"] }
```

**职责划分**：写操作使用 `_bulk`，读操作使用 `_mget`。

### 7.4 批量更新

```bash
curl -XPOST "$ES/_bulk" -H 'Content-Type: application/json' --data-binary @- <<'EOF'
{ "update" : { "_index" : "demo", "_id" : "202407101700001" } }
{ "doc" : {"age" : 18, "hobby": ["游泳"]} }
{ "update" : { "_index" : "demo", "_id" : "202407101700002" } }
{ "doc" : {"age" : 30} }

EOF
```

`update` 的数据行需包裹 `"doc"`，与单条 `_update` 一致。使用脚本做数值累加：

```json
{ "update": { "_index": "demo", "_id": "1" } }
{ "script": { "source": "ctx._source.age += params.n", "params": { "n": 1 } } }
```

### 7.5 批量删除

```bash
curl -XPOST "$ES/_bulk" -H 'Content-Type: application/json' --data-binary @- <<'EOF'
{ "delete" : { "_index" : "demo", "_id" : "202407101700001" } }
{ "delete" : { "_index" : "demo", "_id" : "202407101700002" } }

EOF
```

`delete` 动作**不跟随数据行**。若误加数据行，ES 会将其解析为动作行并报解析错误。

### 7.6 批量操作的分批策略

| 场景 | 建议 |
| --- | --- |
| 单条写入 | 直接使用 `POST /_doc`，`_bulk` 的优势仅在批量时体现 |
| 数百条以内 | 单次 `_bulk` 发送完成 |
| 上万条 | 按 **5~15 MB** 或 **1000~5000 条** 分批 |
| 需要逐条结果 | 使用 `filter_path=items.*.status` 精简返回内容 |

---

## 八、查询 DSL

`_search` 的请求体即 Query DSL：

```bash
# match：分词后匹配，适用于 text 字段
curl -XPOST "$ES/demo/_search" -H 'Content-Type: application/json' -d'
{ "query": { "match": { "name": "张" } } }'

# term：不分词，精确匹配，适用于 keyword 字段
curl -XPOST "$ES/demo/_search" -H 'Content-Type: application/json' -d'
{ "query": { "term": { "brand": "HUAWEI" } } }'

# range：范围查询
curl -XPOST "$ES/demo/_search" -H 'Content-Type: application/json' -d'
{ "query": { "range": { "price": { "gte": 1000, "lte": 5000 } } } }'

# bool：组合查询
curl -XPOST "$ES/demo/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must":     [ { "match": { "title": "手机" } } ],
      "filter":   [ { "term":  { "group": 1 } } ],
      "must_not": [ { "range": { "price": { "lt": 100 } } } ]
    }
  },
  "from": 0, "size": 20,
  "sort": [ { "price": { "order": "desc" } } ]
}'
```

| 子句 | 作用 | 参与相关性评分 |
| --- | --- | --- |
| `must` | 必须匹配 | 是 |
| `filter` | 必须匹配 | **否**，可缓存，性能优于 `must` |
| `should` | 满足越多得分越高 | 是 |
| `must_not` | 必须不匹配 | 否 |

**`match` 与 `term` 的区别**是最常见的疑问：`match` 会先分词再匹配，`term` 用完整值到倒排索引中精确查找。`text` 字段用 `match`，`keyword` 字段用 `term`，混用将查不到数据。

---

## 九、映射与字段类型

ES 会自动推断字段类型（dynamic mapping），但推断结果往往不符合预期：

```bash
curl -s "$ES/demo/_mapping?pretty"
```

自动推断的两个问题：

- 字符串默认映射为 `text` 并附加 `.keyword` 子字段，占用双倍空间
- 首次写入 `"123"` 会将字段定为 `text`，后续写入数字将报 `mapper_parsing_exception`

因此**关键索引应当显式声明 mapping**：

```bash
curl -XPUT "$ES/shopping" -H 'Content-Type: application/json' -d'
{
  "aliases": { "homework": {} },
  "mappings": {
    "properties": {
      "title":    { "type": "text" },
      "price":    { "type": "float" },
      "brand":    { "type": "keyword" },
      "item":     { "type": "text" },
      "ipaddr":   { "type": "ip" },
      "producer": { "type": "text" },
      "type":     { "type": "keyword" },
      "group":    { "type": "byte" }
    }
  },
  "settings": { "number_of_shards": 3, "number_of_replicas": 0 }
}'
```

| 类型 | 用途 | 注意事项 |
| --- | --- | --- |
| `text` | 全文检索，会分词 | 不能用于排序与聚合 |
| `keyword` | 精确值、排序、聚合 | 不分词，配合 `term` 查询 |
| `float` / `double` / `integer` / `long` | 数值 | 价格用 `float` 或 `scaled_float` |
| `ip` | IP 地址 | 支持 CIDR 网段查询 `192.168.5.0/24` |
| `byte` | -128~127 | 适合小型分组编号 |
| `date` | 时间 | **ES 内部统一以 UTC 存储** |

对 `text` 字段排序或聚合会报 `Fielddata is disabled on text fields by default`，应改用 `.keyword` 子字段（如 `brand.keyword`）。

---

## 十、别名

别名是索引之上的逻辑名称，使代码固定引用别名，底层索引可替换：

```bash
# 事后添加别名
curl -XPOST "$ES/_aliases" -H 'Content-Type: application/json' -d'
{ "actions": [ { "add": { "index": "shopping", "alias": "homework" } } ] }'

# 查看
curl -s "$ES/_cat/aliases?v"

# 原子切换：删除旧指向、添加新指向，单次请求完成
curl -XPOST "$ES/_aliases" -H 'Content-Type: application/json' -d'
{ "actions": [
    { "remove": { "index": "shopping-v1", "alias": "homework" } },
    { "add":    { "index": "shopping-v2", "alias": "homework" } }
] }'
```

典型场景：重建索引（调整分片数或 mapping）时，应用始终访问 `homework`，切换过程对用户透明。

---

## 十一、排错

### 11.1 集群层

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `unknown setting [discovery type]` | 参数拼写错误，点被写成空格 | 修正为 `discovery.type` |
| `... in 'reader', line N, column 1` | YAML 格式错误（缩进、中文标点、重复键） | 检查报错行上下文 |
| 安装过其他版本，两者均无法启动 | 数据目录残留旧版本数据 | `rpm -e` → 清理 `/var/lib/elasticsearch/*` → 重装 |
| 脑裂：一台自成集群 | 数据未清理，各自 bootstrap | 全部停止 → 清理数据与日志 → 统一重启 |
| 无 master 节点 | 存活节点未过半 | 检查是否全部节点已启动 |
| 颜色异常但节点齐全 | 分片分配状态卡住 | 全部停止后依次启动 |

### 11.2 索引与文档层

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `index_not_found_exception` | 索引名拼写错误 | `GET /_cat/indices?v` 查看实际名称；索引名必须小写 |
| `resource_already_exists_exception` | 索引已存在 | 更换名称或先删除 |
| `mapper_parsing_exception: failed to parse field [price]` | 字段类型冲突：先写入 `"7999元"` 使字段定为 `text`，后续写入数字报错 | 新建索引 + `_reindex`；**建索引时显式声明 mapping** |
| `Fielddata is disabled on text fields` | 对 `text` 字段排序或聚合 | 使用 `.keyword` 子字段，或将字段映射为 `keyword` |
| `The bulk request must be terminated by a newline` | `_bulk` 请求体末尾缺少换行 | heredoc 中 `EOF` 前补空行；使用 `--data-binary` |
| `_bulk` 中 `delete` 后跟了数据行 | `delete` 动作不需要数据行 | 删除多余行 |
| `_bulk` 返回 `errors: true` 但部分成功 | 每行独立执行 | 查看 `items[].*.error.reason` 定位失败条目 |
| `version_conflict_engine_exception` | 使用 `create` 写入已存在的 `_id` | 改用 `index`（覆盖）或更换 id |
| `all shards failed` | 查询语法错误，如 `term` 查询 `text` 字段、字段名不存在 | 先用 `GET /<index>/_mapping` 确认字段名与类型 |
| 有数据但 `term` 查不到 | `text` 字段会分词，`term` 用完整值匹配 | 改用 `match` 或查询 `字段.keyword` |
| 大量删除后磁盘未释放 | 逻辑删除，等待段合并 | 只读索引可执行 `_forcemerge?max_num_segments=1`，**写入中的索引不要执行** |

### 11.3 部署环境特有

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| 启动报 `JsonParseException: Duplicate field 'network.host'` | 配置文件出现两行同名键，通常源于"向文件末尾追加"而非"修改原行" | ES 的 YAML 解析器**不允许重复键**，用 `grep -n '^network'` 确认仅剩一条 |
| 进程启动中途被 kill，日志含 `oom-killer` | 堆过大（自动推算的内存一半）且物理内存紧张 | 通过 `jvm.options.d/heap.options` 固定堆；或将内存提升至 8 GB |
| 本机 `curl` 正常、其他机器不通 | `network.host` 仍为默认 `localhost` | 改为 `0.0.0.0` 后重启 |
| `_cat/nodes` 的 ip 列为 `10.244.x.0` | 缺少 `network.publish_host`，选中了 overlay 网卡 | 参见 §3.4 |
| 日志出现 `PKIX path building failed` | GeoIP 数据库经代理下载时证书校验失败 | **无害**，集群仍可 `green`；可关闭 `ingest.geoip.downloader.enabled` |

### 11.4 判断致命错误

查看 `journalctl` 输出的**最后几行**：

```bash
journalctl -u elasticsearch -n 30 --no-pager
```

进程真正退出的标志：

```
elasticsearch.service: Main process exited, code=exited, status=1/FAILURE
```

**紧邻其上方的 `Caused by:` 才是根因**。日志中的 `[ERROR]` 级别条目（如 GeoIP 的 SSL 异常）多数不致命——进程仍在运行，仅某个后台任务失败。

---

## 十二、卸载

```bash
systemctl stop elasticsearch
rpm -e elasticsearch          # Debian 系：dpkg -r elasticsearch

# 数据与日志不会自动删除，需手动清理
rm -rf /var/lib/elasticsearch/* /var/log/elasticsearch/*
```

重装前必须清理数据目录，否则旧版本数据会带入新集群，是脑裂的常见来源。

---

## 十三、部署清单

```bash
# 1. 安装
curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.22-x86_64.rpm
rpm -ivh elasticsearch-7.17.22-x86_64.rpm

# 2. 内核参数（先检查，通常已由安装包配置）
sysctl vm.max_map_count

# 3. 堆大小（必须显式设置）
printf -- '-Xms1g\n-Xmx1g\n' > /etc/elasticsearch/jvm.options.d/heap.options

# 4. 配置（三台各一份，修改 node.name 与 publish_host）
vi /etc/elasticsearch/elasticsearch.yml

# 5. 启动
systemctl daemon-reload && systemctl enable --now elasticsearch

# 6. 验证
ss -ntl | grep -E '9[23]00'
curl -s http://<任意节点>:9200/_cat/nodes?v
curl -s "http://<任意节点>:9200/_cluster/health?pretty"

# 7. 集群成形后移除初始化参数，逐台重启
sed -i '/cluster.initial_master_nodes/d' /etc/elasticsearch/elasticsearch.yml
```

分片健康检查：

```bash
curl -s "http://<任意节点>:9200/_cluster/health?pretty"
curl -s "http://<任意节点>:9200/_cat/shards?v" | awk '$4=="UNASSIGNED"'
```

---

## 十四、验收清单

**集群层**

- [ ] `rpm -qa | grep elasticsearch` 有输出
- [ ] `sysctl vm.max_map_count` = 262144
- [ ] `ss -ntl` 可看到 9200 与 9300
- [ ] `curl :9200` 返回的 `cluster_name`、`node.name` 符合预期
- [ ] `_cat/nodes` 显示全部节点，其中一个带 `*`，`ip` 列为业务地址
- [ ] `_cluster/health` = `green`（三节点且存在副本索引）
- [ ] 已移除 `cluster.initial_master_nodes`
- [ ] `systemctl is-enabled elasticsearch` = `enabled`

**数据层**

- [ ] 创建索引返回 `acknowledged: true`
- [ ] 修改副本数生效
- [ ] 删除索引返回 `acknowledged: true`
- [ ] 单条写入返回 `result: created`
- [ ] 按 id 查询返回 `found: true`
- [ ] `_update` 局部更新后未指定字段仍存在
- [ ] `_bulk` 批量写入返回 `errors: false`
- [ ] `_mget` 可一次取回多条文档
- [ ] `_search` 可通过 `match` 检索到写入的数据

**理解层**

- [ ] 9200 与 9300 的协议与用途差异
- [ ] `green` / `yellow` / `red` 各自的缺失对象
- [ ] `match` 与 `term` 的区别，`text` / `keyword` 的对应关系
- [ ] 主分片数为何不可修改，调整时需走何种路径
- [ ] `_bulk` 与 `_mget` 的职责划分

---

## 十五、实战练习

### 15.1 索引操作

```bash
# 5 分片 0 副本
curl -XPUT "$ES/demo-exercise-001" -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":5,"number_of_replicas":0}}'

# 3 分片 1 副本
curl -XPUT "$ES/demo-exercise-002" -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":3,"number_of_replicas":1}}'

# 10 分片 2 副本（三节点上为 yellow，属正常现象）
curl -XPUT "$ES/demo-exercise-003" -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":10,"number_of_replicas":2}}'

# 批量删除 demo 开头的索引
curl -XDELETE "$ES/demo*"
```

执行后通过 `_cat/indices?v&h=health,index,pri,rep,docs.count` 核对分片总数。

### 15.2 数据采集与检索

```bash
# 1. 创建带别名与 mapping 的索引
curl -XPUT "$ES/shopping" -H 'Content-Type: application/json' -d'
{
  "aliases": { "homework": {} },
  "mappings": {
    "properties": {
      "title":    { "type": "text" },
      "price":    { "type": "float" },
      "brand":    { "type": "keyword" },
      "item":     { "type": "text" },
      "ipaddr":   { "type": "ip" },
      "producer": { "type": "text" },
      "type":     { "type": "keyword" },
      "group":    { "type": "byte" }
    }
  },
  "settings": { "number_of_shards": 3, "number_of_replicas": 0 }
}'
```

```bash
# 2. 批量写入数据（注意末尾空行）
curl -XPOST "$ES/_bulk" -H 'Content-Type: application/json' --data-binary @- <<'EOF'
{ "create": { "_index": "shopping" } }
{"title":"HUAWEI Mate 60 Pro 12GB+1TB 南糯紫","price":7999.00,"brand":"HUAWEI","item":"https://www.vmall.com/product/10086009079805.html","ipaddr":"117.78.25.90","producer":"张三","type":"DianZiChanPin","group":1,"color":"南糯紫"}
{ "create": { "_index": "shopping" } }
{"title":"小米电视 ES Pro 85英寸","price":6999.00,"brand":"Xiaomi","item":"https://www.mi.com/product/10000123.html","ipaddr":"111.13.197.1","producer":"张三","type":"JiaDian","group":2,"size":"85英寸"}
{ "create": { "_index": "shopping" } }
{"title":"农夫山泉 天然水 550ml*24瓶","price":39.90,"brand":"NongFu","item":"https://item.jd.com/100012043978.html","ipaddr":"211.152.33.1","producer":"李四","type":"NongChanPing","group":4,"spec":"550ml"}

EOF
```

```bash
# 3. 验证数据与结构
curl -s "$ES/shopping/_count?pretty"
curl -s "$ES/shopping/_mapping?pretty"

# 4. 通过别名查询（与直接查 shopping 等价）
curl -s "$ES/homework/_search?pretty&size=3"

# 5. 标题匹配 + 类型过滤
curl -XPOST "$ES/shopping/_search" -H 'Content-Type: application/json' -d'
{ "query": { "bool": {
    "must":   [ { "match": { "title": "电视" } } ],
    "filter": [ { "term":  { "type": "JiaDian" } } ]
} } }'

# 6. 价格区间 + 排序 + 字段裁剪
curl -XPOST "$ES/shopping/_search" -H 'Content-Type: application/json' -d'
{ "query": { "range": { "price": { "gte": 1000 } } },
  "sort": [ { "price": { "order": "desc" } } ],
  "_source": ["title", "price", "brand"] }'

# 7. 按 IP 网段查询
curl -XPOST "$ES/shopping/_search" -H 'Content-Type: application/json' -d'
{ "query": { "term": { "ipaddr": "111.13.0.0/16" } } }'
```

第 7 条依赖 `ipaddr` 被映射为 `ip` 类型。若由 ES 自动推断为 `text`，则只能做字符串精确匹配，无法使用 CIDR 网段查询——这是显式声明 mapping 的直接收益。

### 15.3 禁用通配符删除

```bash
# 开启后 DELETE /demo* 将被拒绝，必须使用完整名称
curl -XPUT "$ES/_cluster/settings" -H 'Content-Type: application/json' -d'
{ "persistent": { "action.destructive_requires_name": true } }'

# 验证
curl -XDELETE "$ES/demo*"     # 预期返回 action.destructive_requires_name 错误
curl -XDELETE "$ES/demo"      # 正常执行
```

`DELETE *` 一次误操作即可清空集群全部索引，且无回收机制。生产环境应默认开启此项设置。

---

## 十六、参考

- [Elasticsearch 7.17 官方文档](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/index.html)
- [Index modules — 分片与副本](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/index-modules.html)
- [Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html)
- [Mapping — 字段类型](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/mapping-types.html)
- [Query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl.html)
