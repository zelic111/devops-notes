# Elasticsearch

Elasticsearch 7.17.22 三节点集群的搭建与使用记录。

| 文档 | 内容 | 环境 |
| :--- | :--- | :--- |
| [Elasticsearch 7.17 三节点集群](./Elasticsearch-7.17.22-学习笔记.md) | 部署原理、集群发现、索引与文档 API、批量操作、查询 DSL、映射、别名、排错 | Rocky Linux 9.8 · ES 7.17.22 · JDK 随包 |

---

## 目录

| 章节 | 内容 |
| :--- | :--- |
| 一 ~ 二 | 核心概念（索引 / 分片 / 副本 / 分配）、架构决策与集群发现机制 |
| 三 ~ 四 | 集群部署（安装、内核参数、配置、堆内存）、四层验证方法 |
| 五 ~ 七 | 索引管理、文档管理、`_bulk` / `_mget` 批量操作 |
| 八 ~ 十 | 查询 DSL、映射与字段类型、别名切换 |
| 十一 ~ 十二 | 排错手册（集群层 / 索引层 / 环境层）、卸载 |
| 十三 ~ 十五 | 部署清单、验收清单、实战练习 |

## 已验证环境

| 项 | 版本 |
| :--- | :--- |
| OS | Rocky Linux 9.8 |
| Elasticsearch | 7.17.22（RPM） |
| 节点 | 3 台，全角色（master-eligible + data） |
| JVM | 随包自带 JDK，堆固定 1 GB |

Debian / Ubuntu 环境仅需替换包管理命令（`dpkg -i` / `dpkg -r`），其余配置一致。

## 四个实证结论

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

## 排错速查

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

---

## 参考

- [Elasticsearch 7.17 官方文档](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/index.html)
