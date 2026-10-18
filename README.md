# Elasticsearch 7.17 三节点集群实践

从零搭建 ES 7.17 三节点集群的技术笔记，覆盖部署原理、集群配置、索引与文档 API、批量操作、查询 DSL 与排错。

## 文章

- [从零搭建 Elasticsearch 7.17 三节点集群：原理、配置与 API 实战](./Elasticsearch-7.17.22-学习笔记.md)

## 内容概览

| 章节 | 主题 |
| --- | --- |
| 一 ~ 二 | 核心概念（索引、分片、副本、分配）、架构决策与集群发现机制 |
| 三 ~ 四 | 集群部署（安装、内核参数、配置、堆内存）、四层验证方法 |
| 五 ~ 七 | 索引管理、文档管理、`_bulk` / `_mget` 批量操作 |
| 八 ~ 十 | 查询 DSL、映射与字段类型、别名切换 |
| 十一 ~ 十二 | 排错手册（集群层 / 索引层 / 环境层）、卸载 |
| 十三 ~ 十五 | 部署清单、验收清单、实战练习 |

## 已验证环境

| 项 | 版本 |
| --- | --- |
| OS | Rocky Linux 9.8 |
| Elasticsearch | 7.17.22（RPM） |
| 节点 | 3 台，全角色（master-eligible + data） |
| JVM | 随包自带 JDK，堆固定 1 GB |

Debian / Ubuntu 环境仅需替换包管理命令（`dpkg -i` / `dpkg -r`），其余配置一致。

## 几个实证结论

- **`network.publish_host` 在 overlay 网络环境下必须显式指定**：仅配置 `network.host: 0.0.0.0` 时，ES 可能选中 flannel 等虚拟网卡的子网地址（如 `10.244.1.0`），导致跨网段客户端不可达。
- **7.17 的堆内存并非默认 1 GB**：`jvm.options` 未固定堆大小，ES 按物理内存的 50% 推算，内存较小的机器会因此 OOM，必须通过 `jvm.options.d/heap.options` 显式设置。
- **`/etc/sysctl.d/` 的加载顺序由文件名排序决定**：自定义配置应使用排序靠后的文件名（如 `99-zz-elasticsearch.conf`），否则可能被同名参数覆盖。
- **配置文件不允许重复键**：向 `elasticsearch.yml` 追加而非修改原行，会触发 `Duplicate field` 并直接导致启动失败。

## 排错速查

判断哪条错误才是致命的，查看 `journalctl` 输出末尾：

```bash
journalctl -u elasticsearch -n 30 --no-pager
```

进程退出的标志是 `Main process exited, code=exited, status=1/FAILURE`，**紧邻其上方那条 `Caused by:` 才是根因**。日志中的 `[ERROR]` 级别条目（如 GeoIP 的 SSL 异常）多数不致命。

## 许可

本文档采用 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) 许可。
