# 运维学习

> Linux 环境下分布式中间件的集群搭建实录。按实际执行顺序写，包括中途卡住的地方和解决办法——不只是"命令能跑通"，而是"为什么这么配、跳过会出什么事"。

每个主题一个目录，目录内有独立 README 作为该主题的索引页。

## 主题

| 目录 | 主题 | 内容 |
| :--- | :--- | :--- |
| [elasticsearch/](./elasticsearch/) | Elasticsearch 7.17 与 ZooKeeper 3.8 | ES 三节点集群部署原理、索引与文档 API、批量操作、查询 DSL、映射、排错；ZK 角色与选主、znode 操作、watch、四字命令、故障演练、调优 |

## 约定

- 已验证环境：Rocky Linux 9 系列，RPM 安装。Debian / Ubuntu 仅需替换包管理命令（`dpkg -i` / `dpkg -r`），配置一致。
- 涉及具体主机时 IP 一律用 `192.168.5.x` 占位，替换成自己的地址即可。
- 排错章节记录的都是实际踩过的坑，不是假设的错误。

## 许可

本文档采用 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) 许可。
