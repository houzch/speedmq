# SwiftMQ 文档多语言索引 / Documentation Index

本目录存放 `README` 与运维文档（`docs/ops/`）的**多语言版本**。

**简体中文是源语言，原文即唯一事实来源**：根 [README](../../README.md) 与 [docs/ops/](../ops/)，因此不在本目录重复存放 zh-CN 副本。译文如与原文不一致，一律以中文原文为准。

## 语言版本

| 语言 | README | 运维文档 |
| --- | --- | --- |
| 简体中文（源语言） | [README](../../README.md) | [docs/ops/](../ops/) |
| 繁體中文 | [README](zh-TW/README.md) | [ops/](zh-TW/ops/) |
| English | [README](en/README.md) | [ops/](en/ops/) |
| 日本語 | [README](ja/README.md) | [ops/](ja/ops/) |
| 한국어 | [README](ko/README.md) | [ops/](ko/ops/) |
| Español | [README](es/README.md) | [ops/](es/ops/) |
| Deutsch | [README](de/README.md) | [ops/](de/ops/) |
| Français | [README](fr/README.md) | [ops/](fr/ops/) |
| العربية | [README](ar/README.md) | [ops/](ar/ops/) |
| Русский | [README](ru/README.md) | [ops/](ru/ops/) |
| Italiano | [README](it/README.md) | [ops/](it/ops/) |
| Nederlands | [README](nl/README.md) | [ops/](nl/ops/) |
| Português | [README](pt/README.md) | [ops/](pt/ops/) |
| Bahasa Indonesia | [README](id/README.md) | [ops/](id/ops/) |
| ไทย | [README](th/README.md) | [ops/](th/ops/) |
| Tiếng Việt | [README](vi/README.md) | [ops/](vi/ops/) |
| Bahasa Melayu | [README](ms/README.md) | [ops/](ms/ops/) |
| Filipino | [README](fil/README.md) | [ops/](fil/ops/) |

## 目录结构 / Layout

```
docs/i18n/<lang>/
├── README.md                       # 根 README 的译文
└── ops/
    ├── backup-restore.md           # 运维：备份与恢复
    ├── upgrade.md                  # 运维：升级
    ├── security-baseline.md        # 运维：安全基线
    └── monitoring/README.md        # 运维：监控
```

## 说明

- 代码块、命令、配置项名、环境变量、文件路径、端口、URL 与协议标识符（如 `amq.topic`、`x-queue-type`）在译文中保持原样，只有说明性文字被翻译。
- 管理 UI 的界面语言与本文档语言相互独立：UI 语言在管理后台上由用户选择，默认按部署地时区自动确定。
- 新增语种：在 `docs/i18n/<lang>/` 下按上面的结构补齐文件，并更新本索引。
