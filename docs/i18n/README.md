# SpeedMQ 文档多语言索引 / Documentation Index

本目录存放各语言 `README` 译文、**插件开发文档**（`docs/plugin-development*.md`）与运维文档（`docs/ops/`）的**多语言版本**。

**简体中文是源语言，原文即唯一事实来源**：简体中文原文见仓库根 [README-cn.md](../../README-cn.md)（英文版为仓库根 [README.md](../../README.md)），以及 [docs/plugin-development.md](../plugin-development.md)（插件开发总览，另有 4 份同目录语言文档）与 [docs/ops/](../ops/)；因此不在本目录重复存放 zh-CN 副本。译文如与原文不一致，一律以中文原文为准。

## 语言版本

| 语言 | README | 插件开发文档 | 运维文档 |
| --- | --- | --- | --- |
| 简体中文（源语言） | [README-cn.md](../../README-cn.md) | [docs/plugin-development.md](../plugin-development.md) | [docs/ops/](../ops/) |
| 繁體中文 | [README](zh-TW/README.md) | [plugin-development.md](zh-TW/plugin-development.md) | [ops/](zh-TW/ops/) |
| English | [README.md](../../README.md) | [plugin-development.md](en/plugin-development.md) | [ops/](en/ops/) |
| 日本語 | [README](ja/README.md) | [plugin-development.md](ja/plugin-development.md) | [ops/](ja/ops/) |
| 한국어 | [README](ko/README.md) | [plugin-development.md](ko/plugin-development.md) | [ops/](ko/ops/) |
| Español | [README](es/README.md) | [plugin-development.md](es/plugin-development.md) | [ops/](es/ops/) |
| Deutsch | [README](de/README.md) | [plugin-development.md](de/plugin-development.md) | [ops/](de/ops/) |
| Français | [README](fr/README.md) | [plugin-development.md](fr/plugin-development.md) | [ops/](fr/ops/) |
| العربية | [README](ar/README.md) | [plugin-development.md](ar/plugin-development.md) | [ops/](ar/ops/) |
| Русский | [README](ru/README.md) | [plugin-development.md](ru/plugin-development.md) | [ops/](ru/ops/) |
| Italiano | [README](it/README.md) | [plugin-development.md](it/plugin-development.md) | [ops/](it/ops/) |
| Nederlands | [README](nl/README.md) | [plugin-development.md](nl/plugin-development.md) | [ops/](nl/ops/) |
| Português | [README](pt/README.md) | [plugin-development.md](pt/plugin-development.md) | [ops/](pt/ops/) |
| Bahasa Indonesia | [README](id/README.md) | [plugin-development.md](id/plugin-development.md) | [ops/](id/ops/) |
| ไทย | [README](th/README.md) | [plugin-development.md](th/plugin-development.md) | [ops/](th/ops/) |
| Tiếng Việt | [README](vi/README.md) | [plugin-development.md](vi/plugin-development.md) | [ops/](vi/ops/) |
| Bahasa Melayu | [README](ms/README.md) | [plugin-development.md](ms/plugin-development.md) | [ops/](ms/ops/) |
| Filipino | [README](fil/README.md) | [plugin-development.md](fil/plugin-development.md) | [ops/](fil/ops/) |

> 「插件开发文档」一列指向该语种的**总览**（`plugin-development.md`，含心智模型、线协议规格与配置字段）。
> 它还有 4 份同目录的姊妹文档（`plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`），
> 链接写在总览正文里。

## 目录结构 / Layout

```
docs/i18n/<lang>/
├── README.md                       # 根 README 的译文（English 例外：英文版即仓库根 README.md）
├── plugin-development.md           # 插件开发：总览（心智模型 / 线协议 / 配置 / 打包）
├── plugin-development-python.md    # 插件开发：Python
├── plugin-development-nodejs.md    # 插件开发：Node.js
├── plugin-development-php.md       # 插件开发：PHP
├── plugin-development-java.md      # 插件开发：Java
└── ops/
    ├── backup-restore.md           # 运维：备份与恢复
    ├── upgrade.md                  # 运维：升级
    ├── security-baseline.md        # 运维：安全基线
    └── monitoring/README.md        # 运维：监控
```

## 说明

- 代码块、命令、配置项名、环境变量、文件路径、端口、URL 与协议标识符（如 `amq.topic`、`x-queue-type`）在译文中保持原样，只有说明性文字被翻译。
- 代码块**整块原样保留**（含其中的中文注释、日志样例与命令输出），便于与源文档逐块对照。
- 译文中的**仓库内相对链接**会按层级改写：`docs/` 下的源文档写 `../pkg/sidecar/`，译文在 `docs/i18n/<lang>/` 下则写 `../../../pkg/sidecar/`；指向同目录姊妹文档的链接（`plugin-development-*.md`）保持原样。
- 译文不带"多语言横幅"（横幅只在中文源文档里，指向本索引）。
- 管理 UI 的界面语言与本文档语言相互独立：UI 语言在管理后台上由用户选择，默认按部署地时区自动确定。
- 新增语种：在 `docs/i18n/<lang>/` 下按上面的结构补齐文件（README + 5 份插件开发文档 + `ops/`），并更新本索引。
