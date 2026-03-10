# Windows 运行说明

这份说明用于把整个项目文件夹直接发给同事后，在 Windows 机器上快速启动。

## 1. 打包时请确保包含这些内容

- `server.py`
- `scripts/`
- `static/`
- `references/`
- `data/sample_parking_ops.db`
- `data/sample_sales.csv`
- `requirements-server.txt`
- `run_windows.bat`

如果你不打包 `data/sample_parking_ops.db`，那就必须同时打包：

- `data/5个停车场数据/`
- `scripts/build_parking_ops_from_excels.py`

这样脚本会在首次启动时自动从 Excel 重建 SQLite 数据库。

## 2. 同事机器需要的前置条件

- Windows 10/11
- Python 3.10 及以上
- 安装 Python 时勾选 `Add Python to PATH`
- 能联网安装 Python 依赖

## 3. 推荐的 `.env` 配置

项目根目录建议放一个 `.env` 文件：

```env
ANTHROPIC_AUTH_TOKEN=你的密钥
ANTHROPIC_BASE_URL=你的兼容地址
ANTHROPIC_MODEL=claude-opus-4-6
```

说明：

- 没有 `.env` 也能启动页面。
- 但没有 `ANTHROPIC_AUTH_TOKEN` 或 `ANTHROPIC_API_KEY` 时，完整 AI 对话能力会受限。
- 当前停车复杂语义拆解和通用对话链路依赖 Anthropic 通道。

## 4. 启动方法

双击：

```text
run_windows.bat
```

脚本会自动完成这些动作：

1. 检查 Python
2. 创建 `.venv`
3. 安装依赖
4. 检查 `.env`
5. 如果缺少 `data/sample_parking_ops.db`，则尝试从 Excel 重建
6. 启动 Web 服务
7. 自动打开浏览器 `http://127.0.0.1:8000`

## 5. 首次建议验证的问题

启动后可先试这几句：

- `给老板看下今天经营情况`
- `生成最近7天停车经营周报，给管理层看`
- `哪个场子这周最不正常`
- `高林去年 2 月哪天收入最差，为什么`

## 6. 常见问题

### 依赖安装失败

通常是 Python 或网络问题。先确认：

- 命令行执行 `python --version` 或 `py -3 --version` 正常
- 能访问 pip 源

### 页面打开了，但对话报密钥错误

说明 `.env` 没配好，或没有把密钥传给同事。先检查：

- 项目根目录是否存在 `.env`
- `.env` 中是否配置了 `ANTHROPIC_AUTH_TOKEN` 或 `ANTHROPIC_API_KEY`

### 提示找不到 `sample_parking_ops.db`

有两种解决方式：

1. 直接把 `data/sample_parking_ops.db` 一起打包
2. 或把 `data/5个停车场数据/` 一起打包，让脚本自动重建

### 端口 8000 被占用

可以先关闭占用 8000 端口的程序，再重新双击 `run_windows.bat`。

## 7. 关闭服务

关闭启动脚本所在的命令行窗口即可。
