# LiteSandbox

**为 AI Agent 提供本机运行的 Docker 沙箱。**

通过 Python SDK、MCP 或 HTTP API，给每个任务分配独立工作区，在里面运行命令和读写文件。

- **异步执行**：后台运行命令，订阅 SSE 增量日志，按命令取消并保留工作区。
- **受控联网**：支持直接联网、完全离线和代理出站，按沙箱授权指定域名或 HTTPS API。
- **预热与回收**：提前准备容器，任务结束后自动回收。本机串行测试中，分配延迟 P50 从 504 ms 降至 208 ms。

[使用指南](docs/guide.md) · [HTTP API](docs/guide.md#http-api) · [性能数据](docs/guide.md#性能数据)

## 快速开始

需要 **Python 3.11+** 和运行中的 **Docker**。在仓库目录运行：

```sh
python -m pip install .
python -m litesandbox serve
```

服务默认监听 `127.0.0.1:8765`。在另一终端运行这段 Python：

```python
from litesandbox import LiteSandbox

with LiteSandbox().session(ttl_seconds=300) as sandbox:
    sandbox.write_file("hello.sh", "echo 'Hello, LiteSandbox!' > result.txt\n")
    result = sandbox.exec(["sh", "hello.sh"])
    assert result.exit_code == 0, result.stderr
    print(sandbox.read_text("result.txt"), end="")
```

输出 `Hello, LiteSandbox!`。退出 `with` 时客户端会尝试删除沙箱。若服务改了端口，可通过 `LiteSandbox(port=...)` 连接。
