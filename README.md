# Cloudreve SDK

本项目为**第三方制作**的 [Cloudreve](https://github.com/cloudreve/Cloudreve) V3 和 V4 社区版的 Python SDK。

- 开源地址：https://github.com/yxzlwz/cloudreve-sdk
- PyPI：https://pypi.org/project/cloudreve/
- API文档（我们维护 V3 版本；官方提供了 V4 版本的 API 文档）：https://cloudreve.apifox.cn/

## 安装

```bash
pip3 install cloudreve
```

## 适配情况

| 功能                 | V3 适配情况 | V4 适配情况   |
| -------------------- | ----------- | ------------- |
| 登录                 | ✅           | ✅             |
| 列目录               | ✅           | ✅             |
| 获取文件ID           | ✅           | 不需要        |
| 删除文件             | ✅           | ✅             |
| 获取文件属性         | ✅           | ✅             |
| 获取下载URL          | ✅           | ✅             |
| 下载文件             | ✅           | ✅             |
| 创建目录             | ✅           | ✅             |
| 复制文件             | ✅           | ✅             |
| 重命名文件           | ✅           | ✅             |
| 移动文件             | ✅           | ✅             |
| 获取文件直链         | ✅           | ✅             |
| 获取分享链接         | ✅           | ✅             |
| 上传文件（本地存储） | ✅           | ✅             |
| 上传文件（OneDrive） | ✅           | ✅             |
| 上传文件（OSS）      | 待测试      | 中转支持；直传未开放 |
| 上传文件（从机模式） | ❌           | ✅（未经测试） |

说明：

- V4 版本使用 URI 来标识文件和目录，不再需要文件ID。
- “待测试”表示该功能已实现，但由于缺乏测试环境，暂未进行测试，也没有公开使用。如果你使用该存储方案搭建 Cloudreve V4，并且愿意协助测试该功能，请您联系我。

### 本地 V4 文件操作修复

- `upload()` 支持策略已开启的 relay 上传，包括 OSS；`chunk_size=0` 流式发送整文件，空文件也发送完成请求。SDK 不创建策略或修改权限。
- 覆盖文件使用 `upload(local_path, uri, previous=conn.get_info(uri)['primary_entity'])`。旧版本冲突通过 `CloudreveError.code` 传给调用方；上传失败尝试取消已知会话，不自动重试。
- API 请求检查 HTTP 和业务错误码，默认连接／读超时为 10／120 秒，禁止自动重定向。下载仍以临时文件加原子替换保护现有目标文件；跨域下载不携带 CR 鉴权。
- **删除默认进入回收站**：`delete(uri)` 的 `trash_bin` 默认值改为 `True`，保留此前服务端的实际软删除行为。显式 `delete(uri, trash_bin=False)` 现在通过 V4 `skip_soft_delete=true` 实现永久删除；此前该参数被服务端忽略。
- OSS 直传保持未开放；权限和中转策略由部署侧配置。

从 SDK 目录运行离线测试：`uv run --with-editable . python -m unittest discover -s tests -v`。

## 并发、重试与内外网

使用当前源码安装：`uv pip install -e .`。这里的修复尚未发布到 PyPI，不能用同名版本号判断代码是否相同。

```python
import os
from concurrent.futures import ThreadPoolExecutor
from cloudreve import CloudreveV4

with CloudreveV4(
    os.environ['CLOUDREVE_ENDPOINT'],
    timeout=(10, 120),          # 连接超时、单次读取等待超时；不是整项任务截止时间
    read_retries=2,            # GET/HEAD 最多重试两次，0 可关闭
    trust_env=False,           # API 不采用自动发现的系统／环境代理或 netrc；不绕开 TUN
    allow_external_downloads=False,  # 下载只允许配置的 Cloudreve origin
) as conn:
    conn.login(os.environ['CLOUDREVE_USER'], os.environ['CLOUDREVE_PASSWORD'])
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(conn.get_info, ['/a.txt', '/b.txt']))
```

- 一个客户端可供固定线程池执行独立操作，每线程独立 Session / 连接池；先在创建客户端的线程完成配置和登录，再启动 worker。不要并发修改 `.session`、切换账号或关闭客户端，不要跨进程共享客户端。worker 全部结束后用 `close()` 或 `with` 关闭全部连接池；`session.close()` 只关闭当前线程的池。直接使用 `.session.request()` 会绕过 SDK 的重试、刷新及错误处理。
- V4 登录后按 `access_expires` 提前 30 秒刷新，多个线程只触发一次自动刷新；支持 Go 纳秒时间格式。刷新失败不会由排队线程再次发送，先停止 worker 并重新登录。没有过期时间的手工 token 需调用方管理；401 不会触发写请求重放。SDK 不保存密码，不持久化 token，也不提供重启续传。
- 仅 GET/HEAD 对连接错误、超时和 HTTP 429/502/503/504 有限重试；指数退避加抖动，`Retry-After` 等待上限 30 秒。证书错误、业务错误、其它 HTTP 错误不重试。POST/PUT/PATCH/DELETE、创建上传会话、上传块、回调和刷新均不自动重试。下载响应体开始读取后发生中断，不自动重下；目标文件保持原样。
- HTTP 错误保留 `requests.HTTPError`，网络错误保留 requests 异常；业务错误统一为 `CloudreveError`（含 `code`、`message`、`data`），无效 JSON / 缺少整数业务码为 `CloudreveProtocolError`。批量部分成功也抛业务异常并保留 `data`，不是事务回滚。不要无过滤记录完整异常 response、token、签名 URL 或服务端敏感数据。
- V4 已知会话上传失败会尽力取消，原异常保留 `upload_session_id`、`upload_uri`；取消失败另带 `cleanup_error`。创建会话响应丢失时拿不到 ID，无法取消；已完成写入的响应丢失也可能表现为失败。先查远端内容／版本再决定是否重试，不能把超时当作未写入。进程被杀、机器重启时无法保证清理，遗留会话由服务端过期机制处理。
- `upload(..., previous=primary_entity)` 和 `update_file_content(..., previous=primary_entity)` 可做条件覆盖；不传 `previous` 的文本更新保持原有无条件覆盖语义。并发覆盖可能返回锁冲突 `40073` 或旧版本 `40076`，由调用方重新读取并决定，不做盲目重试。多个下载写入同一本地路径是最后一次原子替换胜出，应使用不同目标文件。
- 上传按实际存储策略分流。relay 的 `chunk_size=0` 流式发送整文件，包含空文件；正分片大小按顺序上传，策略的 `chunk_concurrency` 不会让 SDK 擅自并行 relay 分片。多文件并发由线程池控制。OSS 直传仍未开放；OneDrive 直传空文件在创建会话前拒绝。从机上传只携带会话提供的从机凭据。
- SDK 不根据 `internal` 字样猜测路由，不改写签名 URL，也不会在内网失败后切到公网。API 固定配置 origin，默认拒绝重定向。下载采用独立无 CR 鉴权的 Session；`allow_external_downloads=True`（兼容默认值）允许返回的外部存储 URL，但仍不自动重定向。签名传输禁用 netrc／环境代理，只沿用显式 `proxy` 和 `verify`；如需自定义 CA，显式传 `verify='/path/to/ca.pem'`。
- 内网使用需同时满足：客户端能解析并访问内网入口、策略开启上传中转与下载代理。`allow_external_downloads=False` 只限制下载 origin，不证明 DNS 一定指向私网，也不阻止策略指定的从机上传。Cloudreve → OSS 的内网 endpoint 是部署配置；客户端 SDK 无法证明服务端出站计费。
- 普通路径按原始文本编码（包含中文、空格、`%`）；已编码路径请传完整 `cloudreve://` URI，避免二次编码。文件名是否合法仍由服务端校验，例如 `?` 会被当前部署拒绝。`list(order=...)` 正确发送 `order_direction`；游标分页使用返回的 `pagination.next_token`，不要自行解析。SDK 对已实测的 V4 目录／文件交界游标缺陷做窄范围兼容修正，不提供目录快照隔离。

真实验证与边界见 [验证记录](docs/verification.md) 和 [服务端到 OSS 全链路抓包](docs/end-to-end/README.md)、[本机切网后的公网实验](docs/local-external/README.md)，可复现脚本为 [experiments/live.py](experiments/live.py)。只操作 UUID 临时目录和其回收站条目，报告不保存密码、token、签名 URL。

```sh
uv run --with-editable . python -m unittest discover -s tests -v
uv run --env-file .env --with-editable . python experiments/live.py --output /tmp/cloudreve-live.json
```

V3 也使用统一请求、下载及连接管理；V3 的服务端行为、从机和 OneDrive 真实存储未在本轮环境验证。V3 不新增 token 刷新或上传会话取消协议。

## AsyncCloudreveV4（httpx / asyncio）

异步版已从 `cloudreve` 和 `cloudreve.models` 导出，安装当前源码会安装 `httpx>=0.27,<1`。同步 `CloudreveV4` 保持原接口。所有 V4 文件操作及别名都对应协程，参数、返回值和业务错误码一致。

```python
import asyncio
import os
from cloudreve import AsyncCloudreveV4

async def main():
    async with AsyncCloudreveV4(
        os.environ['CLOUDREVE_ENDPOINT'],
        trust_env=False,
        allow_external_downloads=False,
        timeout=(10, 120),
        read_retries=2,
        max_concurrency=4,     # 完整操作的 worker 数；等待队列容量也为 4
    ) as conn:
        await conn.login(os.environ['CLOUDREVE_USER'], os.environ['CLOUDREVE_PASSWORD'])
        await conn.upload('./report.txt', '/report.txt')
        await conn.download('/report.txt', './downloaded.txt')
        print(await conn.list('/'))

asyncio.run(main())
```

- 每次完整 SDK 调用作为一个操作入队，`max_concurrency` 默认 4：最多 4 个操作执行、4 个等待，更多调用等待入队。已入队操作按 FIFO 领取，完成顺序不保证。`upload` 内部的列目录、建会话、传片和校验属于同一个操作，内部调用不重复入队；单 worker 也不会嵌套等待自己。调用方可直接用 `asyncio.gather` / `TaskGroup` 提交多文件任务，无需另外用 semaphore 限制 SDK 并发。
- 同一事件循环内共享客户端和 HTTPX 连接池，保留每个调用者的 ContextVars。分片顺序发送，上传和下载按 64 KiB 流式处理，磁盘读写在线程执行器中完成。不要跨事件循环、线程或进程共享，也不要在工作中切换账号或修改会话配置。SDK 基于 asyncio，不支持 Trio。
- `async with` / `await conn.aclose()` 停止接单，取消排队及执行中的操作，等待取消清理完成，再关闭连接池；等待入队的调用也会被唤醒取消。正常批处理应先等待所有结果，提前退出上下文会取消剩余操作。客户端可多次关闭，关闭后不能重开。
- `timeout=(connect, read)` 同时把连接池等待设为 `connect`、写入等待设为 `read`；它是各阶段的等待超时，不是任务总时限。`request()` 的低层参数遵循 HTTPX（原始内容用 `content=`）。需要总时限时在调用方使用 `asyncio.wait_for`，取消后的清理可能让实际返回晚于截止时间。
- GET/HEAD 有限重试，等待可取消；TLS 证书错误、业务错误和写请求不重试，不跟随重定向。HTTP/网络异常分别为 `httpx.HTTPStatusError` / `httpx.RequestError` 子类；业务异常沿用 `CloudreveError`、`CloudreveProtocolError`。共享路径编码、令牌解析和分页兼容逻辑。
- token 刷新是鉴权请求的 barrier：暂停新的鉴权请求，等待在途鉴权请求结束，独占刷新后放行；多个刷新需求合并。不会等待整项上传结束，因此长操作可以在两个接口之间刷新，避免队列死锁。签名传输不使用登录 token。刷新请求失败或中途取消后不自动重发，应重新登录；只是在等在途请求时取消、尚未发出刷新，则不会把 token 标记为不确定。
- 排队时取消不会发请求；执行中取消保留 `CancelledError`，已知上传会话尽力取消一次，等待清理结束后再返回。重复 `cancel()` 不会留下无人等待的清理任务。下载在原子替换前取消或断流会保留旧目标并删除临时文件；提交完成后的取消不会回滚。进行中的磁盘操作结束后才关闭文件。
- 普通上传失败的异常携带 `upload_session_id`、`upload_uri`，清理失败附带 `cleanup_error`。Python 3.8 的 Task 不保留这些取消详情，需在直接调用 SDK 的协程内部捕获；Python 3.10 中信息可能位于 `__context__` 原异常链。创建会话的响应丢失时无法取得 ID；进程被杀也无法保证清理。实际部署取消接口可能返回 `code=-1`，SDK 不会把它吞掉或当作清理成功，详见验证记录。
- API 固定配置 origin；不会内网失败后切到公网。下载和直传使用独立客户端，不携带 Cloudreve headers、cookies，禁用环境代理，保留显式 `proxy` / `verify`；从机上传只使用上传凭据。`proxy` 支持 URL 或 `{'http': ..., 'https': ...}`。`trust_env=False` 不会绕开操作系统 TUN；`allow_external_downloads=False` 仅限制 URL origin，不能证明整个网络链路都在内网。

新增完整故障矩阵、40 组并发交错、120 秒压力测试和独立 256 MiB 断流测试，见 [并发与容错补测](docs/resilience/README.md)。独立运行：`uv run --with-editable . python -m experiments.large_transfer --size-mib 256 --output /tmp/cloudreve-large.json`。上传源文件的身份、大小或修改时间发生变化时会报错；这不保证远端尚未写入。

真实内网／公网、并发、取消结果见 [异步验证记录](docs/async/README.md)，复现脚本为 [async_live.py](experiments/async_live.py)。HTTPX 用法参考其 [异步文档](https://www.python-httpx.org/async/) 和 [超时文档](https://www.python-httpx.org/advanced/timeouts/)。

## 示例（V3）

- 每个方法都写了详细注释，可以直接阅读源码了解
- 除少数特殊说明的接口外，几乎所有对文件的操作都需要使用由**字母和数字**组成的文件ID作为参数

```python
from cloudreve import Cloudreve

# 初始化
conn = Cloudreve('http://127.0.0.1:5212')

# 登录
conn.login('admin@cloudreve.org', '123456')

# 列目录
conn.list("/")

# 获取文件ID
file_id = conn.get_id('/hello.py')

# 删除文件
conn.delete(file_id, is_dir=False)

# 获取文件属性
data = conn.get_property(file_id)
print(data)

# 获取临时下载URL
url = conn.get_download_url(file_id)

# 下载文件到本地
conn.download(file_id, 'hello_world.py')

# 创建目录
conn.create_dir('/python')

# 复制文件
conn.copy('/hello.py', '/python')

# 重命名文件
conn.rename(file_id, 'world.py')

# 移动文件
conn.move('/world.py', '/python')

# 上传文件
conn.upload('/my_file_backup.py', 'D:/my_file.py')

# 获取文件直链（永久有效）
# 直接返回直链
source_link_str = conn.get_source_url(file_id)
# 返回直链和文件名组成的字典
source_link_dict = conn.get_source_url(file_id, url_only=False)
# 同时获取多个文件，由于Cloudreve API实现问题，此时url_only参数必须为False，返回无序列表
list_of_source_link_dicts = conn.get_source_url([file_id], url_only=False)

# 获取分享链接
# 普通分享文件
share_link_str1 = conn.get_share_url(file_id)
# 禁止预览，下载次数限制为10次，过期时间为一天，密码为123456
share_link_str2 = conn.get_share_url(file_id, preview=False, downloads=10, expire=86400, password='123456')
# 分享文件夹
share_link_str3 = conn.get_share_url('IdOfDir', is_dir=True)
```

## 示例（V4）

- 原则上，Cloudreve V4 使用以 `cloudreve://` 开头的 URI 来标识文件和目录，但你可以在使用本 SDK 时，直接使用以 `/` 开头的路径，程序会自动转换为 URI。
- Cloudreve V4 的 List 接口有分页选项，具体请参考 [List files - Cloudreve API Docs](https://cloudrevev4.apifox.cn/list-files-300233178e0)

```python
from cloudreve import CloudreveV4

# 初始化
conn = CloudreveV4('http://127.0.0.1:5212')

# 登录
conn.login('admin@cloudreve.org', '123456')

# 列目录
conn.list("/")

uri = '/hello.txt'

# 获取文件属性
data = conn.get_info(uri)
print(data)

# 删除文件
conn.delete(uri)

# 下载文件到本地
conn.download(uri, './hello_world.txt')

# 创建目录
conn.create_folder('/python')

# 复制文件
conn.copy('/hello.txt', '/python')

# 重命名文件
conn.rename(uri, 'world.txt')

# 移动文件
conn.move('/world.py', '/python')

# 上传文件
conn.upload('D:/my_file.py', '/my_file_backup.py')

# 创建文件
conn.create_file('/new_file.txt')

# 更新文件
conn.update_file_content('/new_file.txt', 'This is the content of the new file.')

# 获取文件直链（永久有效）
# 简单获取（返回字符串）
source_link_str = conn.get_source_url(uri)
# 获取多个文件（返回由字典组成的列表）
list_of_source_link_dict = conn.get_source_url([uri1, uri2])

# 获取分享链接
# 普通分享文件
share_link_str1 = conn.get_share_url(uri)
# 下载次数限制为10次，过期时间为一天，密码为123456
share_link_str2 = conn.get_share_url(uri, downloads=10, expire=86400, password='123456')
```

## 联系我们

- Email：i@yxzl.dev
- TG 群：https://t.me/+XW2ok10N8DExMDU1

## 感谢

- [@TeoZler](https://github.com/TeoZler) 协助测试 Cloudreve V4 OneDrive 上传
- [@PTPAAA](https://github.com/PTPAAA) 提供 Cloudreve V4 [从机模式上传代码](https://github.com/PTPAAA/cloudreve-sdk/commit/130b5e9f5777a439fd82a4755bfff7a5968bf57f)
