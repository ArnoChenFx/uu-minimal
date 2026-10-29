# uu-minimal — 登录 + 组网最小骨架

独立实现（与主程序同一套协议、行为对齐）、能独立编译运行的三条链路：

1. **登录**：设备注册 → 二维码 → 长轮询 → 换 token（`~/.config/uuremote/config.json`，0600）
2. **组网**：建房 → Socket.IO → `be-controlled` → `soac` offer / answer / 候选
3. **工具**：终端（BINARY 上的 `TERM` 帧）与端口映射（FILE 上的 Message 27）

不含画面、输入、文件传输、剪贴板、多显示器、重连退避、协助码和 GTK 窗口。连上后桌面是黑的、文件传输无响应，**终端和端口映射可用**。

同一二进制也可当控制端（`list` / `ssh` / `sftp` / `tunnel` / `portmap`，只走 TCP 和 WebRTC）。手机只能做控制端：被控需要 `openpty`、fork shell 和 `/proc`，iOS 都不允许。Android 可在 Termux 里编；iOS 只能嵌成静态库，菜单用 SwiftUI，`sftp` 换 russh。桌面 Linux 上的 `run` 是部分被控端。

## 构建与运行

克隆时需带上 submodule（内含打过补丁的 `webrtc*` vendor，与 [uu-remote-linux](https://github.com/VallierDev/uu-remote-linux) 一致）：

推送到 `main` 时会自动打 tag（`v0.1.x` 递增）并在 [GitHub Releases](https://github.com/ArnoChenFx/uu-minimal/releases) 发布 `uu-minimal`（musl）、`uu-minimal-glibc`、`uu-minimal.exe` 与 `SHA256SUMS`。

```sh
git clone --recurse-submodules https://github.com/ArnoChenFx/uu-minimal.git
# 已有仓库：git submodule update --init --recursive
# 跟进主程序 vendor 更新：git submodule update --remote vendor-src
```

```sh
cargo build --release
./target/release/uu-minimal help      # 命令与选项
./target/release/uu-minimal login     # 二维码只打到当前终端
./target/release/uu-minimal run
```

日志默认本程序 `info`，`webrtc*` / `russh` 压到 `warn`。`ssh` / `sftp` / `tunnel` 默认静音（独占终端）。`RUST_LOG` 优先，例如 `RUST_LOG=info,webrtc_ice=debug`；`NO_COLOR=1` 关颜色。

`run` 收到房间级离开通知后会重新建房；短暂 ICE `Disconnected` 保留端口映射 TCP 流，明确 `Failed` / `Closed` 才清理。控制端 `portmap` 在通道关闭、PeerConnection 失败，或连续三次探测无回包后重连；被控端恢复后无需手动重启 `portmap`。重连会建立新链路，已经断开的业务 TCP 连接仍需业务客户端重新建立。

被控端 `run` 还会通过信令事件 `refresh_reconnect_key` 刷新当前房间的重连凭证：Socket.IO 握手完成后约 30 秒首次发送，有效成功应答后约每 5 分钟刷新，失败或无应答约 30 秒重试，避免遗漏此步骤造成约 20 分钟后的房间回收。日志会记录刷新请求和成功/无效应答，凭证仅保存在当前房间内存中；普通心跳及端口映射探测不能替代这一步。此机制参照 [uu-remote-linux v0.1.5 的修复说明](https://github.com/VallierDev/uu-remote-linux/releases/tag/v0.1.5)。

程序默认不写日志文件。Windows 使用 NSSM 时，可在管理员 CMD 中设置日志输出（替换服务名和目录，先创建日志目录）：

```bat
nssm set "服务名" AppStdout "C:\uu-minimal\logs\stdout.log"
nssm set "服务名" AppStderr "C:\uu-minimal\logs\stderr.log"
nssm set "服务名" AppRotateFiles 1
nssm set "服务名" AppRotateOnline 1
nssm set "服务名" AppRotateBytes 10485760
nssm restart "服务名"
```

连接恢复日志主要在 `stderr.log`。NSSM 的进程重启策略无法检测进程内部的链路假死；排查时保留掉线前后的日志。

凭据与主程序共用。`UU_CONFIG` 指向别处的 `config.json`（相对路径按当前目录解析）：

```sh
UU_CONFIG=./config.json ./target/release/uu-minimal auth
UU_CONFIG=./config.json ./target/release/uu-minimal list
UU_CONFIG=./config.json ./target/release/uu-minimal ssh --device <id> --user me
```

`config.json` 里的 `token` / `csrf` 只在本机使用，不要提交（本目录 `.gitignore` 只忽略 `target/` 和日志）。

| 项 | 说明 |
|---|---|
| `platform` / `UU_PLATFORM` | 默认 `mac`。注册走 `/api/v1/device/{macos\|windows}/init`（云端没有 `mac`，会 404）；`x-param-plat` macos=2 / windows=1；ConnectOptions 的 platform mac=4 / windows=3。已有 `device_id` 不会重注册，换平台先删掉再 `login` |
| `machine_guid`、`host_name` | 以配置为准，`UU_MACHINE_GUID` / `UU_HOST_NAME` 可临时覆盖。缺字段时首次 `login` 用 `/etc/machine-id` 和 hostname 补上并写回 |
| macOS 注册附加项 | `system_version` / `model` / `model_identifier` / `system_name` / `resolution` / `dpi`，且 `memory` 必须是字符串（缺了服务端 422/1001）。默认为 `14.5` / `MacBook Pro` / `MacBookPro18,3` / `macOS` / `1920x1080` / `96`，同名字段可覆盖 |
| `ssh_host` / `ssh_port` | `ssh` 和设备菜单的缺省目标；`UU_SSH_HOST` / `UU_SSH_PORT` 可覆盖，否则 `127.0.0.1:22`。命令行主机或 `--port` 优先 |
| `UU_PORT_MAPPING=0` | 关闭被控端端口映射 |

## 控制别人

```sh
uu-minimal auth                 # 本地凭据 + /api/v1/device/list
uu-minimal auth --login         # 失效则重新扫码
uu-minimal list                 # TTY 进菜单；管道只打印

uu-minimal portmap --device <id> 2222 127.0.0.1 22
ssh -p 2222 me@127.0.0.1

# 同一 device 开多个本地端口：一个进程 + 多个 --map（不要开两个 portmap 进程，会 force_join 互踢）
uu-minimal portmap --device <id> \
  --map 2222,127.0.0.1,22 --map 3389,192.168.0.10,3389

# 监听所有网卡（默认 --bind 127.0.0.1）
uu-minimal portmap --device <id> --bind 0.0.0.0 --map 2222,127.0.0.1,22

uu-minimal ssh --device <id> --user me
uu-minimal ssh --device <id> --user me --command "uname -a"

# 系统 ssh 的 -L/-R/-D、scp、rsync 才需要 stdio 桥
uu-minimal tunnel --device <id> 127.0.0.1 22
ssh -o ProxyCommand="uu-minimal tunnel --device <id> %h %p" -p 22 me@127.0.0.1

uu-minimal sftp --device <id> me@127.0.0.1   # exec 系统 sftp，ProxyCommand 指回 tunnel
```

内嵌 SSH（russh）不读 `~/.ssh/config`，也不调用系统 ssh。认证顺序：`--password` → `--key` 或 `~/.ssh/id_{ed25519,ecdsa,rsa}` → 终端输入。主机密钥存在 `~/.config/uuremote/known_hosts`（TOFU，`<host>:<port> <sha256>`），指纹变了就拒绝，不碰 `~/.ssh/known_hosts`。

`list` 顶部「本机」来自 `current_device`（名字字段是 `alias`），不在自有/共享列表里，也不进选择菜单；`controllable` 为假表示官方客户端连不进来。菜单项是 ssh、sftp、portmap（占到 Ctrl-C）。ssh 与 portmap 各占一条 WebRTC，**不要同时开**；同一 device **不要开两个 portmap 进程**，多端口用 **`--map` 重复**（见上）。ssh 退出后菜单会重新 join。

`join` 带 `force_join: true`；`ice_id` 以云端 ack 为准；offer 补 `a=x-uuremote-quic`。官方 host 可能用二进制事件加 gzip_sdp 回 answer。一条 Message 27 流就是一条 TCP。

## 被控与安全

- 终端起 `$SHELL -l`（默认 `/bin/bash`）。会话跨 viewer 存活，重连最多回放 256 KiB scrollback；`DETACH` 只解绑、不杀进程。
- 端口映射按对端给的 `target_host:target_port` 主动去连（10s 超时），等于把 **host 所在内网** 暴露给对端，只在可信账号下开。
- 不监听入向端口，只有一条出向 WSS 加 WebRTC 的 UDP。STUN/TURN 全由云端下发，没有内置中继；云端不给才退回公共 STUN。`force_relay=true` 但没有 TURN 直接报错，不静默降级。候选只留物理网卡（丢掉 `zt*` / `docker` / `br-` / `veth` / `virbr` / `lo`），关闭 mDNS。host 固定 DTLS Server；answer 补 `a=max-message-size:262144`，否则对端按 64 KB 拒绝大帧。
- 二维码等同登录票据，只打在当前终端，不进日志、不落盘、不要截图外发。日志只记字段号、计数和布尔，不记 SDP、候选、token、TURN 凭据、终端字节和映射载荷。

## 代码结构

```text
src/
  main.rs          入口：取命令、初始化日志、交给 cli
  cli/             args 解析、ui、devices（auth/list/菜单）、access（portmap/ssh/sftp/tunnel）
  cloud/           config、creds、签名 REST（api）、扫码登录（login）
  net/             signaling、ice、peer（被控端建连）
  net/viewer/      控制端：rtc（Message 27 客户端）、signal、ssh（russh）
  host/            run 的房间循环、terminal（TERM 帧 + PTY）、portmap（Message 27）
  proto/           wire（protobuf 读取）、envelope（外层信封）
```

协议行为对齐官方 host 与主程序，代码独立重写：字段号、帧布局、常量这类事实一致，实现与命名不复用。
字段号以 `proto/`、`host/`、`net/viewer/` 的代码为准，这里不再列一遍。

## 许可证

MIT，见 [LICENSE](LICENSE)。


## 致谢
- [VallierDev/uu-remote-linux](https://github.com/VallierDev/uu-remote-linux)
- 网易 UU 远程团队：做了一个好用的远程工具。
- [webrtc-rs](https://github.com/webrtc-rs/webrtc) 等开源项目。
