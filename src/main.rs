//! uu-minimal：登录 + 组网 + 终端 + 端口映射的最小骨架。
//!
//! ```text
//! uu-minimal login   # 扫码登录，凭据写入 ~/.config/uuremote/config.json
//! uu-minimal run     # 建房 → 连信令 → 回 answer，并服务终端与端口映射
//! ```
//!
//! 不做的事：画面采集/编码、输入注入、文件传输、剪贴板、多显示器、掉线重连策略。

mod cli;
mod cloud;
mod host;
mod net;
mod proto;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("run");
    let rest: &[String] = if args.is_empty() { &[] } else { &args[1..] };
    cli::init_logger(cli::log_filter(command));
    if let Err(error) = cli::dispatch(command, rest).await {
        eprintln!("错误: {error}");
        std::process::exit(1);
    }
}
