use iroh_lan::{RouterIp, network::Network};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if !self_runas::is_elevated() {
        self_runas::admin()?;
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_thread_ids(true)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let args_ref: Vec<&str> = args.iter().skip(1).map(String::as_str).collect();
    match args_ref.as_slice() {
        ["s", name, password] => {
            let network = Network::new(name, password).await?;

            while matches!(
                network.get_router_state().await?,
                RouterIp::NoIp | RouterIp::AquiringIp(_, _)
            ) {
                sleep(std::time::Duration::from_millis(500)).await;
            }

            println!("my ip is {:?}", network.get_router_state().await?);

            tokio::spawn(async move {
                loop {
                    println!(
                        "Network started with endpoint ID {:?}",
                        network.get_router_state().await
                    );
                    sleep(std::time::Duration::from_secs(5)).await;
                }
            });

            let _ = tokio::signal::ctrl_c().await;
        }
        ["j", name, password] => {
            todo!()
        }
        _ => eprintln!("unknown args: {args_ref:#?}"),
    }

    Ok(())
}
