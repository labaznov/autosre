//! Встроенный TLS веб-морды.
//!
//! Реверс-прокси на сервере без сети — ещё один компонент, который некому
//! ставить ([ADR-0028](../../../docs/adr/0028-bundle-and-systemd.md)). Пароль
//! дежурного по голому HTTP виден в сети целиком, поэтому HTTPS включён по
//! умолчанию: свой сертификат — по пути из настроек, нет своего — агент делает
//! самоподписанный на первом старте и предупреждает об этом.
//!
//! Самоподписанный сертификат — это предупреждение браузера, а не дыра:
//! соединение всё равно зашифровано. Убрать предупреждение можно, подложив
//! сертификат от своего удостоверяющего центра по тем же путям.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::Router;
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;

/// Отказы встроенного TLS.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("{path} не прочитан: {cause}")]
    Read {
        path: PathBuf,
        cause: std::io::Error,
    },
    #[error("{path} не записан: {cause}")]
    Write {
        path: PathBuf,
        cause: std::io::Error,
    },
    #[error("сертификат и ключ идут парой: {0} есть, а пары к нему нет")]
    Unpaired(PathBuf),
    #[error("сертификат или ключ не разобран: {0}")]
    Shape(String),
    #[error("самоподписанный сертификат не сделан: {0}")]
    Make(String),
}

/// Сколько дорисовывать текущие запросы при остановке.
const GRACE: Duration = Duration::from_secs(10);

/// Сертификат по указанным путям; нет обоих — делается самоподписанный.
///
/// Второе значение — сделан ли сертификат только что: об этом стоит сказать
/// в журнале громче, чем о прочитанном.
///
/// # Errors
/// [`TlsError`] на нечитаемом, непарном или неразбираемом файле, а также если
/// сделанный сертификат некуда записать.
pub async fn certificate(cert: &Path, key: &Path) -> Result<(RustlsConfig, bool), TlsError> {
    match (cert.is_file(), key.is_file()) {
        (true, true) => {
            let config = RustlsConfig::from_pem_file(cert, key)
                .await
                .map_err(|cause| TlsError::Shape(cause.to_string()))?;
            Ok((config, false))
        }
        (true, false) => Err(TlsError::Unpaired(cert.to_owned())),
        (false, true) => Err(TlsError::Unpaired(key.to_owned())),
        (false, false) => {
            let (pem, secret) = made()?;
            write(cert, pem.as_bytes(), 0o644)?;
            write(key, secret.as_bytes(), 0o600)?;
            let config = RustlsConfig::from_pem(pem.into_bytes(), secret.into_bytes())
                .await
                .map_err(|cause| TlsError::Shape(cause.to_string()))?;
            Ok((config, true))
        }
    }
}

/// Самоподписанный сертификат на `localhost`: пара PEM, сертификат и ключ.
fn made() -> Result<(String, String), TlsError> {
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .map_err(|cause| TlsError::Make(cause.to_string()))?;
    Ok((made.cert.pem(), made.signing_key.serialize_pem()))
}

/// Пишет файл с заданными правами, заводя каталог.
fn write(path: &Path, bytes: &[u8], mode: u32) -> Result<(), TlsError> {
    let failed = |cause| TlsError::Write {
        path: path.to_owned(),
        cause,
    };
    if let Some(directory) = path.parent() {
        std::fs::create_dir_all(directory).map_err(failed)?;
    }
    std::fs::write(path, bytes).map_err(failed)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(failed)?;
    }
    Ok(())
}

/// Обслуживает веб-морду по HTTPS до сигнала остановки.
///
/// # Errors
/// [`std::io::Error`], если слушать порт не вышло.
pub async fn serve(
    listener: TcpListener,
    config: RustlsConfig,
    router: Router,
    stop: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    // Tokio принимает только неблокирующий сокет; слушатель, собранный из
    // std, по умолчанию блокирующий.
    listener.set_nonblocking(true)?;
    let handle = Handle::new();
    let stopper = handle.clone();
    tokio::spawn(async move {
        stop.await;
        stopper.graceful_shutdown(Some(GRACE));
    });
    axum_server::from_tcp_rustls(listener, config)?
        .handle(handle)
        .serve(router.into_make_service())
        .await
}
