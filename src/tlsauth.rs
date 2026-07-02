//! rustls mTLS plumbing for the push integrador.
//!
//! - `server_config`: ingest side. Requires a client cert chained to our private CA
//!   (`WebPkiClientVerifier`), and presents the ingest server cert.
//! - `client_config`: agent side. Trusts our CA for the server, and presents the
//!   tenant client cert.
//! - `tenant_from_cert`: pull the CN (tenant id) out of the verified peer cert.
//!
//! All validation happens here in Rust — no nginx/TLS-terminating proxy. A proxy, if
//! present, only forwards the TCP port (L4 passthrough).

use anyhow::{anyhow, Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::sync::Arc;

/// Install the ring crypto provider as the process default. Call once at startup,
/// before building any config. Idempotent.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn load_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut rd = std::io::Cursor::new(pem.as_bytes());
    let certs: Vec<_> = rustls_pemfile::certs(&mut rd)
        .collect::<std::result::Result<_, _>>()
        .context("parse certificate PEM")?;
    anyhow::ensure!(!certs.is_empty(), "no certificates found in PEM");
    Ok(certs)
}

fn load_key(pem: &str) -> Result<PrivateKeyDer<'static>> {
    let mut rd = std::io::Cursor::new(pem.as_bytes());
    rustls_pemfile::private_key(&mut rd)
        .context("parse private key PEM")?
        .ok_or_else(|| anyhow!("no private key found in PEM"))
}

fn root_store(ca_pem: &str) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for c in load_certs(ca_pem)? {
        roots.add(c).context("add CA to root store")?;
    }
    Ok(roots)
}

/// Build the ingest server config: mTLS, client cert must chain to `ca_pem`.
pub fn server_config(ca_pem: &str, server_cert_pem: &str, server_key_pem: &str) -> Result<Arc<ServerConfig>> {
    let roots = Arc::new(root_store(ca_pem)?);
    let verifier = rustls::server::WebPkiClientVerifier::builder(roots)
        .build()
        .context("build client cert verifier")?;
    let certs = load_certs(server_cert_pem)?;
    let key = load_key(server_key_pem)?;
    let cfg = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("server config with cert")?;
    Ok(Arc::new(cfg))
}

/// Build the agent client config: trust `ca_pem` for the server, present client cert.
pub fn client_config(ca_pem: &str, client_cert_pem: &str, client_key_pem: &str) -> Result<Arc<ClientConfig>> {
    let roots = root_store(ca_pem)?;
    let certs = load_certs(client_cert_pem)?;
    let key = load_key(client_key_pem)?;
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .context("client config with cert")?;
    Ok(Arc::new(cfg))
}

/// Extract the CN (tenant id) from a verified peer certificate.
pub fn tenant_from_cert(cert: &CertificateDer<'_>) -> Result<String> {
    let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref())
        .map_err(|e| anyhow!("parse peer cert: {e}"))?;
    let cn = parsed
        .subject()
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .ok_or_else(|| anyhow!("peer cert has no CN"))?;
    Ok(cn.to_string())
}
