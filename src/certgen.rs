//! Certificate generation for the push integrador mTLS.
//!
//! A small private CA lives on the VPS (next to `fdb-ingest`). It signs:
//!   - one **server** cert for the ingest endpoint (SAN = host/IP the agent dials),
//!   - one **client** cert per tenant (CN = tenant id) that the agent presents.
//!
//! The ingest's rustls verifier (see `tlsauth`) only trusts certs chained to this CA,
//! and derives the tenant from the client cert CN. No PostgreSQL credential is ever
//! involved here — the client cert is the agent's sole secret.

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};

/// A PEM cert + its PEM private key.
pub struct Pem {
    pub cert_pem: String,
    pub key_pem:  String,
}

/// Generate a fresh private CA (self-signed, can sign other certs).
pub fn generate_ca(common_name: &str) -> Result<Pem> {
    let key = KeyPair::generate().context("generate CA key")?;
    let mut params = CertificateParams::new(Vec::<String>::new()).context("CA params")?;
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let cert = params.self_signed(&key).context("self-sign CA")?;
    Ok(Pem { cert_pem: cert.pem(), key_pem: key.serialize_pem() })
}

/// Reload a CA cert+key from PEM into signing handles. The reconstructed
/// `Certificate` is only used as an issuer reference (its DN + key usages); the
/// child signature is produced with `ca_key`.
fn load_ca(ca_cert_pem: &str, ca_key_pem: &str) -> Result<(Certificate, KeyPair)> {
    let key = KeyPair::from_pem(ca_key_pem).context("parse CA key")?;
    let params = CertificateParams::from_ca_cert_pem(ca_cert_pem).context("parse CA cert")?;
    let cert = params.self_signed(&key).context("rebuild CA handle")?;
    Ok((cert, key))
}

/// Issue a client cert for `tenant` (CN = tenant), signed by the CA. The agent
/// presents this; the ingest maps CN → target database.
pub fn issue_client(ca_cert_pem: &str, ca_key_pem: &str, tenant: &str) -> Result<Pem> {
    let (ca, ca_key) = load_ca(ca_cert_pem, ca_key_pem)?;
    let key = KeyPair::generate().context("generate client key")?;
    let mut params = CertificateParams::new(Vec::<String>::new()).context("client params")?;
    params.distinguished_name.push(DnType::CommonName, tenant);
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let cert = params.signed_by(&key, &ca, &ca_key).context("sign client cert")?;
    Ok(Pem { cert_pem: cert.pem(), key_pem: key.serialize_pem() })
}

/// Issue a server cert for the ingest endpoint. `sans` are the hostnames/IPs the
/// agent will connect to (rustls auto-detects IP vs DNS). CN = first SAN.
pub fn issue_server(ca_cert_pem: &str, ca_key_pem: &str, sans: &[String]) -> Result<Pem> {
    anyhow::ensure!(!sans.is_empty(), "server cert needs at least one SAN (host/IP)");
    let (ca, ca_key) = load_ca(ca_cert_pem, ca_key_pem)?;
    let key = KeyPair::generate().context("generate server key")?;
    let mut params = CertificateParams::new(sans.to_vec()).context("server params")?;
    params.distinguished_name.push(DnType::CommonName, sans[0].clone());
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.signed_by(&key, &ca, &ca_key).context("sign server cert")?;
    Ok(Pem { cert_pem: cert.pem(), key_pem: key.serialize_pem() })
}
