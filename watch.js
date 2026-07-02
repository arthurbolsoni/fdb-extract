#!/usr/bin/env bun
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const TABLES = [
    "BAIRROS",
    "CIDADES",
    "CLASSFISCAL",
    "CLI_NAOVISITADOS",
    "CLI_VENDEDORES",
    "CLIENTES",
    "COMPRAS",
    "COMPRAS_PRODS",
    "DESCONTOS_QTDEMIN",
    "EMPRESAS",
    "ESTADOS",
    "FAMILIAS",
    "FORNECEDORES",
    "GRUPOS",
    "ITENS_OBRIG",
    "ITENS_OBRIG_GR",
    "ITENS_OBRIG_GR_PROD",
    "ITENS_OBRIG_SEG",
    "MARCAS",
    "NATOPERACAO",
    "PAISES",
    "PED_PRODS",
    "PEDIDOS",
    "PRODUTOS",
    "PROMOCOES",
    "PROMOCOES_CLI",
    "PROMOCOES_PRODS",
    "REDES",
    "ROTAS",
    "SEGMENTOS",
    "SUBGRUPOS",
    "TIPOOPERACAO",
    "TRANSPORTADORAS",
    "TROCAS",
    "VENDACOMBO",
    "VENDACOMBO_PART",
    "VENDACOMBO_PRODS",
    "VENDAS",
    "VENDAS_PRODS",
    "VENDEDORES",
    "VISITASREALIZADAS",
    "METAS",
];

const argv = process.argv.slice(2);
const flags = new Map();
for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
        const key = a.slice(2);
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith("--")) {
            flags.set(key, next);
            i++;
        } else {
            flags.set(key, true);
        }
    }
}

const config = {
    database:   flags.get("database")    ?? ".\\BANCO.FDB",
    interval:   flags.get("interval")    ?? "5",
    pgDatabase: flags.get("pg-database") ?? "replica_firebird",
    pgHost:     flags.get("pg-host")     ?? "localhost",
    pgUser:     flags.get("pg-user")     ?? "postgres",
    pgPassword: flags.get("pg-password") ?? "postgres",
    debug:      flags.get("debug")       ?? true,
    release:    flags.get("release")     ?? true,
};

const profile = config.release ? "release" : "debug";
const exe = resolve(__dirname, "target", profile, "fdb-extract.exe");

if (!existsSync(exe)) {
    console.log(`Binary not found: ${exe}`);
    console.log(`Building (cargo build --${profile})...`);
    const build = Bun.spawnSync(["cargo", "build", `--${profile}`], {
        cwd: __dirname,
        stdout: "inherit",
        stderr: "inherit",
    });
    if (build.exitCode !== 0) {
        console.error(`cargo build failed (exit ${build.exitCode})`);
        process.exit(build.exitCode ?? 1);
    }
}

const dbPath = resolve(__dirname, config.database);
if (!existsSync(dbPath)) {
    console.error(`Database file not found: ${dbPath}`);
    process.exit(1);
}

const args = [
    "--database",       config.database,
    "--watch",
    "--watch-interval", String(config.interval),
    "--pg-database",    config.pgDatabase,
    "--pg-host",        config.pgHost,
    "--pg-user",        config.pgUser,
    "--pg-password",    config.pgPassword,
    "--tables",         TABLES.join(","),
];

if (config.debug) args.push("--debug");

console.log(`==> ${exe}`);
console.log(`    database : ${config.database}`);
console.log(`    interval : ${config.interval}s`);
console.log(`    pg       : ${config.pgUser}@${config.pgHost}/${config.pgDatabase}`);
console.log(`    tables   : ${TABLES.length} tables`);
console.log("");

const child = spawn(exe, args, {
    cwd:   __dirname,
    stdio: "inherit",
});

const stop = (sig) => () => {
    if (!child.killed) child.kill(sig);
};
process.on("SIGINT",  stop("SIGINT"));
process.on("SIGTERM", stop("SIGTERM"));

child.on("exit", (code, signal) => {
    process.exit(code ?? (signal ? 1 : 0));
});
