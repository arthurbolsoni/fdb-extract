import Firebird from 'node-firebird';
import pg from 'pg';
import { resolve } from 'path';

const useColor = process.stdout.isTTY && process.env.NO_COLOR === undefined;
const wrap = (open, close) => (s) => useColor ? `\x1b[${open}m${s}\x1b[${close}m` : String(s);
const c = {
    reset:   wrap(0, 0),
    bold:    wrap(1, 22),
    dim:     wrap(2, 22),
    red:     wrap(31, 39),
    green:   wrap(32, 39),
    yellow:  wrap(33, 39),
    blue:    wrap(34, 39),
    magenta: wrap(35, 39),
    cyan:    wrap(36, 39),
    gray:    wrap(90, 39),
    bgGreen: wrap(42, 49),
    bgRed:   wrap(41, 49),
};
const banner = (label, color) => color(c.bold(`\n${'═'.repeat(80)}\n  ${label}\n${'═'.repeat(80)}`));
const section = (label) => c.yellow(c.bold(`\n──── ${label} ────`));

const options = {
    host:     'localhost',
    port:     3050,
    database: resolve('./BANCO.FDB'),
    user:     'SYSDBA',
    password: 'masterkey',
    lowercase_keys: false,
};

const pgConfig = {
    host:     process.env.TEST_PG_HOST ?? 'localhost',
    port:     Number(process.env.TEST_PG_PORT ?? 5432),
    user:     process.env.TEST_PG_USER ?? 'postgres',
    password: process.env.TEST_PG_PASS ?? 'postgres',
    database: process.env.TEST_PG_DB ?? 'replica_firebird',
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function execSql(db, sql, params = []) {
    const paramsCopy = [...params];
    const sqlFull = sql.replace(/\?/g, () => {
        const v = paramsCopy.shift();
        return v === null ? 'NULL' : typeof v === 'string' ? `'${v}'` : v;
    });
    console.log(c.gray('\nSQL: ') + c.cyan(sqlFull));

    return new Promise((resolve, reject) => {
        db.query(sql, params, (err, result) => {
            if (err) return reject(err);
            console.log(c.green('OK'));
            resolve(result);
        });
    });
}

async function pgQueryOne(pgClient, sql, params) {
    const r = await pgClient.query(sql, params);
    return r.rows[0] ?? null;
}

const results = [];

async function waitFor(label, fn, { timeoutMs = 30_000, intervalMs = 500 } = {}) {
    const start = Date.now();
    let lastValue;
    while (Date.now() - start < timeoutMs) {
        lastValue = await fn();
        if (lastValue.ok) {
            const elapsed = Date.now() - start;
            console.log(
                `  ${c.blue('[PG]')} ${label}: ${c.green('OK')} ${c.gray(`(${elapsed}ms)`)} ${c.dim(lastValue.detail ?? '')}`
            );
            results.push({ status: 'PASS', label, elapsedMs: elapsed, detail: lastValue.detail ?? '' });
            return lastValue;
        }
        await sleep(intervalMs);
    }
    const elapsed = Date.now() - start;
    const detail = `last=${JSON.stringify(lastValue)}`;
    results.push({ status: 'FAIL', label, elapsedMs: elapsed, detail });
    throw new Error(`${c.red('[PG]')} ${label}: ${c.red('TIMEOUT')} after ${timeoutMs}ms — ${detail}`);
}

async function expectExists(pgClient, codcidade, expectedCity) {
    return waitFor(`exists codcidade=${codcidade} cidade='${expectedCity}'`, async () => {
        const row = await pgQueryOne(pgClient, 'SELECT codcidade, cidade FROM cidades WHERE codcidade = $1', [codcidade]);
        if (!row) return { ok: false, detail: 'not found' };
        if (row.cidade !== expectedCity) return { ok: false, detail: `cidade='${row.cidade}'` };
        return { ok: true, detail: `cidade='${row.cidade}'` };
    });
}

async function expectAbsent(pgClient, codcidade) {
    return waitFor(`absent codcidade=${codcidade}`, async () => {
        const row = await pgQueryOne(pgClient, 'SELECT codcidade FROM cidades WHERE codcidade = $1', [codcidade]);
        return row ? { ok: false, detail: 'still present' } : { ok: true };
    });
}

async function pgCount(pgClient) {
    const row = await pgQueryOne(pgClient, 'SELECT COUNT(*)::int AS c FROM cidades', []);
    return row.c;
}

async function expectCountDelta(pgClient, before, expectedDelta) {
    return waitFor(`count delta == ${expectedDelta} (before=${before})`, async () => {
        const now = await pgCount(pgClient);
        const delta = now - before;
        if (delta !== expectedDelta) return { ok: false, detail: `delta=${delta} (now=${now})` };
        return { ok: true, detail: `delta=${delta} (now=${now})` };
    });
}

async function expectBulkState(pgClient, codsPresent, codsAbsent, expectedCity) {
    return waitFor(
        `bulk state ${codsPresent.length} present='${expectedCity}' / ${codsAbsent.length} absent`,
        async () => {
            const presentRow = await pgQueryOne(
                pgClient,
                'SELECT COUNT(*)::int AS c FROM cidades WHERE codcidade = ANY($1::int[]) AND cidade = $2',
                [codsPresent, expectedCity],
            );
            if (presentRow.c !== codsPresent.length) {
                return { ok: false, detail: `present=${presentRow.c}/${codsPresent.length}` };
            }
            const absentRow = await pgQueryOne(
                pgClient,
                'SELECT COUNT(*)::int AS c FROM cidades WHERE codcidade = ANY($1::int[])',
                [codsAbsent],
            );
            if (absentRow.c !== 0) {
                return { ok: false, detail: `absent_still_present=${absentRow.c}/${codsAbsent.length}` };
            }
            return {
                ok: true,
                detail: `present=${presentRow.c} absent=${codsAbsent.length - absentRow.c}/${codsAbsent.length}`,
            };
        },
        { timeoutMs: 60_000 },
    );
}

function printOverview() {
    const passed = results.filter(r => r.status === 'PASS').length;
    const failed = results.filter(r => r.status === 'FAIL').length;
    const totalMs = results.reduce((a, r) => a + r.elapsedMs, 0);

    console.log(c.bold(c.magenta('\n' + '═'.repeat(80))));
    console.log(c.bold(c.magenta('  OVERVIEW')));
    console.log(c.bold(c.magenta('═'.repeat(80))));
    for (const r of results) {
        const mark = r.status === 'PASS' ? c.bgGreen(c.bold(' PASS ')) : c.bgRed(c.bold(' FAIL '));
        const time = c.gray(`${String(r.elapsedMs).padStart(5)}ms`);
        const lbl  = r.status === 'PASS' ? c.green(r.label) : c.red(r.label);
        const det  = r.detail ? c.dim(`  (${r.detail})`) : '';
        console.log(`${mark} ${time}  ${lbl}${det}`);
    }
    console.log(c.dim('─'.repeat(80)));
    const summary =
        `TOTAL: ${c.bold(results.length)} | ` +
        `${c.green(`PASS: ${passed}`)} | ` +
        `${failed > 0 ? c.red(`FAIL: ${failed}`) : c.gray(`FAIL: ${failed}`)} | ` +
        `TIME: ${c.gray(`${totalMs}ms`)}`;
    console.log(summary);
    console.log(c.bold(c.magenta('═'.repeat(80))));
}

Firebird.attach(options, async (err, db) => {
    if (err) { console.error(c.red('Connection error:'), err); process.exit(1); }

    const pgClient = new pg.Client(pgConfig);
    await pgClient.connect();
    console.log(c.green('PG connected:'), c.cyan(`${pgConfig.host}:${pgConfig.port}/${pgConfig.database}`));

    try {
        const rows = await new Promise((res, rej) =>
            db.query('SELECT * FROM CIDADES WHERE CIDADE = ?', ['Laguna5'], (e, r) => e ? rej(e) : res(r))
        );
        if (!rows || rows.length === 0) throw new Error('Laguna5 not found');
        const src = rows[0];
        console.log(c.gray('Source record:'), src);

        // --- max cod ---
        const maxRows = await new Promise((res, rej) =>
            db.query('SELECT MAX(CODCIDADE) AS MAXCOD FROM CIDADES', [], (e, r) => e ? rej(e) : res(r))
        );
        const base = maxRows[0].MAXCOD;
        const cod1  = base + 1;
        const cod2  = base + 2;

        console.log(banner('TEST 1: 2 inserts in 1 transaction, then delete+update', c.cyan));
        console.log(section('BEGIN TRANSACTION (2 inserts)'));
        await new Promise((res, rej) => db.transaction(Firebird.ISOLATION_READ_COMMITTED, async (err, transaction) => {
            if (err) return rej(err);

            const insertSql = `INSERT INTO CIDADES (CODCIDADE, CIDADE, UF, CODPAIS, CEPPADRAO, NROPOPULACAO, TIPOPRACA, CODIBGE, NROREGIAO, VALOR_MIN, NROMICRO, DDD, DISTANCIAKM, PERCISS, CODVENDEDOR, DIASENTREGA, COD_AREA_GEO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

            const makeParams = (cod) => [
                cod,
                src.CIDADE,
                src.UF,
                src.CODPAIS,
                src.CEPPADRAO    ?? null,
                src.NROPOPULACAO ?? null,
                src.TIPOPRACA    ?? null,
                src.CODIBGE      ?? null,
                src.NROREGIAO    ?? null,
                src.VALOR_MIN    ?? null,
                src.NROMICRO     ?? null,
                src.DDD          ?? null,
                src.DISTANCIAKM  ?? null,
                src.PERCISS      ?? null,
                src.CODVENDEDOR  ?? null,
                src.DIASENTREGA  ?? null,
                src.COD_AREA_GEO ?? null,
            ];

            const logSql = (sql, params) => {
                const copy = [...params];
                const full = sql.replace(/\?/g, () => {
                    const v = copy.shift();
                    return v === null ? 'NULL' : typeof v === 'string' ? `'${v}'` : v;
                });
                console.log(c.gray('\nSQL: ') + c.cyan(full));
            };

            const p1 = makeParams(cod1);
            logSql(insertSql, p1);
            await new Promise((r2, j2) => transaction.query(insertSql, p1, (e) => e ? j2(e) : (console.log(c.green('OK')), r2())));

            const p2 = makeParams(cod2);
            logSql(insertSql, p2);
            await new Promise((r2, j2) => transaction.query(insertSql, p2, (e) => e ? j2(e) : (console.log(c.green('OK')), r2())));

            transaction.commit((e) => e ? rej(e) : (console.log(c.green(c.bold('\nCOMMIT OK')) + c.gray(` — inserted ${cod1} and ${cod2}`)), res()));
        }));

        console.log(section('VALIDATE PG (after inserts)'));
        await expectExists(pgClient, cod1, src.CIDADE);
        await expectExists(pgClient, cod2, src.CIDADE);

        console.log(c.dim('\nWaiting 2 seconds...'));
        await sleep(2000);

        console.log(section('DELETE'));
        await execSql(db, `DELETE FROM CIDADES WHERE CODCIDADE = ?`, [cod1]);

        console.log(section('UPDATE'));
        await execSql(db, `UPDATE CIDADES SET CIDADE = ? WHERE CODCIDADE = ?`, ['Laguna_edited', cod2]);

        console.log(section('VALIDATE PG (after delete + update)'));
        await expectAbsent(pgClient, cod1);
        await expectExists(pgClient, cod2, 'Laguna_edited');

        console.log(c.dim('\nWaiting 2 seconds...'));
        await sleep(2000);

        const maxRows2 = await new Promise((res, rej) =>
            db.query('SELECT MAX(CODCIDADE) AS MAXCOD FROM CIDADES', [], (e, r) => e ? rej(e) : res(r))
        );
        const base2 = maxRows2[0].MAXCOD;
        const cod3  = base2 + 1;
        const cod4  = base2 + 2;

        console.log(banner('TEST 2: 2 inserts + update + delete in 1 transaction', c.cyan));
        const countBefore = await pgCount(pgClient);
        console.log(c.gray(`PG count(cidades) before tx: `) + c.bold(countBefore));
        console.log(section('BEGIN TRANSACTION (2 inserts + update + delete)'));
        await new Promise((res, rej) => db.transaction(Firebird.ISOLATION_READ_COMMITTED, async (err, transaction) => {
            if (err) return rej(err);

            const insertSql = `INSERT INTO CIDADES (CODCIDADE, CIDADE, UF, CODPAIS, CEPPADRAO, NROPOPULACAO, TIPOPRACA, CODIBGE, NROREGIAO, VALOR_MIN, NROMICRO, DDD, DISTANCIAKM, PERCISS, CODVENDEDOR, DIASENTREGA, COD_AREA_GEO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

            const makeParams = (cod) => [
                cod,
                src.CIDADE,
                src.UF,
                src.CODPAIS,
                src.CEPPADRAO    ?? null,
                src.NROPOPULACAO ?? null,
                src.TIPOPRACA    ?? null,
                src.CODIBGE      ?? null,
                src.NROREGIAO    ?? null,
                src.VALOR_MIN    ?? null,
                src.NROMICRO     ?? null,
                src.DDD          ?? null,
                src.DISTANCIAKM  ?? null,
                src.PERCISS      ?? null,
                src.CODVENDEDOR  ?? null,
                src.DIASENTREGA  ?? null,
                src.COD_AREA_GEO ?? null,
            ];

            const logSql = (sql, params) => {
                const copy = [...params];
                const full = sql.replace(/\?/g, () => {
                    const v = copy.shift();
                    return v === null ? 'NULL' : typeof v === 'string' ? `'${v}'` : v;
                });
                console.log(c.gray('\nSQL: ') + c.cyan(full));
            };

            const runQuery = (sql, params) => {
                logSql(sql, params);
                return new Promise((r2, j2) => transaction.query(sql, params, (e) => e ? j2(e) : (console.log(c.green('OK')), r2())));
            };

            try {
                await runQuery(insertSql, makeParams(cod3));
                await runQuery(insertSql, makeParams(cod4));
                await runQuery(`UPDATE CIDADES SET CIDADE = ? WHERE CODCIDADE = ?`, ['Laguna_tx_edited', cod3]);
                await runQuery(`DELETE FROM CIDADES WHERE CODCIDADE = ?`, [cod4]);

                transaction.commit((e) => e ? rej(e) : (
                    console.log(c.green(c.bold('\nCOMMIT OK')) + c.gray(` — inserted ${cod3} ${cod4} | updated ${cod3} | deleted ${cod4}`)),
                    res()
                ));
            } catch (e) {
                transaction.rollback(() => rej(e));
            }
        }));

        console.log(section('VALIDATE PG (after tx insert+update+delete)'));
        await expectExists(pgClient, cod3, 'Laguna_tx_edited');
        await expectAbsent(pgClient, cod4);
        await expectCountDelta(pgClient, countBefore, 1);

        console.log(c.dim('\nWaiting 2 seconds...'));
        await sleep(2000);

        const maxRows3 = await new Promise((res, rej) =>
            db.query('SELECT MAX(CODCIDADE) AS MAXCOD FROM CIDADES', [], (e, r) => e ? rej(e) : res(r))
        );
        const base3 = maxRows3[0].MAXCOD;
        const BULK_TOTAL  = 200;
        const BULK_DELETE = 50;
        const BULK_KEEP   = BULK_TOTAL - BULK_DELETE;
        const bulkCods    = Array.from({ length: BULK_TOTAL }, (_, i) => base3 + 1 + i);
        const deleteCods  = bulkCods.slice(0, BULK_DELETE);
        const keepCods    = bulkCods.slice(BULK_DELETE);
        const bulkCity    = 'Laguna_bulk_edited';

        console.log(banner(`TEST 3: ${BULK_TOTAL} inserts + ${BULK_DELETE} deletes + ${BULK_KEEP} updates in 1 transaction`, c.magenta));
        const countBefore3 = await pgCount(pgClient);
        console.log(c.gray('PG count(cidades) before tx: ') + c.bold(countBefore3));
        console.log(c.gray('Range:  ') + c.cyan(`${bulkCods[0]}..${bulkCods[bulkCods.length - 1]}`));
        console.log(c.gray('Delete: ') + c.red(`${deleteCods[0]}..${deleteCods[deleteCods.length - 1]}`));
        console.log(c.gray('Update: ') + c.yellow(`${keepCods[0]}..${keepCods[keepCods.length - 1]}`) + c.gray(' → cidade=') + c.cyan(`'${bulkCity}'`));

        const txStart = Date.now();
        await new Promise((res, rej) => db.transaction(Firebird.ISOLATION_READ_COMMITTED, async (err, transaction) => {
            if (err) return rej(err);

            const insertSql = `INSERT INTO CIDADES (CODCIDADE, CIDADE, UF, CODPAIS, CEPPADRAO, NROPOPULACAO, TIPOPRACA, CODIBGE, NROREGIAO, VALOR_MIN, NROMICRO, DDD, DISTANCIAKM, PERCISS, CODVENDEDOR, DIASENTREGA, COD_AREA_GEO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

            const makeParams = (cod) => [
                cod,
                src.CIDADE,
                src.UF,
                src.CODPAIS,
                src.CEPPADRAO    ?? null,
                src.NROPOPULACAO ?? null,
                src.TIPOPRACA    ?? null,
                src.CODIBGE      ?? null,
                src.NROREGIAO    ?? null,
                src.VALOR_MIN    ?? null,
                src.NROMICRO     ?? null,
                src.DDD          ?? null,
                src.DISTANCIAKM  ?? null,
                src.PERCISS      ?? null,
                src.CODVENDEDOR  ?? null,
                src.DIASENTREGA  ?? null,
                src.COD_AREA_GEO ?? null,
            ];

            const runQuiet = (sql, params) =>
                new Promise((r2, j2) => transaction.query(sql, params, (e) => e ? j2(e) : r2()));

            try {
                console.log(c.green(`\n→ INSERT ${BULK_TOTAL} rows...`));
                const tIns = Date.now();
                for (const cod of bulkCods) {
                    await runQuiet(insertSql, makeParams(cod));
                }
                console.log(c.gray(`  inserted `) + c.green(BULK_TOTAL) + c.gray(` in ${Date.now() - tIns}ms`));

                console.log(c.red(`\n→ DELETE ${BULK_DELETE} rows...`));
                const tDel = Date.now();
                await runQuiet(
                    `DELETE FROM CIDADES WHERE CODCIDADE BETWEEN ? AND ?`,
                    [deleteCods[0], deleteCods[deleteCods.length - 1]],
                );
                console.log(c.gray(`  deleted `) + c.red(BULK_DELETE) + c.gray(` in ${Date.now() - tDel}ms`));

                console.log(c.yellow(`\n→ UPDATE ${BULK_KEEP} rows...`));
                const tUpd = Date.now();
                await runQuiet(
                    `UPDATE CIDADES SET CIDADE = ? WHERE CODCIDADE BETWEEN ? AND ?`,
                    [bulkCity, keepCods[0], keepCods[keepCods.length - 1]],
                );
                console.log(c.gray(`  updated `) + c.yellow(BULK_KEEP) + c.gray(` in ${Date.now() - tUpd}ms`));

                transaction.commit((e) => {
                    if (e) return rej(e);
                    console.log(c.green(c.bold('\nCOMMIT OK')) + c.gray(` — bulk tx applied in ${Date.now() - txStart}ms`));
                    res();
                });
            } catch (e) {
                transaction.rollback(() => rej(e));
            }
        }));

        console.log(section('VALIDATE PG (after bulk tx)'));
        await expectBulkState(pgClient, keepCods, deleteCods, bulkCity);
        await expectCountDelta(pgClient, countBefore3, BULK_KEEP);

    } catch (e) {
        console.error(c.red(c.bold('Error:')), c.red(e.message ?? e));
    } finally {
        printOverview();
        await pgClient.end().catch(() => {});
        db.detach();
        const failed = results.filter(r => r.status === 'FAIL').length;
        process.exit(failed > 0 ? 1 : 0);
    }
});
