import { spawnSync } from 'child_process';
import { join } from 'path';

const TABLES = [
    'BAIRROS',
    'CIDADES',
    'CLASSFISCAL',
    'CLI_NAOVISITADOS',
    'CLI_VENDEDORES',
    'CLIENTES',
    'COMPRAS',
    'COMPRAS_PRODS',
    'DESCONTOS_QTDEMIN',
    'EMPRESAS',
    'ESTADOS',
    'FAMILIAS',
    'FORNECEDORES',
    'GRUPOS',
    'ITENS_OBRIG',
    'ITENS_OBRIG_GR',
    'ITENS_OBRIG_GR_PROD',
    'ITENS_OBRIG_SEG',
    'MARCAS',
    'NATOPERACAO',
    'PAISES',
    'PED_PRODS',
    'PEDIDOS',
    'PRODUTOS',
    'PROMOCOES',
    'PROMOCOES_CLI',
    'PROMOCOES_PRODS',
    'REDES',
    'ROTAS',
    'SEGMENTOS',
    'SUBGRUPOS',
    'TIPOOPERACAO',
    'TRANSPORTADORAS',
    'TROCAS',
    'VENDACOMBO',
    'VENDACOMBO_PART',
    'VENDACOMBO_PRODS',
    'VENDAS',
    'VENDAS_PRODS',
    'VENDEDORES',
    'VISITASREALIZADAS',
    'METAS',
];

const DB_PATH = '.\\BANCO.FDB';
const EXE     = join(import.meta.dirname, 'target', 'release', 'fdb-extract.exe');

const res = spawnSync(EXE, [
    '-d', DB_PATH,
    '--pg-database', 'replica_firebird',
    '--pg-host',     'localhost',
    '--pg-port',     '5432',
    '--pg-user',     'postgres',
    '--pg-password', 'postgres',
    '--drop',
    '--unlogged',
    '--all-tables'
    // '--tables', ...TABLES,
], { stdio: 'inherit' });

process.exit(res.status ?? 1);
