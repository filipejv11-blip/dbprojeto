"""
DB Diagnóstico v2.0 — Engine de Diagnóstico
Handles: connections, checks, history, profiles, scheduling
"""

import sqlite3
import threading
import time
import json
import os
import hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Callable


# ── Data Models ─────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    categoria: str
    nome: str
    status: str        # OK | AVISO | ERRO | INFO
    detalhe: str
    valor: Any = None
    ts: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self):
        return asdict(self)


@dataclass
class ConexaoConfig:
    tipo: str          # sqlserver | mysql | postgresql | sqlite
    nome: str = ""     # profile name
    host: str = "localhost"
    porta: int = 0
    banco: str = ""
    usuario: str = ""
    senha: str = ""
    arquivo: str = ""  # SQLite only

    def to_dict(self):
        d = asdict(self)
        d.pop("senha", None)   # never persist password
        return d

    @staticmethod
    def from_dict(d: dict) -> "ConexaoConfig":
        return ConexaoConfig(**{k: v for k, v in d.items() if k in ConexaoConfig.__dataclass_fields__})


@dataclass
class DiagnosticoSession:
    id: str
    profile_name: str
    db_type: str
    started_at: str
    finished_at: str
    total: int
    erros: int
    avisos: int
    ok: int
    info: int
    resultados: List[CheckResult] = field(default_factory=list)


# ── Persistence (local SQLite metadata store) ────────────────────────────────

class Store:
    """Stores profiles and history in a local metadata DB."""

    def __init__(self, path: str = "db_diagnostico_data.db"):
        self.path = path
        self._init()

    def _conn(self):
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        return c

    def _init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS profiles (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    name    TEXT UNIQUE NOT NULL,
                    data    TEXT NOT NULL,
                    updated TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id           TEXT PRIMARY KEY,
                    profile_name TEXT,
                    db_type      TEXT,
                    started_at   TEXT,
                    finished_at  TEXT,
                    total        INTEGER,
                    erros        INTEGER,
                    avisos       INTEGER,
                    ok           INTEGER,
                    info         INTEGER,
                    resultados   TEXT
                );
            """)

    # Profiles ---------------------------------------------------------------
    def save_profile(self, cfg: ConexaoConfig):
        with self._conn() as c:
            data = json.dumps(cfg.to_dict(), ensure_ascii=False)
            c.execute("""
                INSERT INTO profiles(name, data, updated) VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE SET data=excluded.data, updated=excluded.updated
            """, (cfg.nome, data, datetime.now().isoformat()))

    def delete_profile(self, name: str):
        with self._conn() as c:
            c.execute("DELETE FROM profiles WHERE name=?", (name,))

    def list_profiles(self) -> List[ConexaoConfig]:
        with self._conn() as c:
            rows = c.execute("SELECT data FROM profiles ORDER BY updated DESC").fetchall()
        return [ConexaoConfig.from_dict(json.loads(r["data"])) for r in rows]

    # Sessions ---------------------------------------------------------------
    def save_session(self, s: DiagnosticoSession):
        with self._conn() as c:
            c.execute("""
                INSERT OR REPLACE INTO sessions
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (s.id, s.profile_name, s.db_type, s.started_at, s.finished_at,
                  s.total, s.erros, s.avisos, s.ok, s.info,
                  json.dumps([r.to_dict() for r in s.resultados], ensure_ascii=False)))

    def list_sessions(self, limit: int = 50) -> List[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT id,profile_name,db_type,started_at,finished_at,total,erros,avisos,ok,info "
                "FROM sessions ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_session_results(self, session_id: str) -> List[CheckResult]:
        with self._conn() as c:
            row = c.execute("SELECT resultados FROM sessions WHERE id=?", (session_id,)).fetchone()
        if not row:
            return []
        data = json.loads(row["resultados"])
        return [CheckResult(**d) for d in data]

    def delete_session(self, session_id: str):
        with self._conn() as c:
            c.execute("DELETE FROM sessions WHERE id=?", (session_id,))


# ── DB Connector ─────────────────────────────────────────────────────────────

class DBConnector:
    PORTAS = {"sqlserver": 1433, "mysql": 3306, "postgresql": 5432}

    def __init__(self, cfg: ConexaoConfig):
        self.cfg = cfg
        self.conn = None
        self._lock = threading.Lock()

    def connect(self) -> tuple[bool, str]:
        cfg = self.cfg
        try:
            if cfg.tipo == "sqlite":
                if not cfg.arquivo:
                    raise ValueError("Informe o caminho do arquivo .db")
                if not os.path.exists(cfg.arquivo):
                    raise FileNotFoundError(f"Arquivo não encontrado: {cfg.arquivo}")
                self.conn = sqlite3.connect(cfg.arquivo, check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
            elif cfg.tipo == "mysql":
                import mysql.connector
                self.conn = mysql.connector.connect(
                    host=cfg.host, port=cfg.porta or 3306,
                    database=cfg.banco, user=cfg.usuario, password=cfg.senha,
                    connection_timeout=10, autocommit=True
                )
            elif cfg.tipo == "postgresql":
                import psycopg2
                self.conn = psycopg2.connect(
                    host=cfg.host, port=cfg.porta or 5432,
                    dbname=cfg.banco, user=cfg.usuario, password=cfg.senha,
                    connect_timeout=10
                )
            elif cfg.tipo == "sqlserver":
                import pyodbc
                drv = "ODBC Driver 17 for SQL Server"
                cs = (f"DRIVER={{{drv}}};SERVER={cfg.host},{cfg.porta or 1433};"
                      f"DATABASE={cfg.banco};UID={cfg.usuario};PWD={cfg.senha};"
                      "Connection Timeout=10;")
                self.conn = pyodbc.connect(cs)
            else:
                raise ValueError(f"Tipo desconhecido: {cfg.tipo}")
            return True, "Conexão estabelecida"
        except Exception as e:
            return False, str(e)

    def close(self):
        if self.conn:
            try: self.conn.close()
            except: pass
            self.conn = None


class _CursorResult:
    """
    Materializa os resultados do cursor dentro do lock do DBConnector,
    evitando que threads concorrentes misturem resultados de queries diferentes.
    """
    def __init__(self, cursor):
        try:
            self._rows = cursor.fetchall()
        except Exception:
            self._rows = []
        self._pos = 0

    def fetchall(self):
        return self._rows

    def fetchone(self):
        if self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            return row
        return None

    def __iter__(self):
        return iter(self._rows)

    def query(self, sql: str, params=None):
        # FIX: o cursor deve ser criado, executado e os resultados buscados
        # dentro do lock para evitar mistura de resultados entre threads.
        # Retornamos um objeto simples com os dados já materializados.
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(sql, params or [])
            return _CursorResult(cur)


# ── Checks ───────────────────────────────────────────────────────────────────

class CheckRunner:
    """Runs individual diagnostic checks against a live connection."""

    CHECKS_AVAILABLE = {
        "conexao":     "Conexão e Disponibilidade",
        "versao":      "Versão do Servidor",
        "integridade": "Integridade dos Dados",
        "duplicatas":  "Chaves Primárias Duplicadas",
        "performance": "Performance e Índices",
        "espaco":      "Espaço em Disco / Tabelas",
    }

    def __init__(self, connector: DBConnector):
        self.db = connector
        self.tipo = connector.cfg.tipo

    # helpers ----------------------------------------------------------------
    def _tabelas(self) -> List[str]:
        t = self.tipo
        if t == "sqlite":
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        elif t == "mysql":
            sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()"
        elif t == "postgresql":
            sql = "SELECT tablename FROM pg_tables WHERE schemaname='public'"
        else:
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        return [r[0] for r in self.db.query(sql).fetchall()]

    # ── conexao ─────────────────────────────────────────────────────────────
    def check_conexao(self) -> List[CheckResult]:
        """Verifica se a conexão já estabelecida está responsiva e mede latência."""
        results = []
        try:
            t0 = time.time()
            # FIX: não reconecta — usa a conexão já aberta pelo DBConnector.
            # Faz um ping leve para medir latência real.
            t = self.tipo
            if t == "sqlite":
                self.db.query("SELECT 1").fetchone()
            elif t == "mysql":
                self.db.query("SELECT 1").fetchone()
            elif t == "postgresql":
                self.db.query("SELECT 1").fetchone()
            elif t == "sqlserver":
                self.db.query("SELECT 1").fetchone()
            lat = round((time.time() - t0) * 1000, 1)
            results.append(CheckResult("Conexão", "Disponibilidade", "OK",
                                       f"Conexão responsiva — latência {lat} ms", lat))
        except Exception as e:
            results.append(CheckResult("Conexão", "Disponibilidade", "ERRO", str(e)))
        return results

    # ── versao ──────────────────────────────────────────────────────────────
    def check_versao(self) -> List[CheckResult]:
        try:
            t = self.tipo
            if t == "sqlite":
                v = f"SQLite {sqlite3.sqlite_version}"
            elif t == "mysql":
                v = self.db.query("SELECT VERSION()").fetchone()[0]
            elif t == "postgresql":
                v = self.db.query("SELECT version()").fetchone()[0].split(",")[0]
            else:
                v = self.db.query("SELECT @@VERSION").fetchone()[0].split("\n")[0][:80]
            return [CheckResult("Conexão", "Versão do Servidor", "INFO", v)]
        except Exception as e:
            return [CheckResult("Conexão", "Versão do Servidor", "AVISO", str(e))]

    # ── integridade ─────────────────────────────────────────────────────────
    def check_integridade(self) -> List[CheckResult]:
        results = []
        try:
            tabelas = self._tabelas()
            results.append(CheckResult("Integridade", "Tabelas encontradas", "INFO",
                                       f"{len(tabelas)} tabelas no banco", len(tabelas)))
            for tab in tabelas[:40]:
                try:
                    cnt = self.db.query(f'SELECT COUNT(*) FROM "{tab}"').fetchone()[0]
                    results.append(CheckResult("Integridade", f"Registros: {tab}", "INFO",
                                               f"{cnt:,} linhas", cnt))
                    if self.tipo == "sqlite":
                        res = self.db.query(f'PRAGMA integrity_check("{tab}")').fetchone()[0]
                        st = "OK" if res == "ok" else "ERRO"
                        results.append(CheckResult("Integridade", f"integrity_check({tab})", st, res))
                except Exception as e:
                    results.append(CheckResult("Integridade", f"Leitura: {tab}", "AVISO", str(e)))
        except Exception as e:
            results.append(CheckResult("Integridade", "Leitura de tabelas", "ERRO", str(e)))
        return results

    # ── duplicatas ──────────────────────────────────────────────────────────
    def check_duplicatas(self) -> List[CheckResult]:
        results = []
        try:
            tabelas = self._tabelas()
            for tab in tabelas[:30]:
                try:
                    cols_pk = self._pk_cols(tab)
                    if not cols_pk:
                        results.append(CheckResult("Integridade", f"PK: {tab}", "AVISO",
                                                   "Tabela sem chave primária definida"))
                        continue
                    pk_str = ", ".join(f'"{c}"' for c in cols_pk)
                    sql = (f'SELECT {pk_str}, COUNT(*) n FROM "{tab}" '
                           f'GROUP BY {pk_str} HAVING COUNT(*) > 1 LIMIT 10')
                    dups = self.db.query(sql).fetchall()
                    if dups:
                        results.append(CheckResult("Integridade", f"Duplicatas PK: {tab}", "ERRO",
                                                   f"{len(dups)} grupos de PK duplicada"))
                    else:
                        results.append(CheckResult("Integridade", f"Duplicatas PK: {tab}", "OK",
                                                   "Sem duplicatas de chave primária"))
                except Exception as e:
                    results.append(CheckResult("Integridade", f"PK check: {tab}", "AVISO", str(e)))
        except Exception as e:
            results.append(CheckResult("Integridade", "Verificação de PKs", "ERRO", str(e)))
        return results

    def _pk_cols(self, tab: str) -> List[str]:
        t = self.tipo
        try:
            if t == "sqlite":
                return [r[1] for r in self.db.query(f'PRAGMA table_info("{tab}")').fetchall() if r[5] > 0]
            elif t == "mysql":
                return [r[0] for r in self.db.query(
                    "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                    f"WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='{tab}' AND CONSTRAINT_NAME='PRIMARY'"
                ).fetchall()]
            elif t == "postgresql":
                return [r[0] for r in self.db.query(
                    "SELECT a.attname FROM pg_index i "
                    "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                    f"WHERE i.indrelid='{tab}'::regclass AND i.indisprimary"
                ).fetchall()]
        except: pass
        return []

    # ── performance ─────────────────────────────────────────────────────────
    def check_performance(self) -> List[CheckResult]:
        results = []
        try:
            t = self.tipo
            if t == "sqlite":
                wal = self.db.query("PRAGMA journal_mode").fetchone()[0]
                st = "OK" if wal == "wal" else "AVISO"
                results.append(CheckResult("Performance", "Journal Mode", st,
                                           f"Modo atual: {wal} (WAL recomendado)"))
                cs = self.db.query("PRAGMA cache_size").fetchone()[0]
                results.append(CheckResult("Performance", "Cache Size", "INFO",
                                           f"{abs(cs)} páginas ({abs(cs)*4} KB aprox.)", abs(cs)))
                for tab in self._tabelas():
                    idxs = self.db.query(f'PRAGMA index_list("{tab}")').fetchall()
                    if not idxs:
                        results.append(CheckResult("Performance", f"Índices: {tab}", "AVISO",
                                                   "Sem índices — full scan em buscas"))
                    else:
                        results.append(CheckResult("Performance", f"Índices: {tab}", "OK",
                                                   f"{len(idxs)} índice(s) definido(s)", len(idxs)))

            elif t == "mysql":
                slow = self.db.query("SHOW VARIABLES LIKE 'slow_query_log'").fetchone()
                if slow:
                    st = "OK" if slow[1] == "ON" else "AVISO"
                    results.append(CheckResult("Performance", "Slow Query Log", st,
                                               f"slow_query_log = {slow[1]}"))
                rows = self.db.query(
                    "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA=DATABASE() AND ENGINE!='InnoDB' AND ENGINE IS NOT NULL"
                ).fetchall()
                if rows:
                    for r in rows:
                        results.append(CheckResult("Performance", f"Engine: {r[0]}", "AVISO",
                                                   f"Usando {r[1]} (InnoDB recomendado)"))
                else:
                    results.append(CheckResult("Performance", "Engines das tabelas", "OK",
                                               "Todas as tabelas usam InnoDB"))
                idx_cnt = self.db.query(
                    "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE()"
                ).fetchone()[0]
                results.append(CheckResult("Performance", "Total de índices", "INFO",
                                           f"{idx_cnt} índices no banco", idx_cnt))

            elif t == "postgresql":
                dead = self.db.query(
                    "SELECT relname, n_dead_tup FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 10"
                ).fetchall()
                for r in dead:
                    if r[1] > 10000:
                        results.append(CheckResult("Performance", f"Dead tuples: {r[0]}", "AVISO",
                                                   f"{r[1]:,} tuplas mortas — VACUUM recomendado", r[1]))
                    else:
                        results.append(CheckResult("Performance", f"Dead tuples: {r[0]}", "OK",
                                                   f"{r[1]:,} tuplas mortas", r[1]))
                seq = self.db.query(
                    "SELECT relname, seq_scan, idx_scan FROM pg_stat_user_tables "
                    "WHERE seq_scan > idx_scan AND seq_scan > 100 ORDER BY seq_scan DESC LIMIT 5"
                ).fetchall()
                for r in seq:
                    results.append(CheckResult("Performance", f"Seq scan alto: {r[0]}", "AVISO",
                                               f"{r[1]:,} seq scans vs {r[2]:,} index scans"))

            elif t == "sqlserver":
                frags = self.db.query(
                    "SELECT OBJECT_NAME(ips.object_id) AS t, i.name AS n, "
                    "ips.avg_fragmentation_in_percent AS f "
                    "FROM sys.dm_db_index_physical_stats(DB_ID(),NULL,NULL,NULL,'LIMITED') ips "
                    "JOIN sys.indexes i ON ips.object_id=i.object_id AND ips.index_id=i.index_id "
                    "WHERE ips.avg_fragmentation_in_percent>30 AND i.name IS NOT NULL"
                ).fetchall()
                if frags:
                    for r in frags:
                        results.append(CheckResult("Performance", f"Fragmentação: {r[1]}", "AVISO",
                                                   f"Tabela {r[0]} — {r[2]:.1f}% (REBUILD recomendado)", r[2]))
                else:
                    results.append(CheckResult("Performance", "Fragmentação de índices", "OK",
                                               "Nenhum índice com fragmentação > 30%"))
        except Exception as e:
            results.append(CheckResult("Performance", "Análise de performance", "AVISO", str(e)))
        return results

    # ── espaco ──────────────────────────────────────────────────────────────
    def check_espaco(self) -> List[CheckResult]:
        results = []
        try:
            t = self.tipo
            if t == "sqlite":
                # FIX: self.db é o DBConnector — self.db.cfg.arquivo é sempre correto
                arq = self.db.cfg.arquivo
                if arq and os.path.exists(arq):
                    mb = os.path.getsize(arq) / 1024 / 1024
                    st = "AVISO" if mb > 500 else "OK"
                    results.append(CheckResult("Espaço", "Arquivo .db", st,
                                               f"{mb:.2f} MB", round(mb, 2)))
                page_sz = self.db.query("PRAGMA page_size").fetchone()[0]
                page_cnt = self.db.query("PRAGMA page_count").fetchone()[0]
                free = self.db.query("PRAGMA freelist_count").fetchone()[0]
                results.append(CheckResult("Espaço", "Páginas totais", "INFO",
                                           f"{page_cnt:,} páginas × {page_sz}B = {page_cnt*page_sz/1024/1024:.1f} MB",
                                           page_cnt))
                if free > 0:
                    results.append(CheckResult("Espaço", "Páginas livres (fragmentação)", "AVISO",
                                               f"{free:,} páginas livres — execute VACUUM para recuperar espaço", free))

            elif t == "mysql":
                rows = self.db.query(
                    "SELECT TABLE_NAME, ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024,2) AS mb, "
                    "TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
                    "ORDER BY mb DESC LIMIT 15"
                ).fetchall()
                for r in rows:
                    mb = r[1] or 0
                    st = "AVISO" if mb > 1000 else "INFO"
                    results.append(CheckResult("Espaço", f"Tabela: {r[0]}", st,
                                               f"{mb} MB — ~{r[2] or 0:,} linhas", mb))

            elif t == "postgresql":
                rows = self.db.query(
                    "SELECT tablename, "
                    "pg_size_pretty(pg_total_relation_size(tablename::regclass)) AS sz, "
                    "pg_total_relation_size(tablename::regclass) AS bytes "
                    "FROM pg_tables WHERE schemaname='public' "
                    "ORDER BY bytes DESC LIMIT 15"
                ).fetchall()
                for r in rows:
                    mb = r[2] / 1024 / 1024
                    st = "AVISO" if mb > 1000 else "INFO"
                    results.append(CheckResult("Espaço", f"Tabela: {r[0]}", st, str(r[1]), round(mb, 2)))

            elif t == "sqlserver":
                rows = self.db.query(
                    "SELECT TOP 15 t.name, ROUND(SUM(a.total_pages)*8/1024.0,2) AS mb "
                    "FROM sys.tables t "
                    "JOIN sys.indexes i ON t.object_id=i.object_id "
                    "JOIN sys.partitions p ON i.object_id=p.object_id AND i.index_id=p.index_id "
                    "JOIN sys.allocation_units a ON p.partition_id=a.container_id "
                    "GROUP BY t.name ORDER BY mb DESC"
                ).fetchall()
                for r in rows:
                    mb = r[1] or 0
                    st = "AVISO" if mb > 1000 else "INFO"
                    results.append(CheckResult("Espaço", f"Tabela: {r[0]}", st, f"{mb} MB", mb))
        except Exception as e:
            results.append(CheckResult("Espaço", "Análise de espaço", "AVISO", str(e)))
        return results

    # ── runner ───────────────────────────────────────────────────────────────
    def run(self, checks: List[str], progress_cb: Callable = None) -> List[CheckResult]:
        all_results = []
        total = len(checks)
        for i, check_id in enumerate(checks):
            if progress_cb:
                label = self.CHECKS_AVAILABLE.get(check_id, check_id)
                progress_cb(i / total, f"Verificando: {label}...")
            try:
                fn = getattr(self, f"check_{check_id}", None)
                if fn:
                    all_results.extend(fn())
            except Exception as e:
                all_results.append(CheckResult("Sistema", check_id, "ERRO", str(e)))
        if progress_cb:
            progress_cb(1.0, "Diagnóstico concluído!")
        return all_results


# ── Scheduler ────────────────────────────────────────────────────────────────

class Scheduler:
    """Runs periodic diagnostics in a background thread."""

    def __init__(self, store: Store):
        self.store = store
        self._job: Optional[threading.Timer] = None
        self._running = False
        self.interval_min: int = 0
        self.cfg: Optional[ConexaoConfig] = None
        self.checks: List[str] = []
        self.on_done: Optional[Callable] = None

    def start(self, cfg: ConexaoConfig, checks: List[str],
              interval_min: int, on_done: Callable = None):
        self.cfg = cfg
        self.checks = checks
        self.interval_min = interval_min
        self.on_done = on_done
        self._running = True
        self._fire()

    def stop(self):
        self._running = False
        if self._job:
            self._job.cancel()
            self._job = None

    def _fire(self):
        if not self._running:
            return
        # FIX: executa _run_once em thread separada para não bloquear o Timer
        threading.Thread(target=self._run_once, daemon=True).start()
        self._job = threading.Timer(self.interval_min * 60, self._fire)
        self._job.daemon = True
        self._job.start()

    def _run_once(self):
        # FIX: verifica _running novamente — stop() pode ter sido chamado
        # entre o agendamento da thread e sua execução
        if not self._running or self.cfg is None:
            return
        connector = DBConnector(self.cfg)
        ok, msg = connector.connect()
        if not ok:
            return
        runner = CheckRunner(connector)
        t0 = datetime.now()
        results = runner.run(self.checks)
        connector.close()
        t1 = datetime.now()
        session_id = hashlib.md5(t0.isoformat().encode()).hexdigest()[:12]
        erros  = sum(1 for r in results if r.status == "ERRO")
        avisos = sum(1 for r in results if r.status == "AVISO")
        ok_cnt = sum(1 for r in results if r.status == "OK")
        info   = sum(1 for r in results if r.status == "INFO")
        session = DiagnosticoSession(
            id=session_id,
            profile_name=self.cfg.nome or self.cfg.tipo,
            db_type=self.cfg.tipo,
            started_at=t0.isoformat(),
            finished_at=t1.isoformat(),
            total=len(results),
            erros=erros, avisos=avisos, ok=ok_cnt, info=info,
            resultados=results
        )
        self.store.save_session(session)
        if self.on_done and self._running:
            self.on_done(session)
