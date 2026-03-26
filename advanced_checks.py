"""
DB Diagnóstico v3.0 — Checks Avançados
  • Locks e conexões ativas em tempo real
  • Queries lentas em execução
  • Checks SQL customizáveis pelo usuário
"""

import json
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Any
from engine import CheckResult


# ════════════════════════════════════════════════════════════════════════════
# Custom Check Definition
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class CustomCheck:
    id: str
    nome: str
    descricao: str
    sql: str                     # query SQL a executar
    db_tipo: str                 # sqlserver | mysql | postgresql | sqlite | all
    comparador: str              # gt | lt | eq | ne | gte | lte | nonempty | empty
    threshold: Any               # valor de comparação
    status_falha: str = "AVISO"  # AVISO ou ERRO se threshold atingido
    ativo: bool = True

    def to_dict(self): return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "CustomCheck":
        return CustomCheck(**{k: v for k, v in d.items() if k in CustomCheck.__dataclass_fields__})

    def evaluate(self, value: Any) -> bool:
        """Retorna True se o valor ULTRAPASSA o threshold (check falhou)."""
        try:
            c = self.comparador
            t = type(value)(self.threshold) if value is not None else self.threshold
            if c == "gt":       return value > t
            if c == "lt":       return value < t
            if c == "gte":      return value >= t
            if c == "lte":      return value <= t
            if c == "eq":       return value == t
            if c == "ne":       return value != t
            if c == "nonempty": return bool(value)
            if c == "empty":    return not bool(value)
        except Exception:
            pass
        return False


# Checks pré-definidos de exemplo
DEFAULT_CUSTOM_CHECKS: List[CustomCheck] = [
    CustomCheck(
        id="cc_001", nome="Tabelas sem registros",
        descricao="Alerta se tabela crítica estiver vazia",
        sql="SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
        db_tipo="sqlite", comparador="eq", threshold=0,
        status_falha="AVISO", ativo=False
    ),
]


# ════════════════════════════════════════════════════════════════════════════
# Realtime Checks: Locks e Queries Lentas
# ════════════════════════════════════════════════════════════════════════════

class RealtimeChecker:
    """
    Checks em tempo real: locks ativos, queries em execução, conexões.
    Cada método retorna lista de dicts para exibição na grade ao vivo.
    """

    def __init__(self, connector):
        self.db   = connector
        self.tipo = connector.cfg.tipo

    # ── Locks ativos ─────────────────────────────────────────────────────────
    def get_locks(self) -> List[dict]:
        try:
            t = self.tipo
            if t == "postgresql":
                rows = self.db.query("""
                    SELECT
                        pid,
                        usename AS usuario,
                        application_name AS aplicacao,
                        state AS estado,
                        wait_event_type AS tipo_espera,
                        wait_event AS evento_espera,
                        EXTRACT(EPOCH FROM (now() - query_start))::int AS segundos,
                        LEFT(query, 120) AS query
                    FROM pg_stat_activity
                    WHERE state != 'idle'
                      AND pid != pg_backend_pid()
                    ORDER BY segundos DESC NULLS LAST
                    LIMIT 30
                """).fetchall()
                # FIX: fetchall() retorna lista de tuplas — usar cols fixas corretas
                cols = ["pid","usuario","aplicacao","estado","tipo_espera","evento_espera","segundos","query"]
                return [dict(zip(cols, r)) for r in rows]

            elif t == "mysql":
                rows = self.db.query("""
                    SELECT
                        ID AS pid,
                        USER AS usuario,
                        HOST AS host,
                        DB AS banco,
                        COMMAND AS comando,
                        TIME AS segundos,
                        STATE AS estado,
                        LEFT(INFO, 120) AS query
                    FROM information_schema.PROCESSLIST
                    WHERE COMMAND != 'Sleep'
                    ORDER BY TIME DESC
                    LIMIT 30
                """).fetchall()
                cols = ["pid","usuario","host","banco","comando","segundos","estado","query"]
                return [dict(zip(cols, r)) for r in rows]

            elif t == "sqlserver":
                rows = self.db.query("""
                    SELECT TOP 30
                        r.session_id AS pid,
                        s.login_name AS usuario,
                        s.program_name AS aplicacao,
                        r.status AS estado,
                        r.wait_type AS tipo_espera,
                        r.wait_time / 1000 AS segundos,
                        CAST(t.text AS NVARCHAR(120)) AS query
                    FROM sys.dm_exec_requests r
                    JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
                    WHERE r.session_id != @@SPID
                    ORDER BY r.wait_time DESC
                """).fetchall()
                cols = ["pid","usuario","aplicacao","estado","tipo_espera","segundos","query"]
                return [dict(zip(cols, r)) for r in rows]

            elif t == "sqlite":
                return [{"info": "SQLite não suporta monitoramento de locks em tempo real"}]

        except Exception as e:
            return [{"erro": str(e)}]
        return []

    # ── Queries lentas ───────────────────────────────────────────────────────
    def get_slow_queries(self, threshold_sec: int = 5) -> List[dict]:
        try:
            t = self.tipo
            if t == "postgresql":
                rows = self.db.query(f"""
                    SELECT
                        pid,
                        usename AS usuario,
                        EXTRACT(EPOCH FROM (now() - query_start))::int AS duracao_seg,
                        state AS estado,
                        LEFT(query, 200) AS query
                    FROM pg_stat_activity
                    WHERE state = 'active'
                      AND query_start IS NOT NULL
                      AND EXTRACT(EPOCH FROM (now() - query_start)) > {threshold_sec}
                      AND pid != pg_backend_pid()
                    ORDER BY duracao_seg DESC
                    LIMIT 20
                """).fetchall()
                cols = ["pid","usuario","duracao_seg","estado","query"]
                return [dict(zip(cols, r)) for r in rows]

            elif t == "mysql":
                rows = self.db.query(f"""
                    SELECT
                        ID AS pid,
                        USER AS usuario,
                        TIME AS duracao_seg,
                        STATE AS estado,
                        LEFT(INFO, 200) AS query
                    FROM information_schema.PROCESSLIST
                    WHERE COMMAND = 'Query'
                      AND TIME >= {threshold_sec}
                    ORDER BY TIME DESC
                    LIMIT 20
                """).fetchall()
                cols = ["pid","usuario","duracao_seg","estado","query"]
                return [dict(zip(cols, r)) for r in rows]

            elif t == "sqlserver":
                rows = self.db.query(f"""
                    SELECT TOP 20
                        r.session_id AS pid,
                        s.login_name AS usuario,
                        r.total_elapsed_time / 1000 AS duracao_seg,
                        r.status AS estado,
                        CAST(t.text AS NVARCHAR(200)) AS query
                    FROM sys.dm_exec_requests r
                    JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
                    WHERE r.total_elapsed_time / 1000 > {threshold_sec}
                      AND r.session_id != @@SPID
                    ORDER BY r.total_elapsed_time DESC
                """).fetchall()
                cols = ["pid","usuario","duracao_seg","estado","query"]
                return [dict(zip(cols, r)) for r in rows]

            elif t == "sqlite":
                return []

        except Exception as e:
            return [{"erro": str(e)}]
        return []

    # ── Conexões ativas ──────────────────────────────────────────────────────
    def get_connections_summary(self) -> dict:
        try:
            t = self.tipo
            if t == "postgresql":
                row = self.db.query("""
                    SELECT
                        count(*) FILTER (WHERE state = 'active') AS ativas,
                        count(*) FILTER (WHERE state = 'idle') AS idle,
                        count(*) AS total,
                        (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS max_conn
                    FROM pg_stat_activity
                """).fetchone()
                return {"ativas": row[0], "idle": row[1], "total": row[2], "maximo": row[3]}

            elif t == "mysql":
                total  = self.db.query("SHOW STATUS LIKE 'Threads_connected'").fetchone()
                maximo = self.db.query("SHOW VARIABLES LIKE 'max_connections'").fetchone()
                return {"total": int(total[1]) if total else 0,
                        "maximo": int(maximo[1]) if maximo else 0,
                        "ativas": 0, "idle": 0}

            elif t == "sqlserver":
                # FIX: FILTER é sintaxe PostgreSQL — SQL Server usa CASE/SUM
                row = self.db.query("""
                    SELECT
                        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS ativas,
                        COUNT(*) AS total
                    FROM sys.dm_exec_sessions
                    WHERE is_user_process = 1
                """).fetchone()
                return {"ativas": row[0] if row else 0, "total": row[1] if row else 0,
                        "idle": 0, "maximo": 0}

        except Exception:
            pass
        return {"ativas": 0, "idle": 0, "total": 0, "maximo": 0}


# ════════════════════════════════════════════════════════════════════════════
# Custom Check Runner
# ════════════════════════════════════════════════════════════════════════════

class CustomCheckRunner:

    def __init__(self, connector):
        self.db   = connector
        self.tipo = connector.cfg.tipo

    def run(self, checks: List[CustomCheck]) -> List[CheckResult]:
        results = []
        for chk in checks:
            if not chk.ativo:
                continue
            if chk.db_tipo not in ("all", self.tipo):
                continue
            try:
                row = self.db.query(chk.sql).fetchone()
                value = row[0] if row else None
                falhou = chk.evaluate(value)
                status = chk.status_falha if falhou else "OK"
                detalhe = (
                    f"Valor: {value} — threshold {chk.comparador} {chk.threshold} "
                    f"{'(condição atingida)' if falhou else '(OK)'}"
                )
                results.append(CheckResult(
                    categoria="Custom", nome=chk.nome,
                    status=status, detalhe=detalhe, valor=value
                ))
            except Exception as e:
                results.append(CheckResult(
                    categoria="Custom", nome=chk.nome,
                    status="ERRO", detalhe=str(e)
                ))
        return results
