"""
DB Diagnóstico v2.1 — Store Seguro
Estende o Store da v2.0 com criptografia AES-256-GCM nas senhas.
A senha NUNCA é salva em texto claro — sempre criptografada via MasterKey.
"""

import json
import sqlite3
import hashlib
import threading
from datetime import datetime
from typing import List, Optional

from engine import ConexaoConfig, CheckResult, DiagnosticoSession
from security import MasterKey


class SecureStore:
    """
    Persiste perfis (com senhas criptografadas) e histórico de sessões.
    Requer MasterKey desbloqueada para operações com senhas.
    """

    def __init__(self, db_path: str, master_key: MasterKey):
        self._path = db_path
        self._mk   = master_key
        # FIX: lock para serializar acessos do scheduler thread e da UI thread
        self._lock = threading.Lock()
        self._init()

    def _conn(self):
        # FIX: check_same_thread=False seguro pois usamos self._lock
        c = sqlite3.connect(self._path, check_same_thread=False)
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

    # ── Profiles ─────────────────────────────────────────────────────────────
    def save_profile(self, cfg: ConexaoConfig) -> None:
        """Salva perfil com senha criptografada."""
        d = cfg.to_dict()
        if cfg.senha:
            d["senha_enc"] = self._mk.encrypt(cfg.senha)
        data = json.dumps(d, ensure_ascii=False)
        with self._lock:
            with self._conn() as c:
                c.execute("""
                    INSERT INTO profiles(name, data, updated) VALUES(?,?,?)
                    ON CONFLICT(name) DO UPDATE SET data=excluded.data, updated=excluded.updated
                """, (cfg.nome, data, datetime.now().isoformat()))

    def delete_profile(self, name: str) -> None:
        with self._lock:
            with self._conn() as c:
                c.execute("DELETE FROM profiles WHERE name=?", (name,))

    def list_profiles(self) -> List[ConexaoConfig]:
        """Retorna perfis sem senha (para exibição em lista)."""
        with self._conn() as c:
            rows = c.execute("SELECT data FROM profiles ORDER BY updated DESC").fetchall()
        result = []
        for r in rows:
            d = json.loads(r["data"])
            d.pop("senha_enc", None)   # não expõe nem criptografado na listagem
            result.append(ConexaoConfig.from_dict(d))
        return result

    def load_profile_with_password(self, name: str) -> Optional[ConexaoConfig]:
        """Carrega perfil com senha descriptografada (para uso na conexão)."""
        with self._conn() as c:
            row = c.execute("SELECT data FROM profiles WHERE name=?", (name,)).fetchone()
        if not row:
            return None
        d = json.loads(row["data"])
        senha_enc = d.pop("senha_enc", None)
        cfg = ConexaoConfig.from_dict(d)
        if senha_enc:
            try:
                cfg.senha = self._mk.decrypt(senha_enc)
            except Exception:
                cfg.senha = ""   # falha silenciosa — senha pode ter mudado
        return cfg

    # ── Sessions ─────────────────────────────────────────────────────────────
    def save_session(self, s: DiagnosticoSession) -> None:
        with self._lock:
            with self._conn() as c:
                c.execute("""
                    INSERT OR REPLACE INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (s.id, s.profile_name, s.db_type, s.started_at, s.finished_at,
                      s.total, s.erros, s.avisos, s.ok, s.info,
                      json.dumps([r.to_dict() for r in s.resultados], ensure_ascii=False)))

    def list_sessions(self, limit: int = 50) -> List[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT id,profile_name,db_type,started_at,finished_at,"
                "total,erros,avisos,ok,info FROM sessions ORDER BY started_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_session_results(self, session_id: str) -> List[CheckResult]:
        with self._conn() as c:
            row = c.execute("SELECT resultados FROM sessions WHERE id=?",
                            (session_id,)).fetchone()
        if not row:
            return []
        return [CheckResult(**d) for d in json.loads(row["resultados"])]

    def delete_session(self, session_id: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM sessions WHERE id=?", (session_id,))
