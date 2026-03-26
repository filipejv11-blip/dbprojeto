"""
DB Diagnóstico v2.1 — Módulo de Segurança
Responsabilidades:
  - Senha mestra (PBKDF2 + SHA-256, nunca armazenada em texto)
  - Criptografia AES-256-GCM para senhas de perfis
  - Timeout de sessão por inatividade
  - Log de auditoria append-only assinado com HMAC
"""

import os
import time
import hmac
import json
import hashlib
import base64
import threading
import sqlite3
from datetime import datetime
from typing import Optional, Callable

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend


# ── Constantes ───────────────────────────────────────────────────────────────
PBKDF2_ITERATIONS  = 480_000   # OWASP 2024 mínimo para SHA-256
SALT_SIZE          = 32        # bytes
NONCE_SIZE         = 12        # GCM standard
KEY_SIZE           = 32        # AES-256
AUDIT_HMAC_KEY_LEN = 32


# ════════════════════════════════════════════════════════════════════════════
# MasterKey — deriva chave AES da senha mestra via PBKDF2
# ════════════════════════════════════════════════════════════════════════════
class MasterKey:
    """
    Gerencia a chave de criptografia derivada da senha mestra.
    A senha NUNCA é armazenada — apenas o salt e o verificador (hash).
    """

    VERIFIER_FILE = "master.key"   # armazena: salt + verifier (não a senha)

    def __init__(self, data_dir: str = "."):
        self._data_dir  = data_dir
        self._key:  Optional[bytes] = None   # AES-256 key, em memória apenas
        self._salt: Optional[bytes] = None
        os.makedirs(data_dir, exist_ok=True)

    @property
    def _vfile(self) -> str:
        return os.path.join(self._data_dir, self.VERIFIER_FILE)

    @property
    def is_configured(self) -> bool:
        return os.path.exists(self._vfile)

    @property
    def is_unlocked(self) -> bool:
        return self._key is not None

    # ── Setup (primeiro uso) ─────────────────────────────────────────────────
    def setup(self, password: str) -> None:
        """Define a senha mestra pela primeira vez e salva o verificador."""
        if not password or len(password) < 8:
            raise ValueError("A senha mestra deve ter pelo menos 8 caracteres.")
        salt = os.urandom(SALT_SIZE)
        key  = self._derive(password, salt)
        verifier = hmac.new(key, b"db_diagnostico_verify", hashlib.sha256).digest()
        payload = {
            "salt":     base64.b64encode(salt).decode(),
            "verifier": base64.b64encode(verifier).decode(),
            "iter":     PBKDF2_ITERATIONS,
            "version":  1,
        }
        # FIX: escrita atômica — evita corrompimento se o processo morrer durante a escrita
        tmp = self._vfile + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, self._vfile)
        self._key  = key
        self._salt = salt

    # ── Unlock ───────────────────────────────────────────────────────────────
    def unlock(self, password: str) -> bool:
        """Tenta desbloquear com a senha fornecida. Retorna True se correto."""
        if not self.is_configured:
            raise RuntimeError("Senha mestra não configurada.")
        with open(self._vfile) as f:
            payload = json.load(f)
        salt     = base64.b64decode(payload["salt"])
        verifier = base64.b64decode(payload["verifier"])
        iters    = payload.get("iter", PBKDF2_ITERATIONS)
        key      = self._derive(password, salt, iters)
        expected = hmac.new(key, b"db_diagnostico_verify", hashlib.sha256).digest()
        if hmac.compare_digest(expected, verifier):
            self._key  = key
            self._salt = salt
            return True
        return False

    def lock(self) -> None:
        """Apaga a chave da memória (lock de sessão)."""
        if self._key:
            # sobrescreve antes de liberar
            self._key = bytes(KEY_SIZE)
        self._key = None

    # ── Crypto ───────────────────────────────────────────────────────────────
    def encrypt(self, plaintext: str) -> str:
        """Criptografa string com AES-256-GCM. Retorna base64(nonce+ciphertext)."""
        if not self._key:
            raise RuntimeError("App bloqueado. Faça login novamente.")
        nonce = os.urandom(NONCE_SIZE)
        aesgcm = AESGCM(self._key)
        ct = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
        return base64.b64encode(nonce + ct).decode()

    def decrypt(self, token: str) -> str:
        """Descriptografa token AES-256-GCM."""
        if not self._key:
            raise RuntimeError("App bloqueado. Faça login novamente.")
        raw = base64.b64decode(token)
        nonce, ct = raw[:NONCE_SIZE], raw[NONCE_SIZE:]
        aesgcm = AESGCM(self._key)
        return aesgcm.decrypt(nonce, ct, None).decode("utf-8")

    # ── Internal ─────────────────────────────────────────────────────────────
    @staticmethod
    def _derive(password: str, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=KEY_SIZE,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        return kdf.derive(password.encode("utf-8"))


# ════════════════════════════════════════════════════════════════════════════
# SessionGuard — timeout de inatividade
# ════════════════════════════════════════════════════════════════════════════
class SessionGuard:
    """
    Monitora atividade e dispara callback de bloqueio após inatividade.
    Thread-safe. Resolução de 5 s.
    """

    def __init__(self, timeout_minutes: int, on_lock: Callable):
        self._timeout  = timeout_minutes * 60
        self._on_lock  = on_lock
        self._last_act = time.monotonic()
        self._active   = False
        self._thread: Optional[threading.Thread] = None
        self._lock     = threading.Lock()

    def start(self) -> None:
        self._active   = True
        self._last_act = time.monotonic()
        self._thread   = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._active = False

    def ping(self) -> None:
        """Registra atividade do usuário — chame em qualquer interação."""
        with self._lock:
            self._last_act = time.monotonic()

    def _watch(self) -> None:
        while self._active:
            time.sleep(5)
            with self._lock:
                idle = time.monotonic() - self._last_act
            if idle >= self._timeout:
                self._active = False
                self._on_lock()


# ════════════════════════════════════════════════════════════════════════════
# AuditLog — log append-only com HMAC por linha
# ════════════════════════════════════════════════════════════════════════════
class AuditLog:
    """
    Registra eventos de segurança em SQLite com HMAC-SHA256 por linha.
    Garante que linhas não foram alteradas retroativamente.
    Somente leitura para exportação; escrita via append apenas.
    """

    TABLE = "audit_log"

    def __init__(self, db_path: str, hmac_key: bytes):
        self._path     = db_path
        self._hmac_key = hmac_key
        # FIX: lock para serializar acessos concorrentes (UI thread + scheduler thread)
        self._lock     = threading.Lock()
        self._init_db()

    def _conn(self):
        # FIX: check_same_thread=False é seguro porque usamos self._lock para serializar
        c = sqlite3.connect(self._path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def _init_db(self):
        with self._conn() as c:
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE} (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts        TEXT    NOT NULL,
                    event     TEXT    NOT NULL,
                    detail    TEXT,
                    user_host TEXT,
                    mac       TEXT    NOT NULL
                )
            """)

    def log(self, event: str, detail: str = "", user_host: str = "") -> None:
        ts = datetime.utcnow().isoformat() + "Z"
        payload = f"{ts}|{event}|{detail}|{user_host}"
        mac = hmac.new(self._hmac_key, payload.encode(), hashlib.sha256).hexdigest()
        # FIX: serializa escritas concorrentes
        with self._lock:
            with self._conn() as c:
                c.execute(
                    f"INSERT INTO {self.TABLE}(ts,event,detail,user_host,mac) VALUES(?,?,?,?,?)",
                    (ts, event, detail, user_host, mac)
                )

    def verify_integrity(self) -> list[dict]:
        """Verifica HMAC de cada linha. Retorna lista de linhas com status."""
        results = []
        with self._conn() as c:
            rows = c.execute(
                f"SELECT ts,event,detail,user_host,mac FROM {self.TABLE} ORDER BY id"
            ).fetchall()
        for r in rows:
            payload  = f"{r['ts']}|{r['event']}|{r['detail']}|{r['user_host']}"
            expected = hmac.new(self._hmac_key, payload.encode(), hashlib.sha256).hexdigest()
            ok = hmac.compare_digest(expected, r["mac"])
            results.append({**dict(r), "integrity": "OK" if ok else "ADULTERADO"})
        return results

    def recent(self, limit: int = 200) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                f"SELECT ts,event,detail,user_host FROM {self.TABLE} ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
