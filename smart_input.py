"""
DB Diagnóstico v3.0 — Entrada Única Inteligente
Detecta automaticamente o tipo de conexão a partir de qualquer string.
"""

import os
import re
from typing import Optional, Tuple
from engine import ConexaoConfig
from inputs import ConnectionStringParser, EnvFileLoader


class SmartInputDetector:
    """
    Recebe qualquer string do usuário e descobre o que é:
      - DSN URL       → postgresql://, mysql://, etc.
      - ADO.NET       → Server=x;Database=y;...
      - Caminho .db   → arquivo.db, arquivo.sqlite
      - Caminho .env  → arquivo.env
      - Host simples  → localhost, 192.168.1.10
      - Host:porta    → localhost:5432
      - JSON inline   → {"host":"...","type":"..."}

    Retorna (ConexaoConfig parcial, confiança 0-1, tipo_detectado, dica)
    """

    # ── Detecção ─────────────────────────────────────────────────────────────
    @classmethod
    def detect(cls, text: str) -> Tuple[Optional[ConexaoConfig], float, str, str]:
        text = text.strip()
        if not text:
            return None, 0.0, "vazio", "Digite uma string de conexão, caminho de arquivo ou host."

        # 1. DSN URL completa
        if re.match(r'^(postgresql|postgres|mysql|mariadb|sqlserver|mssql|sqlite|sqlite3)://', text, re.I):
            try:
                cfg = ConnectionStringParser.parse(text)
                return cfg, 1.0, "dsn_url", f"DSN detectada — banco {cfg.tipo}"
            except Exception as e:
                return None, 0.3, "dsn_url_invalida", f"Parece uma DSN mas há erro: {e}"

        # 2. ADO.NET style
        if re.search(r'(Server|Data Source|Database|Initial Catalog)\s*=', text, re.I):
            try:
                cfg = ConnectionStringParser._parse_ado(text)
                return cfg, 0.95, "ado_net", f"String ADO.NET detectada — SQL Server"
            except Exception as e:
                return None, 0.3, "ado_invalida", f"Parece ADO.NET mas há erro: {e}"

        # 3. Arquivo SQLite
        if re.search(r'\.(db|sqlite|sqlite3)$', text, re.I):
            cfg = ConexaoConfig(tipo="sqlite", arquivo=text)
            existe = os.path.exists(text)
            conf   = 0.95 if existe else 0.7
            dica   = f"Arquivo SQLite {'encontrado ✅' if existe else 'não encontrado — verifique o caminho'}"
            return cfg, conf, "sqlite_file", dica

        # 4. Arquivo .env
        if text.endswith(".env") or os.path.basename(text) == ".env":
            if os.path.exists(text):
                try:
                    cfg = EnvFileLoader.load(text)
                    return cfg, 0.95, "env_file", f"Arquivo .env carregado — banco {cfg.tipo}"
                except Exception as e:
                    return None, 0.4, "env_invalido", f"Arquivo .env com erro: {e}"
            return None, 0.6, "env_file", "Arquivo .env não encontrado — verifique o caminho"

        # 5. JSON inline
        if text.startswith("{"):
            try:
                import json
                d = json.loads(text)
                tipo = (d.get("type") or d.get("db_type") or d.get("DB_TYPE") or "").lower()
                if tipo:
                    cfg = ConexaoConfig(
                        tipo=tipo,
                        host=d.get("host") or d.get("DB_HOST") or "localhost",
                        porta=int(d.get("port") or d.get("DB_PORT") or 0),
                        banco=d.get("database") or d.get("db") or d.get("DB_NAME") or "",
                        usuario=d.get("user") or d.get("username") or d.get("DB_USER") or "",
                        senha=d.get("password") or d.get("DB_PASSWORD") or "",
                    )
                    return cfg, 0.9, "json_inline", f"JSON detectado — banco {tipo}"
            except Exception:
                pass
            return None, 0.3, "json_invalido", "Parece JSON mas está malformado."

        # 6. host:porta (com porta conhecida)
        m = re.match(r'^([a-zA-Z0-9_.\-]+):(\d+)$', text)
        if m:
            host, port = m.group(1), int(m.group(2))
            tipo = cls._port_to_tipo(port)
            cfg  = ConexaoConfig(tipo=tipo or "postgresql", host=host, porta=port)
            conf = 0.8 if tipo else 0.5
            dica = f"Host:porta detectado — {tipo or 'banco desconhecido'} em {host}:{port}"
            return cfg, conf, "host_port", dica

        # 7. Host simples ou IP
        if re.match(r'^[a-zA-Z0-9_.\-]+$', text):
            cfg = ConexaoConfig(tipo="postgresql", host=text)
            return cfg, 0.4, "host_simples", f"Host detectado: {text} (tipo de banco desconhecido)"

        return None, 0.0, "desconhecido", "Não foi possível identificar o formato. Tente uma DSN ou caminho de arquivo."

    @staticmethod
    def _port_to_tipo(port: int) -> Optional[str]:
        return {1433: "sqlserver", 3306: "mysql", 5432: "postgresql", 5433: "postgresql"}.get(port)

    # ── Sugestões de autocompletar ────────────────────────────────────────────
    EXEMPLOS = [
        "postgresql://usuario:senha@localhost:5432/meubanco",
        "mysql://root:senha@localhost/meubanco",
        "sqlserver://sa:senha@servidor,1433/meubanco",
        "sqlite:///C:/dados/banco.db",
        "Server=localhost;Database=db;User Id=sa;Password=senha;",
        "C:\\dados\\banco.db",
        ".env",
        "localhost:5432",
    ]
