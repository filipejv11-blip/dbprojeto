"""
DB Diagnóstico v2.1 — Módulo de Entradas de Dados
Suporta:
  1. Formulário manual (GUI)
  2. Arquivo .env
  3. String de conexão (DSN URL)
  4. Azure Key Vault
  5. AWS Secrets Manager
  6. Arquivo .cfg criptografado (AES-256-GCM via senha mestra)
"""

import os
import re
import json
import base64
import configparser
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote

from engine import ConexaoConfig


# ════════════════════════════════════════════════════════════════════════════
# 1. Parser de String de Conexão (DSN URL)
# ════════════════════════════════════════════════════════════════════════════
class ConnectionStringParser:
    """
    Parseia strings no formato:
      postgresql://user:pass@host:5432/dbname
      mysql://user:pass@host/db?ssl=true
      sqlserver://user:pass@host:1433/db
      sqlite:///C:/path/to/file.db
      Server=host;Database=db;User Id=usr;Password=pwd;   (SQL Server ADO style)
    """

    SCHEME_MAP = {
        "postgresql": "postgresql",
        "postgres":   "postgresql",
        "mysql":      "mysql",
        "mariadb":    "mysql",
        "sqlserver":  "sqlserver",
        "mssql":      "sqlserver",
        "sqlite":     "sqlite",
        "sqlite3":    "sqlite",
    }

    @classmethod
    def parse(cls, conn_str: str) -> ConexaoConfig:
        conn_str = conn_str.strip()

        # ADO.NET style (SQL Server): Key=Value;Key=Value
        if "=" in conn_str and "://" not in conn_str:
            return cls._parse_ado(conn_str)

        parsed = urlparse(conn_str)
        scheme = parsed.scheme.lower()
        tipo   = cls.SCHEME_MAP.get(scheme)
        if not tipo:
            raise ValueError(
                f"Esquema '{scheme}' não reconhecido.\n"
                "Use: postgresql://, mysql://, sqlserver://, sqlite://"
            )

        if tipo == "sqlite":
            # sqlite:///absolute  ou  sqlite://./relative
            path = parsed.path
            if path.startswith("///"):
                path = path[3:]
            elif path.startswith("//"):
                path = path[2:]
            return ConexaoConfig(tipo="sqlite", arquivo=unquote(path))

        host     = parsed.hostname or "localhost"
        port     = parsed.port or 0
        database = parsed.path.lstrip("/") if parsed.path else ""
        user     = unquote(parsed.username or "")
        password = unquote(parsed.password or "")

        return ConexaoConfig(
            tipo=tipo, host=host, porta=port,
            banco=database, usuario=user, senha=password
        )

    @staticmethod
    def _parse_ado(s: str) -> ConexaoConfig:
        """Parseia formato ADO.NET: Server=x;Database=y;User Id=u;Password=p"""
        kv = {}
        for part in s.split(";"):
            if "=" in part:
                k, _, v = part.partition("=")
                kv[k.strip().lower()] = v.strip()
        host = kv.get("server") or kv.get("data source") or "localhost"
        port = 0
        if "," in host:
            host, _, port_s = host.partition(",")
            try: port = int(port_s)
            except: pass
        return ConexaoConfig(
            tipo="sqlserver",
            host=host,
            porta=port,
            banco=kv.get("database") or kv.get("initial catalog") or "",
            usuario=kv.get("user id") or kv.get("uid") or "",
            senha=kv.get("password") or kv.get("pwd") or "",
        )


# ════════════════════════════════════════════════════════════════════════════
# 2. Leitor de .env
# ════════════════════════════════════════════════════════════════════════════
class EnvFileLoader:
    """
    Lê variáveis de um arquivo .env e monta ConexaoConfig.

    Formato esperado (qualquer combinação):
      DB_TYPE=postgresql
      DB_HOST=localhost
      DB_PORT=5432
      DB_NAME=mydb
      DB_USER=admin
      DB_PASSWORD=secret
      DB_FILE=/path/to/file.db        # para SQLite
      DATABASE_URL=postgresql://...   # alternativa DSN completa
    """

    @staticmethod
    def load(path: str) -> ConexaoConfig:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

        env: dict[str, str] = {}
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, _, v = line.partition("=")
                    # remove aspas opcionais
                    v = v.strip().strip('"').strip("'")
                    env[k.strip().upper()] = v

        # DSN completa tem prioridade
        dsn = env.get("DATABASE_URL") or env.get("DB_URL")
        if dsn:
            cfg = ConnectionStringParser.parse(dsn)
            cfg.nome = env.get("DB_PROFILE_NAME", "")
            return cfg

        tipo = env.get("DB_TYPE", "").lower()
        if not tipo:
            raise ValueError("Variável DB_TYPE não encontrada no .env")

        return ConexaoConfig(
            tipo=tipo,
            nome=env.get("DB_PROFILE_NAME", ""),
            host=env.get("DB_HOST", "localhost"),
            porta=int(env.get("DB_PORT", 0) or 0),
            banco=env.get("DB_NAME", ""),
            usuario=env.get("DB_USER", ""),
            senha=env.get("DB_PASSWORD", ""),
            arquivo=env.get("DB_FILE", ""),
        )


# ════════════════════════════════════════════════════════════════════════════
# 3. Azure Key Vault
# ════════════════════════════════════════════════════════════════════════════
class AzureKeyVaultLoader:
    """
    Lê segredos do Azure Key Vault usando DefaultAzureCredential.
    Requer: az login  OU  variáveis AZURE_CLIENT_ID/SECRET/TENANT_ID.

    Segredos esperados no vault (qualquer um):
      db-connection-string  → DSN completa
      db-type, db-host, db-port, db-name, db-user, db-password
    """

    @staticmethod
    def load(vault_url: str, profile_name: str = "") -> ConexaoConfig:
        try:
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError("Instale: pip install azure-keyvault-secrets azure-identity")

        credential = DefaultAzureCredential()
        client     = SecretClient(vault_url=vault_url, credential=credential)

        def _get(name: str, default: str = "") -> str:
            try:
                return client.get_secret(name).value or default
            except Exception:
                return default

        # tenta DSN primeiro
        dsn = _get("db-connection-string")
        if dsn:
            cfg = ConnectionStringParser.parse(dsn)
            cfg.nome = profile_name or "Azure KV"
            return cfg

        tipo = _get("db-type")
        if not tipo:
            raise ValueError("Segredo 'db-connection-string' ou 'db-type' não encontrado no vault.")

        return ConexaoConfig(
            tipo=tipo.lower(),
            nome=profile_name or "Azure KV",
            host=_get("db-host", "localhost"),
            porta=int(_get("db-port") or 0),
            banco=_get("db-name"),
            usuario=_get("db-user"),
            senha=_get("db-password"),
        )


# ════════════════════════════════════════════════════════════════════════════
# 4. AWS Secrets Manager
# ════════════════════════════════════════════════════════════════════════════
class AWSSecretsLoader:
    """
    Lê segredos do AWS Secrets Manager.
    Requer: aws configure  OU  variáveis AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.

    O segredo pode ser:
      - String DSN:  "postgresql://user:pass@host/db"
      - JSON:        {"DB_TYPE":"mysql","DB_HOST":"...","DB_PASSWORD":"..."}
    """

    @staticmethod
    def load(secret_name: str, region: str = "us-east-1",
             profile_name: str = "") -> ConexaoConfig:
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError:
            raise ImportError("Instale: pip install boto3")

        client = boto3.client("secretsmanager", region_name=region)
        try:
            resp = client.get_secret_value(SecretId=secret_name)
        except Exception as e:
            raise ConnectionError(f"Erro ao acessar AWS Secrets: {e}")

        raw = resp.get("SecretString") or ""

        # tenta JSON
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                dsn = data.get("DATABASE_URL") or data.get("db-connection-string")
                if dsn:
                    cfg = ConnectionStringParser.parse(dsn)
                    cfg.nome = profile_name or secret_name
                    return cfg
                tipo = (data.get("DB_TYPE") or data.get("db-type") or "").lower()
                if not tipo:
                    # FIX: levanta erro específico em vez de silenciosamente tentar parsear como DSN
                    raise ValueError(
                        "Campo 'DB_TYPE' não encontrado no segredo JSON.\n"
                        "Adicione 'DB_TYPE' ao JSON ou use uma DSN em 'DATABASE_URL'."
                    )
                return ConexaoConfig(
                    tipo=tipo,
                    nome=profile_name or secret_name,
                    host=data.get("DB_HOST") or data.get("db-host") or "localhost",
                    porta=int(data.get("DB_PORT") or data.get("db-port") or 0),
                    banco=data.get("DB_NAME") or data.get("db-name") or "",
                    usuario=data.get("DB_USER") or data.get("db-user") or "",
                    senha=data.get("DB_PASSWORD") or data.get("db-password") or "",
                )
        except json.JSONDecodeError:
            pass   # não é JSON — tenta como DSN direta
        except ValueError:
            raise  # re-levanta erros de validação explícitos

        # tenta DSN string direta
        cfg = ConnectionStringParser.parse(raw)
        cfg.nome = profile_name or secret_name
        return cfg


# ════════════════════════════════════════════════════════════════════════════
# 5. Arquivo .cfg Criptografado
# ════════════════════════════════════════════════════════════════════════════
class EncryptedConfigFile:
    """
    Salva/carrega múltiplos perfis em arquivo .cfg criptografado com AES-256-GCM.
    Depende do MasterKey para encrypt/decrypt — o arquivo é inútil sem a senha mestra.

    Formato interno (antes de criptografar):
      JSON list de perfis (sem senha — senhas ficam no Store criptografado)
    """

    MAGIC = b"DBDIAG21"   # header para identificar formato

    def __init__(self, master_key, path: str = "profiles.cfg"):
        self._mk   = master_key
        self._path = path

    def save(self, profiles: list[ConexaoConfig]) -> None:
        plaintext = json.dumps(
            [p.to_dict() for p in profiles], ensure_ascii=False
        )
        token = self._mk.encrypt(plaintext)
        payload = self.MAGIC + token.encode("utf-8")
        # FIX: escrita atômica — evita .cfg corrompido se o processo morrer durante a escrita
        tmp = self._path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(payload)
        os.replace(tmp, self._path)

    def load(self) -> list[ConexaoConfig]:
        if not os.path.exists(self._path):
            raise FileNotFoundError(f"Arquivo não encontrado: {self._path}")
        with open(self._path, "rb") as f:
            raw = f.read()
        if not raw.startswith(self.MAGIC):
            raise ValueError("Arquivo .cfg inválido ou corrompido.")
        token   = raw[len(self.MAGIC):].decode("utf-8")
        plaintext = self._mk.decrypt(token)
        dicts   = json.loads(plaintext)
        return [ConexaoConfig.from_dict(d) for d in dicts]
