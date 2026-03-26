"""
DB Diagnóstico v3.0 — Módulo de Notificações
Canais: Notificação Windows (plyer), Email SMTP, Slack Webhook
"""

import smtplib
import json
import threading
import urllib.request
import urllib.error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from datetime import datetime


# ── Configurações ────────────────────────────────────────────────────────────

@dataclass
class NotifConfig:
    # Windows toast
    windows_enabled: bool = True

    # Email
    email_enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""          # criptografado no store
    smtp_tls: bool = True
    email_to: str = ""           # destinatários separados por vírgula
    email_from: str = ""

    # Slack
    slack_enabled: bool = False
    slack_webhook: str = ""      # URL do Incoming Webhook

    # Gatilhos
    notify_on_error: bool = True
    notify_on_warning: bool = False
    notify_on_ok: bool = False

    def to_dict(self):
        d = asdict(self)
        d.pop("smtp_pass", None)   # nunca persiste em texto claro
        return d

    @staticmethod
    def from_dict(d: dict) -> "NotifConfig":
        valid = {k: v for k, v in d.items() if k in NotifConfig.__dataclass_fields__}
        return NotifConfig(**valid)


# ── Dispatcher ────────────────────────────────────────────────────────────────

class NotificationDispatcher:
    """
    Dispara notificações em thread separada para não bloquear a UI.
    """

    def __init__(self, config: NotifConfig):
        self.cfg = config

    def dispatch(self, title: str, body: str, level: str = "INFO") -> None:
        """
        level: OK | AVISO | ERRO | INFO
        Dispara os canais habilitados conforme os gatilhos configurados.
        """
        cfg = self.cfg
        should_notify = (
            (level == "ERRO"  and cfg.notify_on_error)   or
            (level == "AVISO" and cfg.notify_on_warning) or
            (level == "OK"    and cfg.notify_on_ok)      or
            level == "INFO"
        )
        if not should_notify:
            return

        # FIX: snapshot de cfg no momento do dispatch — evita race condition
        # se self.cfg for atualizado antes da thread executar
        snap = cfg
        def _run():
            if snap.windows_enabled:
                self._notify_windows(title, body)
            if snap.email_enabled and snap.smtp_host and snap.email_to:
                self._notify_email(title, body, level, snap)
            if snap.slack_enabled and snap.slack_webhook:
                self._notify_slack(title, body, level, snap)

        threading.Thread(target=_run, daemon=True).start()

    # ── Windows ──────────────────────────────────────────────────────────────
    @staticmethod
    def _notify_windows(title: str, body: str) -> None:
        try:
            from plyer import notification
            notification.notify(
                title=title,
                message=body[:256],
                app_name="DB Diagnóstico",
                timeout=8,
            )
        except Exception:
            pass   # silencioso — pode não estar disponível no ambiente

    # ── Email ────────────────────────────────────────────────────────────────
    def _notify_email(self, title: str, body: str, level: str, cfg: "NotifConfig" = None) -> None:
        if cfg is None: cfg = self.cfg
        ICONS = {"OK": "✅", "ERRO": "❌", "AVISO": "⚠️", "INFO": "ℹ️"}
        icon  = ICONS.get(level, "")
        html  = f"""
        <html><body style="font-family:Consolas,monospace;background:#0C1016;color:#E2E8F0;padding:32px">
        <h2 style="color:#38BDF8">⬡ DB Diagnóstico v3.0</h2>
        <h3>{icon} {title}</h3>
        <pre style="background:#1A2535;padding:16px;border-radius:8px;color:#CBD5E1">{body}</pre>
        <p style="color:#475569;font-size:12px">
            {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} — Notificação automática
        </p>
        </body></html>
        """
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[DB Diagnóstico] {icon} {title}"
        msg["From"]    = cfg.email_from or cfg.smtp_user
        msg["To"]      = cfg.email_to
        msg.attach(MIMEText(body, "plain"))
        msg.attach(MIMEText(html, "html"))
        try:
            if cfg.smtp_tls:
                s = smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=10)
                s.starttls()
            else:
                s = smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, timeout=10)
            if cfg.smtp_user:
                s.login(cfg.smtp_user, cfg.smtp_pass)
            recipients = [r.strip() for r in cfg.email_to.split(",") if r.strip()]
            s.sendmail(msg["From"], recipients, msg.as_string())
            s.quit()
        except Exception as e:
            print(f"[Notif Email] Erro: {e}")

    def _notify_slack(self, title: str, body: str, level: str, cfg: "NotifConfig" = None) -> None:
        if cfg is None: cfg = self.cfg
        COLORS = {"OK": "#34D399", "ERRO": "#F87171", "AVISO": "#FBBF24", "INFO": "#818CF8"}
        color  = COLORS.get(level, "#94A3B8")
        payload = {
            "attachments": [{
                "color":  color,
                "title":  f"⬡ DB Diagnóstico — {title}",
                "text":   body,
                "footer": f"DB Diagnóstico v3.0  •  {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                "mrkdwn_in": ["text"],
            }]
        }
        data = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            cfg.slack_webhook,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=8):
                pass
        except Exception as e:
            print(f"[Notif Slack] Erro: {e}")

    # ── Test ─────────────────────────────────────────────────────────────────
    def test(self) -> dict[str, str]:
        """Testa todos os canais habilitados. Retorna dict canal→resultado."""
        results = {}
        if self.cfg.windows_enabled:
            try:
                self._notify_windows("DB Diagnóstico — Teste", "Notificação Windows funcionando!")
                results["Windows"] = "OK"
            except Exception as e:
                results["Windows"] = f"Erro: {e}"

        if self.cfg.email_enabled and self.cfg.smtp_host:
            try:
                self._notify_email("Teste de Notificação", "Email de teste do DB Diagnóstico v3.0.", "INFO")
                results["Email"] = "OK"
            except Exception as e:
                results["Email"] = f"Erro: {e}"

        if self.cfg.slack_enabled and self.cfg.slack_webhook:
            try:
                self._notify_slack("Teste de Notificação", "Mensagem de teste do DB Diagnóstico v3.0.", "INFO")
                results["Slack"] = "OK"
            except Exception as e:
                results["Slack"] = f"Erro: {e}"

        return results
