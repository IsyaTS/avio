#!/usr/bin/env python3
import os, json, subprocess, time

ENV = "/opt/avio/.env"
WEBHOOK = "http://127.0.0.1:8000/webhook"

def read_env(key):
    try:
        with open(ENV, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith(key + "="):
                    return line.strip().split("=",1)[1]
    except FileNotFoundError:
        pass
    return ""

def send_event(tenant, channel, text, token):
    payload = {
        "provider": channel,
        "channel": channel,
        "event": "messages.incoming",
        "tenant": tenant,
        "message_id": f"SIM-{int(time.time()*1000)}",
        "peer": "sim-user",
        "from": "sim-user",
        "text": text,
        "timestamp": int(time.time())
    }
    url = f"{WEBHOOK}?tenant={tenant}&token={token}"
    headers = [
        "-H", f"X-Webhook-Token: {token}",
        "-H", f"Authorization: Bearer {token}",
        "-H", f"X-Auth-Token: {token}",
        "-H", "Content-Type: application/json",
    ]
    return subprocess.run(
        ["curl","-sS","-o","/dev/null","-w","%{http_code}", "-X","POST", url, *headers, "-d", json.dumps(payload)],
        capture_output=True, text=True, check=False
    ).stdout.strip()

def tail_reply():
    # показываем последние строки логов app и выдёргиваем возможный ответ
    p = subprocess.run(
        ["docker","compose","logs","--since=3s","app"],
        capture_output=True, text=True
    )
    lines = p.stdout.splitlines()
    # эвристика по ключевым словам
    keys = ("generated", "reply", "send", "outbox", "telegram", "whatsapp")
    hits = [ln for ln in lines if any(k in ln.lower() for k in keys)]
    if hits:
        print("\n".join(hits[-12:]))
    else:
        print("(логов с ответом не видно; см. полные логи: docker compose logs --since=5s app)")

def main():
    token = read_env("WEBHOOK_SECRET")
    if not token:
        print("WEBHOOK_SECRET не найден в .env")
        return
    try:
        tenant = int(input("Tenant ID [1]: ").strip() or "1")
    except:
        print("Некорректный tenant"); return
    channel = (input("Канал [telegram|whatsapp|avito] (по умолчанию telegram): ").strip() or "telegram")
    if channel not in ("telegram","whatsapp","avito"):
        print("Некорректный канал"); return

    print("\nДиалог. /q для выхода.")
    while True:
        msg = input("Вы: ").strip()
        if not msg: continue
        if msg in ("/q","/quit","/exit"): break
        code = send_event(tenant, channel, msg, token)
        print(f"[webhook] HTTP {code}")
        time.sleep(1.2)
        print("Бот (по логам):")
        tail_reply()
        print()
if __name__ == "__main__":
    main()
