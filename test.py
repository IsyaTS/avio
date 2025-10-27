#!/usr/bin/env python3
import os, json, subprocess, shlex

def get_app():
    p = subprocess.run(["bash","-lc","docker compose ps -q app"], capture_output=True, text=True)
    cid = p.stdout.strip()
    if not cid: raise SystemExit("app container not found")
    return cid

INLINE = r"""
import os, json, sys
ten = int(os.environ.get("TEN","1"))
chan = os.environ.get("CHAN","telegram")
text = os.environ.get("TEXT","")
reply = None
err = None

try:
    # Вариант 1: функция на верхнем уровне
    from app.brain import generate_sales_reply as gen
    reply = gen(tenant=ten, channel=chan, text=text, history=[])
except Exception as e1:
    try:
        # Вариант 2: через Planner
        from app.brain.planner import Planner
        pl = Planner()
        if hasattr(pl, "generate_sales_reply"):
            reply = pl.generate_sales_reply(tenant=ten, channel=chan, text=text, history=[])
        elif hasattr(pl, "plan") or hasattr(pl, "respond"):
            f = getattr(pl, "respond", getattr(pl, "plan", None))
            reply = f(tenant=ten, channel=chan, text=text, history=[])
        else:
            raise RuntimeError("Planner has no known reply method")
    except Exception as e2:
        err = f"{type(e2).__name__}: {e2}"

# Нормализуем вывод
out = {"ok": bool(reply and not err), "error": err}
if isinstance(reply, dict):
    out["reply"] = reply.get("text") or reply.get("reply") or json.dumps(reply, ensure_ascii=False)
elif isinstance(reply, (list, tuple)):
    out["reply"] = " ".join(map(str, reply))
elif reply is not None:
    out["reply"] = str(reply)
print(json.dumps(out, ensure_ascii=False))
"""

def call_brain(app_cid, ten, chan, text):
    env = os.environ.copy()
    env["TEN"] = str(ten)
    env["CHAN"] = chan
    env["TEXT"] = text
    cmd = ["bash","-lc", f"docker exec -i {shlex.quote(app_cid)} python - <<'PY'\n{INLINE}\nPY"]
    p = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if p.returncode != 0:
        return {"ok": False, "error": p.stderr.strip() or "exec failed"}
    try:
        return json.loads(p.stdout.strip().splitlines()[-1])
    except Exception as e:
        return {"ok": False, "error": f"bad json: {e}", "raw": p.stdout}

def main():
    app = get_app()
    try:
        ten = int(input("Tenant ID [1]: ").strip() or "1")
    except:
        print("Некорректный tenant"); return
    chan = (input("Канал [telegram|whatsapp|avito] (по умолчанию telegram): ").strip() or "telegram")
    if chan not in ("telegram","whatsapp","avito"):
        print("Некорректный канал"); return
    print("\nДиалог. /q для выхода.")
    while True:
        msg = input("Вы: ").strip()
        if not msg: continue
        if msg in ("/q","/quit","/exit"): break
        res = call_brain(app, ten, chan, msg)
        if res.get("ok") and res.get("reply"):
            print("Бот:", res["reply"])
        else:
            print("ERR:", res.get("error") or res)

if __name__ == "__main__":
    main()
