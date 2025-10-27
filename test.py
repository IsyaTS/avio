#!/usr/bin/env python3
import os, json, subprocess, shlex, inspect, sys

INLINE = r"""
import os, json, asyncio, inspect, sys

ten = int(os.environ.get("TEN","1"))
chan = os.environ.get("CHAN","telegram")
text = os.environ.get("TEXT","")

mods = ["app.core", "app.brain", "app.brain.planner"]
names = ["generate_sales_reply","generate_reply","reply","respond","plan"]
cand = []

def collect():
    for m in mods:
        try:
            mod = __import__(m, fromlist=["*"])
        except Exception:
            continue
        for nm in dir(mod):
            low = nm.lower()
            if any(k in low for k in names):
                fn = getattr(mod, nm)
                if callable(fn):
                    try:
                        sig = inspect.signature(fn)
                        params = set(sig.parameters.keys())
                    except Exception:
                        params = set()
                    cand.append((m, nm, fn, params))
    # уникализируем по объекту
    uniq = []
    seen = set()
    for m,n,f,p in cand:
        if id(f) in seen: continue
        seen.add(id(f)); uniq.append((m,n,f,p))
    return uniq

def call_fn(fn, params):
    # собираем kwargs из доступных параметров
    kw = {}
    if "tenant" in params: kw["tenant"] = ten
    if "channel" in params: kw["channel"] = chan
    if "text" in params: kw["text"] = text
    if "history" in params: kw["history"] = []
    if "ctx" in params: kw["ctx"] = {}
    if inspect.iscoroutinefunction(fn):
        return asyncio.run(fn(**kw))
    res = fn(**kw) if kw else fn(text)
    return res

out = {"ok": False}
errs = []

for m,n,fn,params in collect():
    try:
        res = call_fn(fn, params)
        ans = None
        if isinstance(res, dict):
            ans = res.get("text") or res.get("reply") or res.get("message") or json.dumps(res, ensure_ascii=False)
        elif isinstance(res, (list, tuple)):
            ans = " ".join(map(str,res))
        elif res is not None:
            ans = str(res)
        if ans:
            out = {"ok": True, "module": m, "func": n, "reply": ans}
            break
    except Exception as e:
        errs.append(f"{m}.{n}: {type(e).__name__}: {e}")

if not out["ok"]:
    out = {"ok": False, "error": "no callable reply found", "tried": errs[:10]}

print(json.dumps(out, ensure_ascii=False))
"""

def get_app():
    p = subprocess.run(["bash","-lc","docker compose ps -q app"], capture_output=True, text=True)
    cid = p.stdout.strip()
    if not cid:
        print("ERR: app container not found", file=sys.stderr)
        sys.exit(1)
    return cid

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
        line = p.stdout.strip().splitlines()[-1]
        return json.loads(line)
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
        if res.get("ok"):
            print(f"Бот: {res.get('reply')}")
        else:
            print("ERR:", res.get("error"))
            tried = res.get("tried")
            if tried:
                print("Tried:")
                for t in tried:
                    print(" -", t)
            break

if __name__ == "__main__":
    main()
