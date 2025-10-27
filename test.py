#!/usr/bin/env python3
import os, json, subprocess, shlex, sys

INLINE = r"""
import os, json, asyncio, inspect

TEN = int(os.environ.get("TEN","1"))
CHAN = os.environ.get("CHAN","telegram")
TEXT = os.environ.get("TEXT","")

CANDIDATE_MODULES = ["app.core", "app.brain", "app.brain.planner"]
NAME_HINTS = ["generate_sales_reply","generate_reply","reply","respond","plan"]

def extract_text(res):
    if res is None:
        return ""
    if isinstance(res, str):
        return res.strip()
    if isinstance(res, (list, tuple)):
        s = " ".join(map(str, res)).strip()
        if s: return s
    if isinstance(res, dict):
        for k in ("text","reply","answer","content","message","final","output"):
            v = res.get(k)
            if v:
                return str(v).strip()
        # nested common spots
        for path in (("data","text"),("message","text")):
            d = res
            ok = True
            for p in path:
                if isinstance(d, dict) and p in d:
                    d = d[p]
                else:
                    ok = False; break
            if ok and d:
                return str(d).strip()
        # nothing found -> shortest json
        return json.dumps(res, ensure_ascii=False)
    # object with common attributes
    for a in ("text","reply","answer","content","message","final","output","response","body"):
        if hasattr(res, a):
            v = getattr(res, a)
            if v:
                return str(v).strip()
    # pydantic/dataclass helpers
    for m in ("to_dict","dict","model_dump"):
        if hasattr(res, m) and callable(getattr(res, m)):
            try:
                d = getattr(res, m)()
                txt = extract_text(d)
                if txt: return txt
            except Exception:
                pass
    # plan-like fields fallback
    parts = []
    for a in ("analysis","stage","cta"):
        if hasattr(res, a):
            v = getattr(res, a)
            if isinstance(v, str) and v.strip():
                parts.append(v.strip())
    if hasattr(res, "next_questions"):
        try:
            nq = getattr(res, "next_questions")
            if isinstance(nq, (list, tuple)) and nq:
                parts.append(str(nq[0]))
        except Exception:
            pass
    if parts:
        return " ".join(parts).strip()
    return str(res)

def collect_candidates():
    out = []
    seen = set()
    for modname in CANDIDATE_MODULES:
        try:
            mod = __import__(modname, fromlist=["*"])
        except Exception:
            continue
        for nm in dir(mod):
            low = nm.lower()
            if any(h in low for h in NAME_HINTS):
                fn = getattr(mod, nm)
                if callable(fn) and id(fn) not in seen:
                    seen.add(id(fn))
                    try:
                        sig = inspect.signature(fn)
                        params = set(sig.parameters.keys())
                    except Exception:
                        params = set()
                    out.append((modname, nm, fn, params))
    return out

def call_fn(fn, params):
    kw = {}
    if "tenant" in params: kw["tenant"] = TEN
    if "channel" in params: kw["channel"] = CHAN
    if "text" in params: kw["text"] = TEXT
    if "history" in params: kw["history"] = []
    if "ctx" in params: kw["ctx"] = {}
    if inspect.iscoroutinefunction(fn):
        return asyncio.run(fn(**kw))
    try:
        return fn(**kw) if kw else fn(TEXT)
    except TypeError:
        # last resort
        return fn(TEXT)

tried = []
for m,n,fn,params in collect_candidates():
    try:
        res = call_fn(fn, params)
        ans = extract_text(res)
        if ans:
            print(json.dumps({"ok": True, "module": m, "func": n, "reply": ans}, ensure_ascii=False))
            break
    except Exception as e:
        tried.append(f"{m}.{n}: {type(e).__name__}: {e}")
else:
    print(json.dumps({"ok": False, "error": "no callable reply found", "tried": tried[:10]}, ensure_ascii=False))
"""

def get_app():
    p = subprocess.run(["bash","-lc","docker compose ps -q app"], capture_output=True, text=True)
    cid = p.stdout.strip()
    if not cid:
        print("ERR: app container not found", file=sys.stderr); sys.exit(1)
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
    line = (p.stdout or "").strip().splitlines()[-1:] or [""]
    try:
        return json.loads(line[0])
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
            if res.get("tried"):
                print("Tried:"); [print(" -", t) for t in res["tried"]]
            break

if __name__ == "__main__":
    main()
