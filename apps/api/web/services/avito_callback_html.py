from __future__ import annotations

import html
import json
from typing import Any, Mapping


_STYLE = """
      body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        padding: 32px;
        background: #f9fafb;
        color: #111827;
      }
      .card {
        max-width: 460px;
        margin: 0 auto;
        padding: 24px;
        background: #fff;
        border-radius: 12px;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
      }
      .card h1 { margin: 0 0 12px; font-size: 20px; font-weight: 700; }
      .card p { margin: 0 0 16px; line-height: 1.5; }
      .status {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 13px;
        font-weight: 600;
      }
      .status.success { background: #dcfce7; color: #166534; }
      .status.error { background: #fee2e2; color: #b91c1c; }
      .hint { font-size: 13px; color: #6b7280; }
"""


def render_avito_callback_html(ok: bool, message: str, payload: Mapping[str, Any]) -> str:
    safe_message = html.escape(message, quote=False)
    data_json = _payload_json(ok, payload)
    status_class = "success" if ok else "error"
    status_text = "Успешно" if ok else "Ошибка"
    ok_js = "true" if ok else "false"
    return f"""<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <title>Avito OAuth</title>
    <style>{_STYLE}</style>
  </head>
  <body>
    <div class="card">
      <div class="status {status_class}">{status_text}</div>
      <h1>Avito OAuth</h1>
      <p>{safe_message}</p>
      <p class="hint">Окно закроется автоматически. Если этого не произошло - закройте его вручную.</p>
    </div>
    <script>
      (function() {{
        var payload = {data_json};
        try {{
          if (typeof payload === 'object' && payload) {{
            payload.source = 'avito-oauth';
            payload.ok = {ok_js};
          }}
          if (window.opener && window.opener !== window) {{
            window.opener.postMessage(payload, '*');
          }}
        }} catch (err) {{}}
        setTimeout(function() {{
          try {{ window.close(); }} catch (err) {{}}
        }}, 2000);
      }})();
    </script>
  </body>
</html>"""


def _payload_json(ok: bool, payload: Mapping[str, Any]) -> str:
    try:
        return json.dumps(dict(payload), ensure_ascii=False)
    except Exception:
        return json.dumps({"source": "avito-oauth", "ok": ok})
