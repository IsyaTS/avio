from __future__ import annotations

import os

import uvicorn

from .api import create_app


def main() -> None:  # pragma: no cover - CLI entrypoint
    app = create_app()
    port = int(os.getenv("TGWORKER_PORT", "9000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":  # pragma: no cover
    main()
