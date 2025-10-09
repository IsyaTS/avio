from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run("tgworker.api:create_app", host="0.0.0.0", port=8085)


if __name__ == "__main__":
    main()
