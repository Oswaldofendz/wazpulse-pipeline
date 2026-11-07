"""Read-only local diagnostics. Does not import the bot or read .env files."""
import ast
import importlib.util
from pathlib import Path
import sys


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    root = Path(__file__).resolve().parents[1]
    errors = []
    for path in (root / "src").glob("*.py"):
        try:
            ast.parse(path.read_text(encoding="utf-8-sig"))
        except SyntaxError as exc:
            errors.append(f"{path.name}:{exc.lineno}: {exc.msg}")
    print("WaCapital: diagnÃ³stico offline, sin acceder a credenciales")
    print("Sintaxis:", "OK" if not errors else "ERROR")
    for message in errors:
        print(message)
    print("Python actual:", ".".join(map(str, sys.version_info[:3])))
    print("Python declarado por el proyecto: 3.12")
    missing = []
    for module in ("requests", "dotenv", "supabase", "feedparser", "PIL"):
        present = importlib.util.find_spec(module) is not None
        print(module + ":", "instalado" if present else "pendiente")
        if not present:
            missing.append(module)
    print("No se ha conectado Telegram, publicado contenido ni modificado Supabase.")
    print("Las comprobaciones de dependencias no certifican sus versiones ni una conexiÃ³n real.")
    return 1 if errors or missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
