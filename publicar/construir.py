#!/usr/bin/env python3

import shutil
from pathlib import Path

from generar_archivar import main as generar_archivar_main
from generar_descubrir import main as generar_descubrir_main


BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
DIST_DIR = BASE_DIR / "dist"
FAVICON_SRC = SRC_DIR / "favicon.png"


def copiar_fuentes() -> None:
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    shutil.copytree(SRC_DIR, DIST_DIR)

    if FAVICON_SRC.exists():
        shutil.copy2(FAVICON_SRC, DIST_DIR / "favicon.png")


def main() -> int:
    copiar_fuentes()
    generar_descubrir_main()
    generar_archivar_main()
    print(f"publicar: build listo en {DIST_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
