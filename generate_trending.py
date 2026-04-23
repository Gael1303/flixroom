#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera trending_movie.json e trending_tv.json consultando o Supabase.
Rodado pela GitHub Action diariamente. Requer a service role key no ambiente.

Uso local:
    SUPABASE_URL=https://xxx.supabase.co \
    SUPABASE_SERVICE_KEY=eyJ... \
    python generate_trending.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

# ── Configuração via variáveis de ambiente ────────────────────────────────────

SUPABASE_URL         = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("❌  SUPABASE_URL e SUPABASE_SERVICE_KEY são obrigatórios.", file=sys.stderr)
    sys.exit(1)

# ── Parâmetros de geração ─────────────────────────────────────────────────────

CONFIGS = [
    # (content_type, limit, min_views, arquivo_saída)
    ("movie", 75, 2, "data/trending_movie.json"),
    ("tv",    75, 2, "data/trending_tv.json"),
]

# ── Helpers HTTP ──────────────────────────────────────────────────────────────

def _headers():
    return {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }


def _get(url: str, timeout: int = 15) -> dict | list | None:
    try:
        req = Request(url)
        for k, v in _headers().items():
            req.add_header(k, v)
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"❌  HTTP {e.code} em GET {url}\n    {body}", file=sys.stderr)
    except URLError as e:
        print(f"❌  URLError em GET {url}: {e.reason}", file=sys.stderr)
    except Exception as e:
        print(f"❌  Erro inesperado em GET {url}: {e}", file=sys.stderr)
    return None


# ── Consulta ao Supabase (tabela content_views via REST) ──────────────────────

def fetch_trending(content_type: str, limit: int, min_views: int) -> list[dict]:
    """
    Lê diretamente da tabela content_views com service key.
    Ordena por view_count DESC, filtra por content_type e min_views.

    Ajuste o nome da tabela/colunas se o seu schema for diferente.
    """
    params = urlencode({
        "select": "tmdb_id,imdb_id,content_type,view_count,created_at",
        "content_type": f"eq.{content_type}",
        "view_count":   f"gte.{min_views}",
        "order":        "view_count.desc",
        "limit":        str(limit),
    })

    url = f"{SUPABASE_URL}/rest/v1/content_views?{params}"
    print(f"  → GET {url}")

    data = _get(url)
    if data is None:
        return []

    if not isinstance(data, list):
        print(f"  ⚠  Resposta inesperada: {type(data)}", file=sys.stderr)
        return []

    print(f"  ✓  {len(data)} itens recebidos para content_type={content_type}")
    return data


# ── Escrita do JSON ───────────────────────────────────────────────────────────

def write_json(path: str, data: list, content_type: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "content_type": content_type,
        "count":        len(data),
        "items":        data,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(path) / 1024
    print(f"  ✓  {path} gravado ({len(data)} itens, {size_kb:.1f} KB)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n🚀  Gerando JSONs de trending — {datetime.now(timezone.utc).isoformat()}")
    print(f"    Supabase: {SUPABASE_URL}\n")

    errors = 0
    for content_type, limit, min_views, out_path in CONFIGS:
        print(f"📦  {content_type.upper()} (limit={limit}, min_views={min_views})")
        items = fetch_trending(content_type, limit, min_views)

        if not items:
            print(f"  ⚠  Nenhum item retornado para {content_type}. Pulando gravação.")
            errors += 1
            continue

        write_json(out_path, items, content_type)
        print()

    if errors:
        print(f"\n⚠  Concluído com {errors} erro(s).")
        sys.exit(1)
    else:
        print("✅  Todos os JSONs gerados com sucesso.")


if __name__ == "__main__":
    main()
