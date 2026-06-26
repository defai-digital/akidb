#!/usr/bin/env python3
"""OpenAI-compatible embedding sidecar backed by current ax-engine.

ax-engine 6.x exposes embeddings through the native Python Session API, not by
running `ax-engine serve <embedding-alias>`. This sidecar keeps AkiDB's Rust
client on a stable `/v1/embeddings` HTTP contract while using
`Session.embed_batch_flat_bytes()` underneath.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any


LOG = logging.getLogger("akidb.ax_engine_embedding_server")


def _load_tokenizer(model_dir: Path):
    try:
        from transformers import AutoTokenizer

        return AutoTokenizer.from_pretrained(str(model_dir), trust_remote_code=True)
    except Exception as transformers_error:
        tokenizer_json = model_dir / "tokenizer.json"
        if not tokenizer_json.is_file():
            raise RuntimeError(
                "failed to load tokenizer with transformers, and tokenizer.json "
                f"was not found in {model_dir}: {transformers_error}"
            ) from transformers_error

        try:
            from tokenizers import Tokenizer
        except ImportError as tokenizers_error:
            raise RuntimeError(
                "tokenizers is required when transformers cannot load the tokenizer"
            ) from tokenizers_error

        return Tokenizer.from_file(str(tokenizer_json))


def _tokenize(tokenizer: Any, text: str) -> list[int]:
    if hasattr(tokenizer, "encode") and callable(tokenizer.encode):
        encoded = tokenizer.encode(text, add_special_tokens=True)
        if hasattr(encoded, "ids"):
            token_ids = list(encoded.ids)
        else:
            token_ids = list(encoded)
    else:
        raise RuntimeError("tokenizer does not expose an encode method")

    eos_id = getattr(tokenizer, "eos_token_id", None)
    if eos_id is None and hasattr(tokenizer, "token_to_id"):
        eos_id = tokenizer.token_to_id("<|endoftext|>") or tokenizer.token_to_id("</s>")

    if eos_id is not None and (not token_ids or token_ids[-1] != eos_id):
        token_ids.append(int(eos_id))
    return token_ids


class EmbeddingRuntime:
    def __init__(
        self,
        *,
        model_dir: Path,
        model_id: str,
        max_batch_tokens: int,
        pooling: str,
        normalize: bool,
    ) -> None:
        if not model_dir.is_dir():
            raise RuntimeError(f"model directory does not exist: {model_dir}")
        manifest = model_dir / "model-manifest.json"
        if not manifest.is_file():
            raise RuntimeError(
                "model directory is not an ax-engine native artifact directory; "
                f"expected {manifest}. Raw Hugging Face snapshots must be converted "
                "or replaced with ax-engine-compatible artifacts before use."
            )

        from ax_engine import Session

        self.model_dir = model_dir
        self.model_id = model_id
        self.pooling = pooling
        self.normalize = normalize
        self.tokenizer = _load_tokenizer(model_dir)
        self._session_lock = threading.Lock()
        self.session = Session(
            model_id=model_id,
            mlx=True,
            mlx_model_artifacts_dir=str(model_dir),
            max_batch_tokens=max_batch_tokens,
        )
        LOG.info("loaded ax-engine embedding session model_id=%s model_dir=%s", model_id, model_dir)

    def close(self) -> None:
        self.session.close()

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        batch_token_ids = [_tokenize(self.tokenizer, text) for text in texts]
        with self._session_lock:
            blob, batch_size, hidden_size = self.session.embed_batch_flat_bytes(
                batch_token_ids,
                pooling=self.pooling,
                normalize=self.normalize,
            )

        import numpy as np

        array = np.frombuffer(blob, dtype=np.float32).reshape(batch_size, hidden_size)
        return array.astype(np.float32, copy=False).tolist()


class EmbeddingHandler(BaseHTTPRequestHandler):
    runtime: EmbeddingRuntime
    api_key: str | None = None

    server_version = "AkiDbAxEngineEmbedding/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str, code: str) -> None:
        self._send_json(
            status,
            {
                "error": {
                    "message": message,
                    "type": "invalid_request_error" if status < 500 else "server_error",
                    "code": code,
                }
            },
        )

    def _authorize(self) -> bool:
        if not self.api_key:
            return True
        expected = f"Bearer {self.api_key}"
        actual = self.headers.get("Authorization", "")
        if actual != expected:
            self._send_error_json(401, "missing or invalid bearer token", "unauthorized")
            return False
        return True

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(
                200,
                {
                    "status": "ok",
                    "backend": "ax-engine",
                    "model": self.runtime.model_id,
                    "model_dir": str(self.runtime.model_dir),
                },
            )
            return
        self._send_error_json(404, "not found", "not_found")

    def do_POST(self) -> None:
        if self.path != "/v1/embeddings":
            self._send_error_json(404, "not found", "not_found")
            return
        if not self._authorize():
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_error_json(400, "invalid content length", "invalid_content_length")
            return

        try:
            request = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as error:
            self._send_error_json(400, f"invalid JSON: {error}", "invalid_json")
            return

        raw_input = request.get("input")
        if isinstance(raw_input, str):
            texts = [raw_input]
        elif isinstance(raw_input, list) and all(isinstance(item, str) for item in raw_input):
            texts = raw_input
        else:
            self._send_error_json(
                400,
                "input must be a string or an array of strings",
                "invalid_input",
            )
            return

        try:
            embeddings = self.runtime.embed_texts(texts)
        except Exception as error:
            LOG.exception("embedding request failed")
            self._send_error_json(500, str(error), "embedding_failed")
            return

        response_model = request.get("model") or self.runtime.model_id
        self._send_json(
            200,
            {
                "object": "list",
                "model": response_model,
                "data": [
                    {
                        "object": "embedding",
                        "index": index,
                        "embedding": embedding,
                    }
                    for index, embedding in enumerate(embeddings)
                ],
                "usage": {
                    "prompt_tokens": 0,
                    "total_tokens": 0,
                },
            },
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", required=True, help="Local Qwen embedding model artifact directory")
    parser.add_argument("--model-id", default="Qwen/Qwen3-Embedding-4B", help="Model name returned in responses")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--max-batch-tokens", type=int, default=4096)
    parser.add_argument("--pooling", choices=["last", "mean", "cls"], default="last")
    parser.add_argument("--no-normalize", action="store_true", help="Disable L2 normalization")
    parser.add_argument("--api-key", default=None, help="Optional bearer token required for requests")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    runtime = EmbeddingRuntime(
        model_dir=Path(args.model_dir).expanduser().resolve(),
        model_id=args.model_id,
        max_batch_tokens=args.max_batch_tokens,
        pooling=args.pooling,
        normalize=not args.no_normalize,
    )

    EmbeddingHandler.runtime = runtime
    EmbeddingHandler.api_key = args.api_key
    server = HTTPServer((args.host, args.port), EmbeddingHandler)

    def stop(_signum: int, _frame: Any) -> None:
        LOG.info("shutting down")
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    LOG.info("listening on http://%s:%s/v1/embeddings", args.host, args.port)
    try:
        server.serve_forever()
    finally:
        runtime.close()
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
