"""Snowflake REST API client for SQL API and Snowpipe Streaming."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from base64 import b64encode
from typing import Any
from urllib.parse import quote

import aiohttp
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import jwt

_LOGGER = logging.getLogger(__name__)

_TOKEN_REFRESH_MARGIN = 50 * 60


class SnowflakeRestClient:
    """Async client for Snowflake SQL API and Snowpipe Streaming."""

    def __init__(self, account: str, account_locator: str, user: str, private_key_pem: str) -> None:
        self._account = account.upper()
        self._account_locator = account_locator.upper()
        self._user = user.upper()
        self._private_key = load_pem_private_key(
            private_key_pem.encode(), password=None
        )
        # Hostnames can't contain underscores, so Snowflake uses hyphens in the URL
        self._account_url = (
            f"https://{account.lower().replace('_', '-')}.snowflakecomputing.com"
        )
        self._ingest_host: str | None = None
        self._scoped_token: str | None = None
        self._token_expires: float = 0

    def _get_public_key_fingerprint(self) -> str:
        pub_der = self._private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        digest = hashlib.sha256(pub_der).digest()
        return "SHA256:" + b64encode(digest).decode()

    def _generate_jwt(self) -> str:
        now = int(time.time())
        fingerprint = self._get_public_key_fingerprint()
        qualified_user = f"{self._account_locator}.{self._user}"
        issuer = f"{qualified_user}.{fingerprint}"
        payload = {
            "iss": issuer,
            "sub": qualified_user,
            "iat": now,
            "exp": now + 59 * 60,
        }
        return jwt.encode(payload, self._private_key, algorithm="RS256")

    # ── SQL API (/api/v2/statements) ──

    async def execute_sql(
        self,
        session: aiohttp.ClientSession,
        statement: str,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        bindings: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Execute a SQL statement via the Snowflake SQL API. Returns the response JSON.

        bindings maps "1", "2", ... to {"type": ..., "value": ...} for each ? placeholder.
        """
        jwt_token = self._generate_jwt()
        url = f"{self._account_url}/api/v2/statements"
        body: dict[str, Any] = {"statement": statement, "timeout": 60}
        if database:
            body["database"] = database
        if schema:
            body["schema"] = schema
        if warehouse:
            body["warehouse"] = warehouse
        if bindings:
            body["bindings"] = bindings
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        async with session.post(url, headers=headers, json=body) as resp:
            result = await resp.json()
            if resp.status != 200:
                msg = result.get("message", f"SQL API error {resp.status}")
                raise RuntimeError(msg)
            return result

    # ── Snowpipe Streaming (Elastic Channel) ──

    async def _get_ingest_host(self, session: aiohttp.ClientSession) -> str:
        if self._ingest_host:
            return self._ingest_host
        jwt_token = self._generate_jwt()
        url = f"{self._account_url}/v2/streaming/hostname"
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Accept": "application/json",
        }
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            host = (await resp.text()).strip()
            self._ingest_host = host.replace("_", "-")
            _LOGGER.debug("Streaming ingest host: %s", self._ingest_host)
            return self._ingest_host

    async def _get_scoped_token(self, session: aiohttp.ClientSession) -> str:
        now = time.time()
        if self._scoped_token and now < self._token_expires:
            return self._scoped_token
        ingest_host = await self._get_ingest_host(session)
        jwt_token = self._generate_jwt()
        url = f"{self._account_url}/oauth/token"
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "scope": ingest_host,
        }
        async with session.post(url, headers=headers, data=data) as resp:
            resp.raise_for_status()
            body = await resp.text()
            try:
                parsed = json.loads(body)
                self._scoped_token = parsed["token"]
            except (json.JSONDecodeError, KeyError):
                self._scoped_token = body.strip()
            self._token_expires = now + _TOKEN_REFRESH_MARGIN
            _LOGGER.debug("Obtained scoped streaming token")
            return self._scoped_token

    async def send_rows(
        self,
        session: aiohttp.ClientSession,
        database: str,
        schema: str,
        table: str,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0
        ingest_host = await self._get_ingest_host(session)
        token = await self._get_scoped_token(session)
        ndjson_lines = [json.dumps(row, default=str) for row in rows]
        payload = "\n".join(ndjson_lines) + "\n"
        url = (
            f"https://{ingest_host}/v2/streaming/data"
            f"/databases/{quote(database, safe='')}/schemas/{quote(schema, safe='')}"
            f"/tables/{quote(table, safe='')}/rows"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-ndjson",
            "Accept": "application/json",
        }
        async with session.post(url, headers=headers, data=payload.encode()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Streaming insert failed ({resp.status}): {body}")
            _LOGGER.debug("Streamed %d rows to %s.%s.%s", len(rows), database, schema, table)
            return len(rows)
