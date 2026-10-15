"""Snowpipe Streaming Elastic Channel REST API client."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from base64 import b64encode
from typing import Any

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import jwt

_LOGGER = logging.getLogger(__name__)

# Scoped tokens are valid for ~60 min; refresh at 50 min
_TOKEN_REFRESH_MARGIN = 50 * 60


class SnowflakeStreamingClient:
    """Async client for Snowpipe Streaming Elastic Channel REST API."""

    def __init__(self, account: str, account_locator: str, user: str, private_key_pem: str) -> None:
        self._account = account.upper()
        self._account_locator = account_locator.upper()
        self._user = user.upper()
        self._private_key_pem = private_key_pem
        self._private_key = load_pem_private_key(
            private_key_pem.encode(), password=None
        )
        self._account_url = f"https://{account}.snowflakecomputing.com"
        self._ingest_host: str | None = None
        self._scoped_token: str | None = None
        self._token_expires: float = 0

    def _get_public_key_fingerprint(self) -> str:
        """SHA-256 fingerprint of the public key in Snowflake format."""
        pub_der = self._private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        digest = hashlib.sha256(pub_der).digest()
        return "SHA256:" + b64encode(digest).decode()

    def _generate_jwt(self) -> str:
        """Generate a JWT for Snowflake key-pair auth."""
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

    async def _get_ingest_host(self, session: aiohttp.ClientSession) -> str:
        """Get the streaming ingest hostname (cached)."""
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
            # Replace underscores with dashes per Snowflake docs
            self._ingest_host = host.replace("_", "-")
            _LOGGER.debug("Streaming ingest host: %s", self._ingest_host)
            return self._ingest_host

    async def _get_scoped_token(self, session: aiohttp.ClientSession) -> str:
        """Exchange JWT for a scoped streaming token (cached with TTL)."""
        now = time.time()
        if self._scoped_token and now < self._token_expires:
            return self._scoped_token

        ingest_host = await self._get_ingest_host(session)
        jwt_token = self._generate_jwt()
        # Token exchange goes to the account (control) URL, not the ingest host
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
            # Response may be raw token text or JSON {"token": "..."}
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
        """Send rows via Elastic Channel. Returns count of rows sent."""
        if not rows:
            return 0

        ingest_host = await self._get_ingest_host(session)
        token = await self._get_scoped_token(session)

        # Build NDJSON payload
        ndjson_lines = []
        for row in rows:
            ndjson_lines.append(json.dumps(row, default=str))
        payload = "\n".join(ndjson_lines) + "\n"

        url = (
            f"https://{ingest_host}/v2/streaming/data"
            f"/databases/{database}/schemas/{schema}"
            f"/tables/{table}/rows"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-ndjson",
            "Accept": "application/json",
        }
        async with session.post(url, headers=headers, data=payload.encode()) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"Streaming insert failed ({resp.status}): {body}"
                )
            _LOGGER.debug("Streamed %d rows to %s.%s.%s", len(rows), database, schema, table)
            return len(rows)
