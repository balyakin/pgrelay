"""Bearer token authentication."""

import hmac

from fastapi import Header, HTTPException, status

from pgrelay.config.settings import Settings


def check_bearer_token(authorization: str, tokens: set[str]) -> bool:
    """Check bearer token without raising on non-ASCII input"""
    authorization_bytes = authorization.encode()
    for token in tokens:
        expected = f"Bearer {token}"
        expected_bytes = expected.encode()
        if hmac.compare_digest(authorization_bytes, expected_bytes):
            return True
    return False


async def require_api_token(
    settings: Settings,
    authorization: str | None = Header(default=None),
) -> None:
    """Require a configured bearer token when API tokens are configured."""
    tokens = settings.get_api_tokens()
    if not tokens:
        return
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": {
                    "code": "unauthorized",
                    "message": "Missing bearer token",
                    "details": {},
                }
            },
        )

    if not check_bearer_token(authorization, tokens):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": {
                    "code": "unauthorized",
                    "message": "Invalid bearer token",
                    "details": {},
                }
            },
        )
