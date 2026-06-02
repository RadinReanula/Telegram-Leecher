import logging
import ssl

import certifi
from aiogram.client.session.aiohttp import AiohttpSession

logger = logging.getLogger(__name__)


def build_ssl_context(*, verify: bool) -> ssl.SSLContext:
    if not verify:
        logger.warning("BOT_SSL_VERIFY=false — SSL certificate verification is disabled for the bot.")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # certifi bundle only (merging Windows store can break with some antivirus SSL inspection).
    return ssl.create_default_context(cafile=certifi.where())


def configure_process_ssl_env() -> None:
    ca_bundle = certifi.where()
    import os

    os.environ.setdefault("SSL_CERT_FILE", ca_bundle)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", ca_bundle)


def create_bot_session(*, timeout: float, verify_ssl: bool) -> AiohttpSession:
    if verify_ssl:
        configure_process_ssl_env()
    else:
        logger.warning(
            "Bot HTTPS certificate verification is DISABLED (BOT_SSL_VERIFY=false)."
        )

    session = AiohttpSession(timeout=timeout)
    session._connector_init["ssl"] = build_ssl_context(verify=verify_ssl)
    session._should_reset_connector = True
    if session._session is not None and not session._session.closed:
        # Ensure the next request picks up the new SSL settings.
        session._session = None
    return session
