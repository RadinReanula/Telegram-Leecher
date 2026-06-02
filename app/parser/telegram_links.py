import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

# Public: t.me/username/123  |  Private: t.me/c/1867392134/42
_LINK_PATTERN = re.compile(
    r"(?:https?://)?(?:t(?:elegram)?\.me)/"
    r"(?:c/(?P<private_id>\d+)/(?P<private_msg>\d+)|"
    r"(?P<username>[a-zA-Z0-9_]+)/(?P<public_msg>\d+))",
    re.IGNORECASE,
)

_MESSAGE_URL_PATTERN = re.compile(
    r"https?://(?:t(?:elegram)?\.me)/[^\s<>\"']+",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ParsedLink:
    message_id: int
    chat_id: int | None = None
    username: str | None = None
    private_internal_id: int | None = None

    @property
    def is_private(self) -> bool:
        return self.private_internal_id is not None


@dataclass(frozen=True, slots=True)
class LinkExtractionResult:
    links: list[str] = field(default_factory=list)
    parsed: list[ParsedLink] = field(default_factory=list)
    invalid_count: int = 0


def _normalize_matched_url(url: str) -> str:
    return url.rstrip(".,);]}>")


def extract_telegram_links(text: str) -> LinkExtractionResult:
    seen: set[str] = set()
    links: list[str] = []
    parsed: list[ParsedLink] = []
    invalid_count = 0

    for match in _MESSAGE_URL_PATTERN.finditer(text):
        raw = _normalize_matched_url(match.group(0))
        if raw in seen:
            continue
        try:
            parsed_link = parse_telegram_link(raw)
        except ValueError:
            invalid_count += 1
            continue
        seen.add(raw)
        links.append(raw)
        parsed.append(parsed_link)

    return LinkExtractionResult(links=links, parsed=parsed, invalid_count=invalid_count)


def extract_telegram_link(text: str) -> str | None:
    result = extract_telegram_links(text)
    return result.links[0] if result.links else None


def parse_telegram_link(link: str) -> ParsedLink:
    normalized = link.strip()
    if not normalized.startswith("http"):
        normalized = f"https://{normalized.lstrip('/')}"

    parsed_url = urlparse(normalized)
    host = (parsed_url.netloc or "").lower().replace("www.", "")
    if host not in {"t.me", "telegram.me"}:
        raise ValueError("Not a Telegram message link (t.me / telegram.me).")

    path = parsed_url.path.strip("/")
    match = _LINK_PATTERN.search(f"https://t.me/{path}")
    if not match:
        raise ValueError(
            "Unsupported link format. Use t.me/channel/123 or t.me/c/1234567890/123."
        )

    if match.group("private_id"):
        internal_id = int(match.group("private_id"))
        message_id = int(match.group("private_msg"))
        chat_id = int(f"-100{internal_id}")
        return ParsedLink(
            chat_id=chat_id,
            message_id=message_id,
            private_internal_id=internal_id,
        )

    username = match.group("username")
    if username.lower() in {"joinchat", "addstickers", "share", "proxy", "socks", "iv"}:
        raise ValueError("Link must point to a channel/group message, not a special t.me path.")

    message_id = int(match.group("public_msg"))
    return ParsedLink(username=username, message_id=message_id)
