"""Regex extraction of contact data from a firm's own web pages."""

from __future__ import annotations

import re

# French landline / mobile, tolerant of +33, spaces, dots, dashes, NBSP
_PHONE_RE = re.compile(
    r"(?<![\d./])(?:\+33[\s. -]?|0)\s?[1-9](?:[\s. -]?\d{2}){4}(?![\d])"
)
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# 9 digits, optionally + 5 (SIRET), with spaces/dots as separators
_SIREN_RE = re.compile(r"(?<!\d)(\d{3})[\s. ]?(\d{3})[\s. ]?(\d{3})(?![\d])")
_HREF_RE = re.compile(r"""href\s*=\s*["']([^"'#]+)["']""", re.I)

_EMAIL_JUNK = ("@example.", "@sentry.", "@2x", "@3x", "sentry.io", "wixpress.com", ".png", ".jpg", ".gif", ".svg")
_CONTACT_PATH_RE = re.compile(
    r"(mentions?-?l[ée]gales?|contact|nous-contacter|coordonn[ée]es|equipe|"
    r"[ée]quipe|team|cabinet|qui-sommes|about|a-propos|cgv|cgu)",
    re.I,
)


def phones(text: str) -> list[str]:
    out: list[str] = []
    for m in _PHONE_RE.findall(text):
        d = re.sub(r"\D", "", m)
        if d.startswith("33"):
            d = "0" + d[2:]
        if len(d) == 10 and d[0] == "0" and d[1] != "0":
            f = f"{d[0:2]} {d[2:4]} {d[4:6]} {d[6:8]} {d[8:10]}"
            if f not in out:
                out.append(f)
    return out


def emails(text: str) -> list[str]:
    out: list[str] = []
    for e in _EMAIL_RE.findall(text):
        el = e.lower()
        if any(j in el for j in _EMAIL_JUNK):
            continue
        if el not in out:
            out.append(el)
    return out


def sirens(text: str) -> set[str]:
    return {"".join(g) for g in _SIREN_RE.findall(text)}


def contact_links(html: str, base_host: str) -> list[str]:
    """Same-site URLs that look like a contact / legal page."""
    out: list[str] = []
    for href in _HREF_RE.findall(html):
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        if _CONTACT_PATH_RE.search(href):
            out.append(href)
    return out[:8]
