"""Free `raison_sociale + commune -> website domain` discovery.

Engine: the `ddgs` package (DuckDuckGo). It manages the vqd token, rotates
endpoints and uses a browser-impersonating HTTP client (`primp`), which gets
past the plain-scraping CAPTCHA that hits datacenter IPs. It can still be
rate-limited; the batch aborts early if every call fails.

Best-effort: a chunk of firms won't resolve -> marked `no_domain`, retried next
run.
"""

from __future__ import annotations

import urllib.parse

from ddgs import DDGS

# aggregators / directories / socials — never a cabinet's own site
_DIRECTORY_HOSTS = {
    "pagesjaunes.fr", "societe.com", "verif.com", "infogreffe.fr", "pappers.fr",
    "annuaire-entreprises.data.gouv.fr", "bodacc.fr", "score3.fr", "manageo.fr",
    "kompass.com", "europages.fr", "indeed.com", "fr.indeed.com", "linkedin.com",
    "fr.linkedin.com", "facebook.com", "m.facebook.com", "francetravail.fr",
    "pole-emploi.fr", "hellowork.com", "leboncoin.fr", "mappy.com", "yelp.fr",
    "yelp.com", "cylex-france.fr", "118000.fr", "justacote.com", "google.com",
    "google.fr", "wikipedia.org", "fr.wikipedia.org", "youtube.com", "twitter.com",
    "x.com", "instagram.com", "societe-france.com", "b-reputation.com",
    "dnb.com", "opencorporates.com", "figaro.fr", "lefigaro.fr", "ouest-france.fr",
    "leparisien.fr", "usine-digitale.fr", "chambre-des-notaires.fr",
    "notaires.fr", "cnb.avocat.fr", "avocat.fr", "experts-comptables.fr",
    "welcometothejungle.com", "glassdoor.fr", "verif.fr", "corporama.com",
    "lesechos.fr", "batiactu.com", "data.gouv.fr", "insee.fr", "service-public.fr",
    "orias.fr", "verspieren.com", "meilleurtaux.com", "lecomparateurassurance.com",
    # search engines that leak through DDG results
    "bing.com", "bing.fr", "duckduckgo.com", "qwant.com", "ecosia.org",
    "yahoo.com", "search.brave.com", "startpage.com", "yandex.com", "ask.com",
    # legal / formalities / data aggregators
    "doctrine.fr", "annuaire-commissaire-justice.fr", "infonet.fr", "bonial.fr",
    "leguichetdesformalites.fr", "guichet-entreprises.fr", "formalites.legalstart.fr",
    "legalstart.fr", "captaincontrat.com", "contract-factory.com", "agera.fr",
    "annuaire-mairie.fr", "l-expert-comptable.com", "compta-online.com",
    "editions-tissot.fr", "net-iris.fr", "village-justice.com", "avocats-conseils.com",
    "petites-affiches.fr", "actu-juridique.fr", "dalloz.fr", "lextenso.fr",
    "verif-siren.com", "entreprises.lefigaro.fr", "bilans-entreprises.fr",
    "companiesbook.com", "trouver-entreprise.com", "france-secret.com",
}

# host prefixes that are always search engines / never a firm site
_SEARCH_HOST_RE = ("bing.", "duckduckgo.", "qwant.", "ecosia.", "yahoo.", "yandex.", "search.")


class DiscoveryError(RuntimeError):
    """DDG refused the request (rate limit / block)."""


def _host(url: str) -> str | None:
    try:
        h = urllib.parse.urlparse(url).netloc.lower()
    except ValueError:
        return None
    if h.startswith("www."):
        h = h[4:]
    return h or None


def _registrable(host: str) -> str:
    parts = host.split(".")
    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "gouv", "asso"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _blocked(host: str) -> bool:
    if host.startswith(_SEARCH_HOST_RE):
        return True
    reg = _registrable(host)
    return reg in _DIRECTORY_HOSTS or reg.endswith(".gouv.fr")


def discover_domain(
    raison_sociale: str | None,
    commune: str | None,
    ddgs: DDGS,
) -> str | None:
    if not raison_sociale:
        return None
    q = raison_sociale if not commune else f"{raison_sociale} {commune}"
    try:
        results = ddgs.text(q, region="fr-fr", max_results=8)
    except Exception as e:  # ddgs raises its own exception types
        raise DiscoveryError(str(e)) from e
    for res in results or []:
        h = _host(res.get("href", ""))
        if h and not _blocked(h):
            return h
    return None
