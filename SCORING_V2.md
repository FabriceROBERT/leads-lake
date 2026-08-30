# Scoring V2 — score par segment

But : remplacer le score unique (aujourd'hui piloté à ~65 % par les offres
d'emploi) par un score à **composantes communes pondérées par segment**, alimenté
par des signaux financiers, des événements datés (fenêtre de pertinence) et une
source d'activité propre à chaque métier.

Décidé le 2026-08-28 : périmètre V1 = **toutes les sources**, y compris les
sources métier. Split `avocat_notaire` → `avocat` + `notaire` via l'annuaire des
notaires.

---

## 1. Composantes du score

Chaque composante est normalisée 0..1, puis `score = Σ poids[c] × composante[c]`,
cast 0..100. Les poids dépendent du segment.

| Composante | Définition | Source |
|---|---|---|
| `fit` | forme juridique structurée, ancienneté 2-15 ans | SIRENE (déjà là) |
| `contact` | adresse + code postal + géoloc présents | SIRENE (déjà là) |
| `taille` | proxy du volume d'actes : percentile de CA **dans le segment**, tranche effectif RNE, nb établissements | recherche-entreprises + SIRENE |
| `signal_rh` | offres d'emploi : volume, récence, métier pertinent, CDI | France Travail (déjà là, repondéré) |
| `signal_now` | max des **fenêtres d'événements datés**, chacune avec sa demi-vie | BODACC + RNE + SIRENE (delta) |
| `activite_metier` | source d'activité **propre au segment** (voir §3) | Sitadel / DVF / ORIAS / matching adresse / BODACC agrégé |
| `reforme` | « pourquoi maintenant » réglementaire, statique par segment | — |

### Fenêtres d'événements (`signal_now`)

| Événement | Source | Demi-vie | Segments concernés |
|---|---|---|---|
| Offre d'emploi | France Travail | 30-45 j | tous |
| Changement de dirigeant / d'associé | RNE + BODACC (modif) | 9-12 mois | tous |
| Acquisition d'un cabinet / cession de fonds (le cabinet est acquéreur) | BODACC (ventes et cessions) | 6-9 mois | EC, avocat, CGP, domicil. |
| Ouverture d'un établissement secondaire | SIRENE (delta `nb_etablissements` entre 2 `data_version`) | 6 mois | tous |
| Transfert de siège / modification statutaire | BODACC (modifications générales) | 3-6 mois | tous |
| CA en hausse > +10 % N/N-1 | recherche-entreprises `finances` | 12 mois | tous |

### Exclusions (hors liste, pas un malus)

Entité morte : radiée du RCS, liquidation clôturée, `etatAdministratifEtablissement = F`.

### Drapeaux (affichés au commercial, non pondérés)

`en redressement`, `nouveau dirigeant depuis <date>`, `a racheté un cabinet en
<date>`, `CA en baisse`, `comptes non déposés`.

---

## 2. Matrice de pondération (points max par composante, ≈100 / segment)

| Composante | EC | Avocat | Notaire | CGP | Promoteur | Domicil. |
|---|--:|--:|--:|--:|--:|--:|
| fit | 10 | 10 | 8 | 10 | 8 | 8 |
| contact | 10 | 10 | 10 | 10 | 10 | 10 |
| taille | 25 | 15 | 20 | 12 | 25 | 15 |
| signal_rh | 20 | 20 | 8 | 18 | 12 | 12 |
| signal_now | 15 | 15 | 14 | 25 | 20 | 20 |
| activite_metier | 5 | 15 | 20 | 8 | 15 | 25 |
| reforme | 15 | 5 | 10 | 15 | 0 | 0 |

`bande_score` : seuils à recalibrer par segment après le premier run (viser
~10 % chaud / ~30 % tiède).

---

## 3. Source d'activité par segment (`activite_metier`)

| Segment | Signal | Source | Statut source |
|---|---|---|---|
| **Expert-comptable** | (minimal) portefeuille estimé via CA | recherche-entreprises | ✅ branché |
| **Avocat** | densité de procédures collectives du département | BODACC agrégé | ✅ branché |
| **Notaire** | intensité transactionnelle locale (nb mutations 12 mois, valeur médiane) par commune/dept | **DVF** (Demandes de Valeurs Foncières) | ✅ open data data.gouv, gros fichier (~2 Go/an) |
| **CGP** | statut CIF/COA/MIA, ancienneté d'immatriculation | **ORIAS** | ⚠️ pas d'open data propre — recherche + captcha sur orias.fr ; à confirmer (fichier « inscriptions à l'Orias » sur lesdatalistes.fr à vérifier) |
| **Promoteur** | logements autorisés 12 mois par commune/dept (+ par SIREN si présent) | **Sitadel** | ✅ open data ; identification du maître d'ouvrage par SIREN **partielle** → surtout exploitable en agrégat commune/dept |
| **Domiciliation** | nb de sociétés hébergées à l'adresse du cabinet | matching d'adresse SIRENE (stock établissements déjà ingéré) | ✅ calcul interne, bruité |

### Split avocat / notaire

Le NAF 69.10Z couvre avocats, notaires, huissiers/commissaires de justice,
mandataires judiciaires. Discriminateur retenu : **croisement avec l'annuaire des
notaires**.

⚠️ Vérifié le 2026-08-28 : **pas de dataset open-data officiel** « annuaire des
notaires » avec SIREN sur data.gouv.fr. Options :
- scraper l'annuaire officiel `notaires.fr/fr/directory` (offices + adresses,
  SIREN à rapprocher via SIRENE sur adresse + nom) ;
- à défaut, heuristique sur la raison sociale (`NOTAIRE` / `NOTARIAL` /
  `OFFICE NOTARIAL`) en V1, annuaire en V2.

---

## 4. Nouveaux artefacts

### Ingestion (Bronze)

| Script | Sortie Bronze | DAG |
|---|---|---|
| `ingestion/dvf.py` | `source=dvf/dataset=mutations` | `ingestion_batch` |
| `ingestion/sitadel.py` | `source=sitadel/dataset=permis` | `ingestion_batch` |
| `ingestion/orias.py` | `source=orias/dataset=intermediaires` | `ingestion_batch` |
| `ingestion/annuaire_notaires.py` | `source=annuaire_notaires/dataset=offices` | `ingestion_batch` |
| (existants) `recherche_entreprises.py`, `bodacc.py` — passer `--source siege` sur tout le parc | | |

### Silver

| Job | Entrée | Sortie |
|---|---|---|
| `jobs/silver_enrichissement.py` | `bronze/recherche_entreprises` + `bronze/bodacc` | `silver/enrichissement` — 1 ligne/siren : CA, CA N-1, croissance, résultat, catégorie, tranche effectif RNE, flags BODACC, événements datés |
| `jobs/silver_zone_immobilier.py` | `bronze/dvf` | `silver/zone_immobilier` — par commune+dept : nb mutations 12 m, valeur médiane, index 0..1 |
| `jobs/silver_zone_construction.py` | `bronze/sitadel` | `silver/zone_construction` — par commune+dept : logements autorisés 12 m, index 0..1 |
| `jobs/silver_domiciliation_parc.py` | `bronze/sirene` (stock étab) + `silver/cabinet` (domiciliation) | `silver/domiciliation_parc` — par siret : nb sociétés hébergées |
| `jobs/silver_cabinet.py` (modif) | + `bronze/annuaire_notaires` | segment `avocat` / `notaire` ; `est_actif` depuis l'état SIRENE |

### Gold

`jobs/gold_leads_scored.py` (refonte) :
- joins : `silver/enrichissement` (siren), `silver/zone_immobilier` +
  `silver/zone_construction` (code_commune/dept), `silver/domiciliation_parc`
  (siret), `silver/orias` (siren) ;
- `SEGMENT_WEIGHTS: dict[str, dict[str, int]]` = la matrice §2 ;
- calcul par composante normalisée + somme pondérée ;
- `exclude` sur entité morte ;
- `flags` (array) + `score_detail` (map composante→points) exposés ;
- `motifs_score` enrichi des nouveaux événements.

### Schéma / API / Front

- `app/schemas/leads_schema.py` — champs issus de `silver/enrichissement` (déjà
  présents pour l'enrichissement à la volée), + `flags: list[str]`,
  `score_detail: dict`.
- `app/controllers/leads_controller.py` — `_enrich` devient un **fallback**
  (Gold porte déjà les champs) ; garder l'appel BODACC pour la fraîcheur de la
  liste d'événements.
- `leads-lake-front/src/lib/enums.ts` — `Segment` : ajouter `avocat`,
  `notaire`, retirer `avocat_notaire` ; libellés, pitch, map NAF (les deux →
  69.10Z).
- `leads-lake-front/src/app/page.tsx` — badges `flags` sur la fiche, détail du
  score optionnel.

### DAG

- `dags/ingestion_batch.py` — tâches `dvf`, `sitadel`, `orias`,
  `annuaire_notaires` → `trigger_silver`.
- `dags/silver.py` — `silver_enrichissement`, `silver_zone_immobilier`,
  `silver_zone_construction`, `silver_domiciliation_parc` après `silver_cabinet`,
  tous en amont de `gold`.
- `dags/_lib.py` — DVF est volumineux : `--driver-memory` dédié ou traitement
  par année.

---

## 5. Ordre de construction

**Phase A — enrichissement + refonte du score**
1. ✅ `silver_enrichissement.py` (ratios_financiers + BODACC + RNE fallback).
2. Backfill :
   - ✅ **financiers** : `ingestion.ratios_financiers` — 1 fichier bulk
     (data.economie.gouv.fr / `ratios_inpi_bce`), pas d'appel unitaire. Remplace
     le backfill `recherche_entreprises` (l'API a banni l'IP de la VM après
     ~5 500 requêtes — WAF OVH, `Connection refused`).
   - 🔄 **BODACC** : `ingestion.bodacc --source siege --resume` (checkpointé,
     part-files tous les 5 000). En cours (~40 %).
   - RNE (`recherche_entreprises`) : hors backfill. Reste pour la **fiche**
     (lookup 1 siren) une fois le ban levé ; dirigeants / Qualiopi seulement.
3. ✅ `gold_leads_scored.py` — `SEGMENT_WEIGHTS`, composantes `taille` /
   `signal_now` / `reforme`, `exclude`, `flags`.
4. ✅ Schéma + fiche (badges flags, bloc Contacts).

**Phase B — split avocat / notaire**
5. `ingestion/annuaire_notaires.py` (scraper notaires.fr ou heuristique).
6. `silver_cabinet.py` — segment split + `est_actif`.
7. `enums.ts` + front + `SEGMENT_METIERS` gold.

**Phase C — sources métier (`activite_metier`)**
8. `ingestion/dvf.py` + `silver_zone_immobilier.py` → notaire.
9. `ingestion/sitadel.py` + `silver_zone_construction.py` → promoteur.
10. `ingestion/orias.py` + `silver/orias` → CGP.
11. `silver_domiciliation_parc.py` → domiciliation.
12. `gold_leads_scored.py` — brancher `activite_metier` par segment.

**Phase D — orchestration + doc**
13. Tâches DAG, `DEPLOY.md` / `README.md`, run bout en bout sur la VM.

---

## 5 bis. Sous-système — enrichissement contacts par crawl

Objectif : téléphone / e-mail / dirigeant par cabinet, absents de SIRENE/RNE.
**Pas** Google Places (payant + CGU interdisent le stockage). À la place, un
**crawler propre** seedé par le Gold, sortie dans un **bucket / préfixe dédié**
(`bronze/source=crawl`), rafraîchi par Airflow.

### Deux étages, deux cadences

| Étage | Quoi | Cadence | Coût |
|---|---|---|---|
| Découverte `siren → domaine` | sirens sans domaine connu, ou découverte échouée / périmée | `@monthly` | 0 € |
| Crawl / refresh `domaine → contacts` | re-fetch des domaines connus, `next_due_at` par bande (chaud +30 j, tiède +90 j, froid +180 j) | `@weekly` | 0 € (VM) |

Le domaine est **caché à vie** → la découverte ne repasse que sur les ratés et
les nouveaux entrants. **100 % gratuit** (décision 2026-08-28).

Moteur : le paquet **`ddgs`** (DuckDuckGo). Le scraping HTML brut de DDG est
inutilisable depuis la VM — l'IP datacenter reçoit systématiquement une CAPTCHA
(« select all squares containing a duck »). `ddgs` gère le token vqd, la
rotation d'endpoints et un client HTTP qui imite un navigateur (`primp`), ce qui
passe. Il peut quand même être rate-limité : le batch `crawl_discovery`
s'interrompt après 10 échecs consécutifs (sirens laissés `pending`, re-tentés au
run suivant). Taux de résolution à confirmer en réel.

### Frontier = table Postgres dédiée (pas la file Airflow)

**Hébergement (décision 2026-08-28)** : conteneur **`postgres:16` dédié**
(`crawl-db`, `docker-compose.crawl.yml`, réseau `leads-lake-net`, ~30-50 Mo RAM
au repos). **Pas** la base d'Airflow : le frontier a son propre cycle de vie de
migrations et ne doit pas être emporté par un upgrade / reset Airflow ; il peut
aussi migrer plus tard vers un Postgres managé gratuit sans démêlage. Volume
Docker sauvegardé à part.

```sql
crawl_frontier(
  siren PK, domaine, priority,          -- priority = score du lead
  status,   -- pending | resolved_verified | resolved_unverified
            -- | no_domain | crawl_failed | dead
  attempts, last_crawled_at, next_due_at
)
```

Le DAG déclenche un **worker long-running** qui draine `next_due_at <= now()`
par `priority` desc, puis repositionne `next_due_at` selon la bande. Resumable :
chaque run traite ce qui est dû et écrit une partition datée.

### Crawl par domaine (peu profond)

`/`, `/contact`, `/nous-contacter`, `/mentions-legales`, `/equipe` + liens
same-domain à 1-2 niveaux. Extraction :
- `tel:` + regex `0[1-9]([ .\-]?\d{2}){4}`
- `mailto:` + regex e-mail
- **SIREN/SIRET sur `/mentions-legales`** → si == siren cible ⇒
  `siren_verifie_sur_site = true` (élimine les faux rapprochements nom↔domaine)
- bonus : dirigeant nommé, capital social, ville RCS

Politesse : 1 req / 1-2 s par domaine, cap global ~50-100, cache robots.txt,
backoff + retry, quarantaine des domaines morts, User-Agent identifiable.

### Bronze / Silver

```
bronze/source=crawl/dataset=contacts/data_version=<YYYY-MM-DD>/part-*.parquet
   siren, domaine, url_source, telephones[], emails[],
   siren_verifie_sur_site: bool, dirigeant_mentionne, rcs_ville,
   capital_social, http_status, robots_ok, crawled_at
```

`silver/contacts` : dernière valeur non nulle par siren sur toutes les
`data_version` (window siren, `crawled_at` desc), priorité à
`siren_verifie_sur_site = true`. → jointure Gold.

**Effet sur le score (décision 2026-08-28)** : la composante `contact` (poids 10,
bornée `0..10`) est redistribuée, sans inflation :

```
contact_raw = when(adresse & code_postal, 5)
            + when(latitude, 3)
            + when(telephone_verifie, 2)     # silver/contacts, siren_verifie_sur_site
```

Un téléphone vérifié = léger bonus (~2 pts bruts, dilués par la normalisation).
Tant que le crawl n'a pas tourné, le terme vaut 0 → aucun impact rétro.

**Affichage** : bloc « Contacts » dans la fiche (téléphone `tel:`, e-mail
`mailto:`, site web), badge « vérifié » si `siren_verifie_sur_site`.

### DAGs

- `crawl_discovery` (`@monthly`) : Gold → sirens sans domaine → DDG puis Common
  Crawl → upsert `crawl_frontier`.
- `crawl_contacts` (`@weekly`, `max_active_runs=1`) : K workers, drain, checkpoint
  Bronze, reprogramme `next_due_at`.
- `silver_contacts` (après chaque run crawl) : merge → `silver/contacts`.

### KPI couverture

`SELECT status, count(*) FROM crawl_frontier GROUP BY status` →
`gold/kpi_couverture_contacts` (% résolus vérifiés / non vérifiés / sans
domaine / morts).

### Légal

Contact public sur le site **de l'entreprise elle-même**, prospection B2B :
intérêt légitime RGPD. robots.txt respecté, opt-out tenu. Bloctel ne vise que
les particuliers. Stockage autorisé (contrairement à Google Places).

### Ordre de construction (après Phase A du scoring)

1. ✅ `docker-compose.crawl.yml` (`crawl-db` Postgres dédié) + `crawl/schema.sql`
   (`crawl_frontier`), `ensure_schema()` au démarrage de chaque batch.
2. ✅ `ingestion/crawl_discovery.py` (paquet `ddgs`) + `_discovery.py` (blocklist)
   + DAG `crawl_discovery`. Testé : ~94 % résolus, ~60 % de yield utile.
3. ✅ `ingestion/crawl_worker.py` + `_crawl_extract.py` (regex tel/mail/SIREN,
   vérif `/mentions-legales`) + DAG `crawl_contacts`.
4. ✅ `jobs/silver_contacts.py` + DAG `silver_contacts`.
5. ✅ Jointure `silver/contacts` dans `gold_leads_scored.py` (composante
   `contact` : +2 si `contact_verifie`) + passthrough `telephone` / `email` /
   `site_web`. **Reste** : `kpi_couverture_contacts`.
6. ✅ Fiche front : bloc « Contacts » (téléphone `tel:` / e-mail `mailto:` /
   site + badge « vérifié »).

**Worker v2 — collaborateurs (à faire après le 1ᵉʳ run complet)**
7. `crawl_worker.py` : extraire les pages `/equipe` / `/associes` /
   `/notre-equipe` en `collaborateurs: [{nom, fonction, email, telephone}]`
   (parsing `lxml` : bloc autour de chaque `mailto:` → nom = 2 mots capitalisés
   proches, fonction = mot-clé métier). Nouveau champ dans
   `bronze/source=crawl` + `silver/contacts`.
8. Fiche : bloc « Équipe ».
   Limites : ~40-60 % des sites avec page équipe, e-mails souvent masqués,
   RGPD (e-mail nominatif = donnée perso, opt-out par personne).
   Relance : remettre `crawl_frontier.next_due_at = now()` pour re-crawler.

---

## 6. Points ouverts

- **ORIAS** : confirmer une source exploitable (fichier lesdatalistes.fr ?
  export après recherche ?) sinon `activite_metier` CGP se rabat sur CA +
  effectif.
- **Sitadel** : taux de SIREN maître d'ouvrage renseigné → décider agrégat seul
  vs agrégat + per-SIREN.
- **Annuaire notaires** : scraper `notaires.fr` vs heuristique raison sociale
  pour la V1.
- **DVF** sur la VM ARM : traiter par année pour tenir la mémoire.
- Recalibrage des seuils `bande_score` par segment après le premier run complet.
- **Crawl contacts** : décisions prises — découverte 100 % gratuite (DDG + Common
  Crawl, ~70-80 % de résolution), `crawl_frontier` dans un Postgres dédié
  (`crawl-db`). Reste à valider : nombre de workers concurrents tenables sur la
  VM, et si `silver/contacts` alimente la composante `contact` du score ou reste
  purement affichage fiche.
