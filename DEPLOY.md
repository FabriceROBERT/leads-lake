# Déploiement — Oracle Cloud Always Free (0 €)

Tout tourne sur **une seule VM ARM** : Caddy + front Next + API FastAPI (web),
et Airflow + Kafka + Spark (pipeline). Le data lake reste sur Wasabi.

```
VM Oracle Always Free (ARM Ampere, ~3 OCPU / 18 Go)
 ├─ docker-compose.prod.yml     caddy (HTTPS) → front + api
 ├─ docker-compose.airflow.yml  airflow (webserver + scheduler + postgres)
 └─ docker-compose.spark.yml    kafka + images spark/py/tiles (lancées par Airflow)
```

---

## 1. Créer la VM

1. Compte **Oracle Cloud** (carte bancaire requise à l'inscription, non débitée).
2. *Compute → Instances → Create* :
   - Image : **Ubuntu 24.04** (ou 22.04)
   - Shape : **VM.Standard.A1.Flex** (ARM, *Always Free eligible*) — **3 OCPU / 18 Go**
     *(l'enveloppe gratuite = 4 OCPU / 24 Go ; si la capacité ARM manque, réessayer plus tard ou changer de region)*
   - Boot volume : 100 Go (gratuit jusqu'à 200 Go)
   - Ajouter ta **clé SSH publique**
3. Noter l'**IP publique**.

## 2. Ouvrir les ports 80 / 443

Deux niveaux de pare-feu sur Oracle, il faut faire les deux.

**a) Security List / NSG** (console Oracle) — *Networking → VCN → Security Lists → default* → *Add Ingress Rules* :

| Source | Protocole | Port |
|---|---|---|
| 0.0.0.0/0 | TCP | 80 |
| 0.0.0.0/0 | TCP | 443 |
| `<ton IP>/32` | TCP | 22 |

**b) iptables de la VM** (les images Oracle Ubuntu bloquent tout sauf 22) :

```bash
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save
```

## 3. Installer Docker

```bash
sudo apt update && sudo apt install -y ca-certificates curl git
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker            # ou se déconnecter / reconnecter
```

## 4. Nom de domaine gratuit (DuckDNS)

1. Sur [duckdns.org](https://www.duckdns.org) : se connecter, créer le domaine
   `papperless-leads` → tu obtiens `papperless-leads.duckdns.org`.
2. Champ *current ip* → mettre l'**IP publique de la VM**, *update*.
3. (Optionnel) garder l'IP à jour via cron :
   ```bash
   ( crontab -l 2>/dev/null; echo "*/15 * * * * curl -s 'https://www.duckdns.org/update?domains=papperless-leads&token=<TON_TOKEN>&ip=' >/dev/null" ) | crontab -
   ```

## 5. Récupérer le code

```bash
cd ~
git clone <URL_repo_backend>  leads-lake
git clone <URL_repo_front>    leads-lake-front
cd leads-lake
```

Les deux dépôts doivent être **côte à côte** (`docker-compose.prod.yml` référence `../leads-lake-front`).

## 6. Configurer `.env`

```bash
cp .env.example .env
nano .env
```

À renseigner :

```ini
ENV=prod
FRONT_URL=https://papperless-leads.duckdns.org
SITE_DOMAIN=papperless-leads.duckdns.org

LAKE_ROOT=s3://papperlesspreprod-leads-lake
S3_ENDPOINT_URL=https://s3.eu-west-2.wasabisys.com
S3_REGION=eu-west-2
AWS_ACCESS_KEY_ID=<clé Wasabi>
AWS_SECRET_ACCESS_KEY=<secret Wasabi>

FT_CLIENT_ID=<France Travail>
FT_CLIENT_SECRET=<France Travail>
```

`.env` est gitignoré — il ne quitte jamais la VM.

## 7. Lancer le web (front + API + HTTPS)

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Caddy obtient le certificat Let's Encrypt tout seul (ports 80/443 + DNS OK).
Ouvre **https://papperless-leads.duckdns.org** → la carte doit s'afficher.

Logs : `docker compose -f docker-compose.prod.yml logs -f caddy api front`

## 8. Lancer le pipeline (Airflow)

```bash
# réseau partagé attendu par les composes airflow/spark
docker network create leads-lake-net || true

# images
docker compose -f docker-compose.spark.yml --profile tools build      # spark + tiles
docker build -f docker/py.Dockerfile -t leads-lake-py:latest .        # tâches Python

# Kafka (pour le streaming France Travail)
docker compose -f docker-compose.spark.yml up -d kafka

# Airflow
docker compose -f docker-compose.airflow.yml up -d
```

Dans l'UI Airflow, **dépauser** les DAGs (`ingestion_batch`, `silver`, `gold`,
`france_travail`). `ingestion_batch` est `@monthly` et enchaîne Silver puis Gold.

### Accès à l'UI Airflow (port 8080) — **ne pas exposer publiquement**

Elle n'a pas d'auth durcie. Y accéder par tunnel SSH depuis ton poste :

```bash
ssh -L 8080:localhost:8080 ubuntu@<IP_VM>
# puis http://localhost:8080  (admin / admin)
```

## 9. Mémoire

Sur 18 Go : web + Airflow + Kafka ≈ 3-4 Go. Le pool Airflow `heavy` (1 slot)
sérialise les jobs Spark → `silver_cabinet` avec `--driver-memory 4g` + le
parquet SIRENE (~2 Go) passe. Sur une VM plus petite (12 Go), baisser
`--driver-memory` dans `dags/_lib.py` (ex. `2g`).

## 10. Mettre à jour

```bash
cd ~/leads-lake && git pull
cd ~/leads-lake-front && git pull
cd ~/leads-lake
docker compose -f docker-compose.prod.yml up -d --build
```

## Notes

- Le front et l'API sont sur le **même domaine** (Caddy route par chemin) → pas
  de CORS, `NEXT_PUBLIC_API_URL` vide = appels relatifs.
- L'API relit tout le Parquet Gold depuis Wasabi au 1er appel (~5 s) puis le
  garde en cache 5 min ; le `restart: unless-stopped` la garde vivante.
- Les enrichissements (recherche-entreprises, BODACC) sont des appels sortants
  HTTPS depuis la VM, aucun port entrant nécessaire.
- Oracle peut récupérer une VM *Always Free* totalement inactive 7 j : Airflow
  qui tourne suffit à l'éviter.
