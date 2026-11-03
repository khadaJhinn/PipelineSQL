---
language:
- fr
license: etalab-2.0
tags:
- energy
- electricity
- rte
---

# Consommation électrique en France — RTE

Pipeline reproductible : **API RTE → JSON Lines et Excel brut → transformation par
lots → Parquet → base SQL DuckDB → Excel relu depuis SQL → analyses SQL**.

La publication sur Hugging Face est réalisée **manuellement**, selon le choix retenu
pour ce projet. Le programme prépare les fichiers à déposer. Le périmètre retenu
couvre le rendu technique, sans oral.

Source : [RTE / ODRÉ, éCO2mix national temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/).
Les données sources sont sous Licence Ouverte 2.0 / Etalab, avec attribution à RTE.
Les puissances sont en **MW**, les données temps réel sont provisoires.

## Lancer le projet

Depuis le dossier PipelineSQL, dans PowerShell :

```powershell
# Installation initiale ou mise à jour des dépendances après récupération du code.
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt

# Pipeline complet sur les 7 derniers jours disponibles.
.venv\Scripts\python app.py run --days 7
```

Python 3.11 ou plus récent est nécessaire. Aucune clé d'API n'est nécessaire pour
cette source publique. Sans argument, `python app.py` lance également sept jours.
La dernière date est déterminée à partir des consommations mesurées, en excluant
les prévisions futures sans mesure.

Les chemins des fichiers sont affichés à la fin. Pour une nouvelle collecte des
mêmes dates, y compris les corrections RTE, utiliser `--refresh` :

```powershell
.venv\Scripts\python app.py run --days 60 --batch-size 500 --refresh
```

Une période explicite utilise des dates françaises, début inclus et fin **exclue** :

```powershell
.venv\Scripts\python app.py run --start 2026-09-05 --end 2026-11-04
```

Cette période couvre le 5 septembre au 3 novembre inclus. Le dernier jour peut
être incomplet. Une période contient au maximum 366 jours, dans la limite des
données effectivement disponibles dans le jeu temps réel.

## Les fichiers produits

```text
PipelineSQL/
├── sql/
│   ├── 001_schema.sql
│   ├── 002_indexes.sql
│   └── analyses.sql
├── scripts/
│   ├── ingest/rte.py
│   ├── transform/clean.py
│   ├── load/database.py
│   ├── publish/prepare.py
│   ├── analyze.py
│   └── pipeline.py
├── data/                         # Jamais versionné dans Git
│   ├── raw/<snapshot>/
│   │   ├── AAAA-MM-JJ_00000.jsonl
│   │   ├── brut.xlsx
│   │   └── manifest.json
│   ├── checkpoints/<identite>.json
│   ├── processed/<snapshot>/
│   │   ├── consommation.parquet
│   │   ├── consommation_sql.xlsx
│   │   ├── quality.json
│   │   ├── load_report.json
│   │   └── pipeline_report.json
│   ├── reports/<snapshot>/
│   │   ├── *.csv                 # Résultats des 7 analyses
│   │   ├── analysis_report.json
│   │   └── index_explain_analyze.txt
│   ├── publish/<snapshot>/
│   │   ├── README.md             # Dataset Card à déposer sur le Hub
│   │   ├── provenance.json
│   │   └── data/consommation.parquet
│   └── electricity.duckdb
├── tests/
├── app.py
└── README.md
```

Double-cliquer sur les Excel pour les consulter. Les CSV d'analyse s'ouvrent aussi
dans Excel. La nouvelle base SQL est `data/electricity.duckdb`, utilisable avec un
client DuckDB. **L'ancienne base `electricity.sqlite` et les anciennes archives restent
conservées** ; les nouvelles exécutions utilisent DuckDB, qui permet
[`EXPLAIN ANALYZE`](https://duckdb.org/docs/current/guides/meta/explain_analyze).

## Ingestion et reprise

La collecte utilise **aiohttp**, un timeout total de 30 secondes et un sémaphore
limitant la concurrence à quatre requêtes. La pagination traite 100 lignes par page
sur des fenêtres journalières françaises, y compris aux changements d'heure.
Les erreurs réseau, timeouts et HTTP 408, 429, 500, 502, 503, 504 sont réessayés
trois fois avec attente exponentielle. Les autres erreurs HTTP arrêtent la collecte.

Chaque ligne JSONL conserve un enregistrement source complet. `brut.xlsx` contient
les mêmes enregistrements et tous leurs champs, avant nettoyage métier.
Les noms de pages sont déterministes : `AAAA-MM-JJ_offset.jsonl`. L'identité du
snapshot dépend de la source et des dates, avec une génération `v001`, `v002`, etc.

Un checkpoint est écrit atomiquement après chaque page. Les pages déjà reçues et
leurs empreintes SHA-256 sont vérifiées à la reprise. Pour reprendre une collecte
interrompue, relancer la même période **sans `--refresh`**, ou fournir son checkpoint :

```powershell
.venv\Scripts\python app.py run --resume data/checkpoints/IDENTITE.json
```

Remplacer `IDENTITE.json` par le fichier présent dans `data/checkpoints/`.
`--resume` ne se combine pas avec `--start`, `--end` ou `--refresh`.

Le checkpoint mutable est séparé des fichiers bruts. Le manifeste final est créé
uniquement après collecte et export Excel complets. Les archives brutes terminées
ne sont jamais réécrites. Une collecte complète existante est réutilisée ; `--refresh`
crée une nouvelle génération et conserve l'ancienne. Si le nombre de lignes change
pendant une pagination, la collecte s'arrête et demande un nouveau snapshot.

## Transformation et chargement

Les JSONL sont lus par lots (1 000 lignes par défaut). Une base de travail SQLite
temporaire déduplique et trie les mesures **sur disque**, y compris entre deux lots.
La sortie Parquet est écrite progressivement avec un schéma Arrow stable et des
groupes de lignes bornés. La base de travail est supprimée en fin de traitement.

Le nettoyage valide le périmètre France, les dates avec fuseau, les nombres finis
et une consommation positive ou nulle. Les prévisions sans consommation mesurée
restent dans le brut. Les valeurs manquantes restent nulles, les échanges et le
pompage conservent leur signe. Les doublons identiques sont supprimés et les
mesures contradictoires arrêtent la transformation.

La production totale additionne les huit filières principales. Les sous-filières
ne sont pas additionnées de nouveau ; si une filière manque, le total reste nul.
Les champs sources supplémentaires restent disponibles dans le brut.

Le chargement DuckDB lit les groupes Parquet par lots, dans une transaction unique.
La clé primaire `(date_heure_utc, perimetre)` empêche les doublons. Les révisions
récentes mettent les mesures à jour ; une archive récupérée plus tôt ne peut pas
écraser une révision récupérée plus récemment. Les contraintes vérifient les dates,
la consommation, le périmètre et la provenance vers `pipeline_runs`.

L'Excel final est écrit en continu, **après validation de la transaction et relecture
en SQL**, pour les instants de cette exécution. La base conserve les instants des
exécutions précédentes. Si l'export échoue après le chargement, relancer `load`
régénère l'Excel sans créer de doublons. DuckDB utilise un budget de buffers de
256 Mo et deux threads ; ce budget ne représente pas toute la RAM du processus.

## Étapes séparées

```powershell
.venv\Scripts\python app.py ingest --days 7
.venv\Scripts\python app.py transform --raw data/raw/SNAPSHOT --batch-size 500
.venv\Scripts\python app.py load --processed data/processed/SNAPSHOT --batch-size 500
.venv\Scripts\python app.py analyze
```

Remplacer `SNAPSHOT` par le nom affiché lors de la collecte. Les étapes complètes
peuvent être relancées ; une transformation existante est vérifiée et réutilisée.
Le lot Parquet d'origine ne change pas lors d'une réutilisation.

## Analyses SQL et index

[sql/analyses.sql](sql/analyses.sql) contient sept analyses :

1. Dernières mesures disponibles.
2. Statistiques journalières avec `GROUP BY`.
3. Évolution entre journées disponibles avec CTE et `LAG`.
4. Trois pointes par jour avec `ROW_NUMBER`.
5. Moyenne glissante sur quatre mesures disponibles.
6. Provenance des mesures avec `LEFT JOIN`.
7. Mix journalier sur les instants où toutes les filières sont disponibles.

Les requêtes ne comblent pas les trous et ne présentent pas les MW comme des MWh.
Quatre mesures disponibles ne constituent pas toujours une heure complète.

L'index défini dans [sql/002_indexes.sql](sql/002_indexes.sql) cible
`consommation_mw`. Le benchmark recherche le nombre de mesures et les bornes
temporelles correspondant à une puissance précise, ici la puissance maximale
observée. Il compare la même requête avant et après création de cet index, vérifie
des résultats identiques, enregistre les deux plans **EXPLAIN ANALYZE**, et mesure
la médiane de 31 exécutions après trois échauffements.

Le rapport indique si le moteur a effectivement choisi un **Index Scan**.
Un ratio avant/après supérieur à 1 indique un gain ; inférieur à 1, un ralentissement.
Sur quelques milliers de lignes, les coûts fixes peuvent masquer le gain ou rendre
l'index moins rapide. Le rapport conserve le résultat réel, sans promettre de gain.
Seul l'index nommé par ce projet est recréé pendant cette mesure.

## Dépôt manuel sur Hugging Face

Préparer le dossier à déposer :

```powershell
.venv\Scripts\python app.py prepare-hub --processed data/processed/SNAPSHOT
```

Le dossier affiché contient le Parquet, un `README.md` qui sert de Dataset Card
et un fichier de provenance sans chemins personnels. Cette commande reste locale
et ne demande aucun token. Elle protège les fichiers déjà préparés et une carte
modifiée à la main contre l'écrasement.

Sur Hugging Face, créer un dépôt de type **Dataset**, privé pendant le développement,
puis déposer le contenu du dossier préparé en conservant le sous-dossier `data/`.
La [documentation officielle du dépôt manuel](https://huggingface.co/docs/hub/datasets-adding)
décrit l'ajout des fichiers dans l'onglet « Files and versions ».

Après le dépôt, créer le tag **v1.0.0** sur le dépôt Hugging Face pour figer cette
version. Les [tags du Hub](https://huggingface.co/docs/huggingface_hub/guides/repository#branches-and-tags)
désignent une révision du dépôt ; écrire « v1.0.0 » dans une carte ne crée pas ce tag.
Le script de publication automatique prévu dans le PDF est remplacé ici par la
préparation locale et le dépôt manuel, conformément au choix demandé.

## Dataset Card

La [carte complète générée](scripts/publish/dataset_card.md) documente la source,
la licence, le schéma, les dates réelles, les volumes, le nettoyage, les limites
et les empreintes. Elle est remplie à partir du snapshot lors de `prepare-hub`.

| Élément | Description |
| --- | --- |
| Producteur | RTE, distribution via ODRÉ |
| Jeu source | `eco2mix-national-tr`, API Explore v2.1 |
| Périmètre | France, national |
| Fréquence source | Quart d'heure pour la consommation et les filières principales |
| Licence | Licence Ouverte 2.0 / Etalab, attribution RTE et date de collecte |
| Données | Télémesures, forfaits et estimations provisoires |
| Sortie | Parquet, une ligne par instant UTC et périmètre |
| Unités | MW et gCO₂/kWh pour l'intensité carbone |
| Limites | Dernier jour incomplet, trous possibles, révisions et historique limité |

Les horodatages Parquet sont des textes ISO 8601 avec décalage explicite. Les mesures
sont des doubles, nullables sauf la consommation. Les MW mesurent une puissance ;
le pipeline n'intègre pas d'énergie en MWh et n'interpole aucune valeur manquante.
Pour un historique consolidé, consulter le [jeu RTE consolidé et définitif](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-cons-def/).
Le portail annonce un rafraîchissement toutes les quinze minutes et un quota de
50 000 appels par utilisateur et par mois.

## Configuration

Les valeurs par défaut fonctionnent sans fichier `.env`. Pour les modifier :

```powershell
Copy-Item .env.example .env
```

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `RTE_API_BASE` | API publique ODRÉ v2.1 | Adresse du catalogue |
| `RTE_DATASET` | `eco2mix-national-tr` | Jeu source |
| `DATA_DIR` | `data` | Racine des archives et résultats |
| `DATABASE_PATH` | `data/electricity.duckdb` | Base DuckDB |
| `BATCH_SIZE` | `1000` | Lignes par lot |

Les chemins d'environnement relatifs sont résolus depuis la racine du dépôt.
`--database` et `--output` acceptent aussi des chemins relatifs au dossier de lancement.
En l'absence de `DATABASE_PATH`, la base est créée dans `DATA_DIR`.
L'ancien réglage `SQLITE_PATH` est remplacé par `DATABASE_PATH`.

Le dossier `data/` entier et les secrets sont exclus de Git. Les dépendances de
publication Hugging Face ne sont pas nécessaires à l'exécution du projet.

## Tests et résultats mesurés

```powershell
.venv\Scripts\python -m pytest -q
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

**40 tests** couvrent un serveur HTTP local, la pagination, la concurrence bornée,
les reprises exponentielles, les timeouts, les checkpoints, les verrouillages
Windows, les changements d'heure, les doublons entre lots, les schémas nullables,
la transaction et le rollback entre lots, les exports, les sept analyses, l'index,
le lancement complet de la commande et la préparation locale du dataset.
La CI GitHub Actions exécute les tests sous Windows et Linux, sans appel réel à RTE.

Exécution réelle vérifiée le **3 novembre 2026**, sur 60 jours du **5 septembre au
3 novembre inclus** : **5 760 lignes brutes**, **52 lignes sans consommation mesurée**,
**5 708 mesures chargées** et des lots de **500 lignes**.

`pipeline_report.json` contient les durées et pics RSS de chaque étape, les volumes
bruts et Parquet, et l'indication d'une transformation réutilisée.
Les pics RSS concernent le processus entier et sont échantillonnés toutes les 20 ms ;
ils incluent les bibliothèques et caches. Les durées d'une reprise concernent cette
invocation, pas les tentatives précédentes. Les résultats de l'index et ses plans
sont dans `analysis_report.json` et `index_explain_analyze.txt`.

Les anciennes données restent sur la machine. Le prototype initial `app.py` est
conservé dans le premier commit `e8f8e17`.
