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

Pipeline Python : API publique éCO2mix → Excel brut + JSON immuable →
transformation → Parquet → SQLite → Excel relu depuis SQL.

La publication sur Hugging Face est reportée à ta demande. Aucun envoi vers ce Hub
n'est effectué et aucun token Hugging Face n'est nécessaire.

Source : [RTE / ODRÉ, éCO2mix national temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/).
Les puissances sont exprimées en MW. Les données temps réel sont provisoires.
Réutilisation sous Licence Ouverte 2.0 avec attribution à RTE.

## Installation

Python 3.11 ou plus récent.

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

## Exécuter le pipeline

Depuis la racine du dépôt, récupérer les sept derniers jours disponibles :

```powershell
.venv\Scripts\python app.py run --days 7
```

Sans argument, `python app.py` lance également ces sept jours. Le pipeline découvre
la dernière date contenant une consommation mesurée ; il ne confond pas les dates
futures de prévision avec les dernières mesures. Le dernier jour peut être incomplet.
L'API est publique et ne nécessite pas de clé RTE.

Choisir une période (début inclus, fin **exclue**, dates en Europe/Paris) :

```powershell
.venv\Scripts\python app.py run --start 2026-10-28 --end 2026-11-04
```

Chaque exécution affiche les chemins des fichiers créés. Elle utilise un nouvel
identifiant d'archive et ne modifie jamais une archive brute existante.

```text
PipelineSQL/
├── app.py
├── sql/
│   ├── 001_schema.sql
│   └── analyses.sql
├── scripts/
│   ├── ingest/rte.py
│   ├── transform/clean.py
│   ├── load/sqlite.py
│   └── pipeline.py
├── data/
│   ├── raw/<execution>/
│   │   ├── AAAA-MM-JJ_00000.json
│   │   ├── brut.xlsx
│   │   └── manifest.json
│   ├── processed/<execution>/
│   │   ├── consommation.parquet
│   │   ├── consommation_sql.xlsx
│   │   ├── quality.json
│   │   └── load_report.json
│   └── electricity.sqlite
├── tests/
└── README.md
```

### Lancer les étapes séparément

```powershell
.venv\Scripts\python -m scripts.pipeline ingest --days 7
# Remplacer ID par l'identifiant affiché par la collecte.
.venv\Scripts\python -m scripts.pipeline transform --raw data/raw/ID
.venv\Scripts\python -m scripts.pipeline load --processed data/processed/ID
```

On peut relancer `load` sur le même dossier : les clés SQL empêchent les doublons
et l'Excel final est régénéré depuis SQL. Pour rejouer une transformation, effectuer
une nouvelle collecte ; un dossier transformé existant est protégé contre l'écrasement.
Une archive dont le manifeste indique `failed` ne peut pas être transformée.

### Configuration

| Variable | Valeur par défaut | Rôle |
| --- | --- | --- |
| `RTE_API_BASE` | `https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets` | API ODRÉ |
| `RTE_DATASET` | `eco2mix-national-tr` | Jeu national temps réel |
| `SQLITE_PATH` | `data/electricity.sqlite` | Fichier de base SQL |

Un chemin SQLite relatif dans `.env` est résolu depuis la racine du dépôt.
L'option `--database chemin.sqlite` permet de choisir une autre base pour `run` ou
`load` ; ce chemin est relatif au dossier de lancement. SQLite ne demande ni serveur
ni mot de passe. Les autres moteurs SQL ne sont pas implémentés.

## Collecte et transformation

La collecte utilise `httpx.AsyncClient`, avec au plus quatre requêtes simultanées,
des fenêtres journalières et une pagination de 100 lignes. Les erreurs réseau, HTTP
408, 429, 500, 502, 503 et 504 sont réessayées jusqu'à trois fois avec délai croissant.
Les autres erreurs HTTP arrêtent l'étape. Chaque période couvre au maximum 366 jours,
sous réserve de la disponibilité dans le jeu temps réel.

Le JSON conserve le corps de réponse reçu. `brut.xlsx` contient les champs de tous
les enregistrements retournés, sans nettoyage métier. Le JSON constitue la référence
pour les valeurs et types d'origine, Excel restant un format de consultation.
Le manifeste conserve l'URL source, la période, la date de collecte, les paramètres,
le nombre de lignes et les empreintes SHA-256 des pages. Ces empreintes sont vérifiées
avant transformation. Les fichiers bruts ne sont jamais réécrits par le pipeline.

La transformation :

- conserve uniquement les consommations mesurées du périmètre France ; les lignes
  de prévision sans consommation restent dans les données brutes ;
- valide les dates avec fuseau, les valeurs numériques finies et la consommation
  positive ou nulle ; une mesure invalide arrête le traitement avant SQL ;
- conserve les valeurs manquantes sous forme de valeurs nulles ;
- garde les échanges signés et le pompage tels que fournis ;
- déduplique les lignes identiques et rejette les mesures contradictoires au même instant ;
- calcule la production totale à partir des huit filières principales, sans ajouter
  de nouveau leurs sous-filières ; si une filière manque, le total reste nul ;
- écrit les résultats en Parquet et les compteurs dans `quality.json`.

Le chargement SQL s'effectue dans une transaction. La clé primaire est
`(date_heure_utc, perimetre)` ; les corrections RTE mettent les lignes à jour sans
doublon. Une archive récupérée plus tôt ne peut pas remplacer une révision récupérée
plus récemment. `pipeline_runs` conserve les chargements réussis et leur origine.
L'Excel `consommation_sql.xlsx` est produit **après chargement et relecture en base**,
sur les instants concernés par cette exécution. La base conserve aussi les mesures
des exécutions précédentes. Si l'export Excel échoue après la transaction SQL,
relancer l'étape `load` régénère l'export sans créer de doublons.

Les requêtes de consultation sont dans [sql/analyses.sql](sql/analyses.sql).

## Dataset Card

| Propriété | Description |
| --- | --- |
| Producteur | RTE, distribution via Open Data Réseaux Énergies |
| Source | `eco2mix-national-tr`, API Explore v2.1 |
| Périmètre | France, données nationales |
| Fréquence source | Quart d'heure pour la consommation et les filières principales |
| Licence | Licence Ouverte 2.0 / Etalab ; citer RTE et la date de collecte |
| Nature | Télémesures, forfaits et estimations provisoires, susceptibles de correction |
| Usage | Analyse descriptive de la consommation et du mix électrique |
| Format nettoyé | Parquet, une ligne par instant UTC et périmètre |

### Dictionnaire des colonnes nettoyées

| Colonne | Type | Description |
| --- | --- | --- |
| `date_heure_utc` | texte ISO 8601 avec `+00:00` | Instant de mesure, clé temporelle |
| `date_heure_paris` | texte ISO 8601 avec décalage | Heure française, changement d'heure pris en compte |
| `perimetre` | texte | `France` |
| `nature` | texte | Nature fournie par RTE |
| `consommation_mw` | réel obligatoire | Puissance électrique consommée en MW |
| `prevision_j1_mw`, `prevision_j_mw` | réels nullable | Prévisions de consommation |
| `fioul_mw`, `charbon_mw`, `gaz_mw`, `nucleaire_mw` | réels nullable | Puissances des filières thermiques |
| `eolien_mw`, `solaire_mw`, `hydraulique_mw`, `bioenergies_mw` | réels nullable | Puissances des autres filières |
| `pompage_mw` | réel nullable | Valeur signée fournie par RTE |
| `echanges_physiques_mw` | réel nullable | Échanges physiques, signe source conservé |
| `taux_co2_g_kwh` | réel nullable | Intensité carbone en gCO₂/kWh |
| `production_totale_mw` | réel nullable | Somme des huit filières principales |
| `source_dataset` | texte | Identifiant du jeu source |

La table SQL ajoute `source_retrieved_at` et `run_id` pour tracer la provenance.
Les MW mesurent une **puissance**, pas une énergie en MWh : le pipeline ne calcule
pas d'énergie en intégrant les mesures. Il ne comble pas les trous et n'interpole
aucune mesure. Les sous-filières détaillées restent accessibles dans l'Excel et les
JSON bruts. L'archive conserve les champs sources supplémentaires sans les ajouter
automatiquement au schéma SQL.

Le jeu temps réel couvre plusieurs mois et n'est pas une archive historique complète.
Pour des analyses historiques, consulter le [jeu consolidé et définitif RTE](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-cons-def/).
Le portail annonce un rafraîchissement toutes les quinze minutes et un quota de
50 000 appels API par utilisateur et par mois : éviter des relances excessives.

## Vérifications

```powershell
.venv\Scripts\python -m pytest -q
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

Les 23 tests utilisent une API simulée et des bases temporaires : pagination, reprises,
échecs de collecte, changement d'heure, validation, déduplication, upsert, rollback,
export SQL et intégrité des archives sur toute la chaîne. Une CI GitHub Actions
exécute ces contrôles sous Windows et Linux ; aucun appel RTE réel dans les tests.

Première exécution réelle vérifiée le **3 novembre 2026**, sur la période française
du **28 octobre au 3 novembre inclus** : 672 lignes brutes, 54 lignes sans consommation
mesurée, 618 mesures chargées dans SQLite et relues dans l'Excel final.

Les exports, la base locale et les secrets sont exclus de Git. Ils restent disponibles
dans le dossier `data/` de cette machine et sont régénérés par la commande `run`.
Le prototype `app.py` d'origine est conservé dans le premier commit `e8f8e17`.
