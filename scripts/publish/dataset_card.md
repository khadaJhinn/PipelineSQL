---
language:
- fr
license: etalab-2.0
pretty_name: Consommation électrique en France — RTE
tags:
- energy
- electricity
- rte
configs:
- config_name: default
  data_files:
  - split: train
    path: data/consommation.parquet
---

# Consommation électrique en France — RTE

Dataset de mesures nationales de consommation et de production électrique.
Producteur : **RTE**. Distribution par Open Data Réseaux Énergies (ODRÉ).

## Source et licence

[Jeu source éCO2mix national temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/).
API utilisée : $source_url

Les données sources sont sous [Licence Ouverte 2.0 / Etalab](https://www.etalab.gouv.fr/licence-ouverte-open-licence/).
Attribution : **RTE / ODRÉ, éCO2mix national temps réel, collecte du $retrieved_at**.
Le nettoyage ne change pas la licence des données sources.

## Contenu et version

- Version prévue pour le dépôt manuel : **v1.0.0**.
- Archive source : `$run_id`.
- Lignes brutes collectées : **$raw_rows**.
- Lignes sans consommation mesurée écartées : **$unmeasured_rows**.
- Doublons identiques supprimés : **$duplicate_rows**.
- Mesures nettoyées : **$clean_rows**.
- Premier instant mesuré : **$first_utc**.
- Dernier instant mesuré : **$last_utc**.
- Période de collecte française : début `$start` inclus, fin `$end_exclusive` exclue.
- Fichier : `data/consommation.parquet` ; une ligne par instant UTC et périmètre.
- Le split `train` contient tout le dataset ; aucune séparation apprentissage/test n'est définie.

## Collecte et préparation

Collecte HTTP asynchrone avec aiohttp, concurrence limitée, timeouts, pagination et
reprises exponentielles. Les pages sont archivées en JSON Lines et vérifiées par
SHA-256. Les checkpoints permettent de reprendre une collecte interrompue.

Le traitement procède par lots. Il valide le périmètre France, les dates avec fuseau,
les nombres finis et une consommation positive ou nulle. Il conserve les valeurs
manquantes et les signes des échanges. Les prévisions sans consommation mesurée
sont exclues. Les doublons identiques sont supprimés ; les conflits arrêtent le lot.
La production totale additionne les huit filières principales, sans leurs sous-filières.
Si une filière est manquante, le total reste nul.

## Schéma

| Colonne | Type Parquet | Signification |
| --- | --- | --- |
| `date_heure_utc` | string | Instant ISO 8601 en UTC avec décalage explicite |
| `date_heure_paris` | string | Heure française avec décalage explicite |
| `perimetre` | string | France |
| `nature` | string | Nature indiquée par la source |
| `consommation_mw` | double | Puissance consommée, obligatoire |
| `prevision_j1_mw`, `prevision_j_mw` | double nullable | Prévisions |
| `fioul_mw`, `charbon_mw`, `gaz_mw`, `nucleaire_mw` | double nullable | Filières thermiques |
| `eolien_mw`, `solaire_mw`, `hydraulique_mw`, `bioenergies_mw` | double nullable | Autres filières |
| `pompage_mw` | double nullable | Pompage, signe source conservé |
| `echanges_physiques_mw` | double nullable | Échanges, signe source conservé |
| `taux_co2_g_kwh` | double nullable | Intensité carbone en gCO₂/kWh |
| `production_totale_mw` | double nullable | Somme des huit filières principales |
| `source_dataset` | string | Identifiant du jeu RTE source |

## Usages et limites

Destiné à l'analyse descriptive de la consommation et du mix électrique en France.
Les MW mesurent une puissance, pas une énergie en MWh. Aucune intégration d'énergie,
interpolation ou imputation des valeurs manquantes n'est effectuée.

Les données temps réel proviennent de télémesures, forfaits et estimations ; elles
sont provisoires et peuvent être corrigées. Le dernier jour peut être incomplet,
des mesures peuvent manquer et l'historique du jeu temps réel est limité.
Les doublons temporels du changement d'heure restent distincts grâce à l'UTC.
Ce jeu n'est pas un service de prévision ni une archive définitive exhaustive.

## Provenance et reproductibilité

La source est `$source_dataset`. Le fichier `provenance.json` fournit les compteurs
et les empreintes des fichiers publiés. Aucun secret, identifiant de base ou chemin
personnel n'est nécessaire à l'utilisation de ce dataset.

SHA-256 du Parquet : `$parquet_sha256`.

Code du pipeline : [PipelineSQL sur GitHub](https://github.com/khadaJhinn/PipelineSQL).
La préparation est locale ; la publication et le tag sur le Hub sont réalisés manuellement.
