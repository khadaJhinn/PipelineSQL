-- analyse: dernieres_mesures
-- 1. Dernières mesures disponibles, puissance en MW.
SELECT date_heure_paris, consommation_mw, production_totale_mw
FROM electricity_consumption
ORDER BY date_heure_utc DESC
LIMIT 96;

-- analyse: statistiques_journalieres
-- 2. GROUP BY : statistiques par jour français, y compris les jours incomplets.
SELECT substr(date_heure_paris, 1, 10) AS date_france,
       COUNT(*) AS mesures_disponibles,
       ROUND(AVG(consommation_mw), 2) AS consommation_moyenne_mw,
       MIN(consommation_mw) AS consommation_min_mw,
       MAX(consommation_mw) AS consommation_max_mw
FROM electricity_consumption
GROUP BY date_france
ORDER BY date_france;

-- analyse: evolution_journaliere
-- 3. CTE et LAG : évolution entre journées disponibles, sans inventer les jours absents.
WITH daily AS (
    SELECT substr(date_heure_paris, 1, 10) AS jour,
           AVG(consommation_mw) AS moyenne_mw
    FROM electricity_consumption
    GROUP BY jour
), changes AS (
    SELECT jour, moyenne_mw,
           LAG(jour) OVER (ORDER BY jour) AS jour_precedent_disponible,
           LAG(moyenne_mw) OVER (ORDER BY jour) AS moyenne_precedente_mw
    FROM daily
)
SELECT *, ROUND(moyenne_mw - moyenne_precedente_mw, 2) AS variation_mw
FROM changes
ORDER BY jour;

-- analyse: trois_pointes_par_jour
-- 4. ROW_NUMBER : les trois mesures les plus élevées de chaque jour.
WITH ranked AS (
    SELECT date_heure_utc, date_heure_paris, consommation_mw,
           ROW_NUMBER() OVER (
               PARTITION BY substr(date_heure_paris, 1, 10)
               ORDER BY consommation_mw DESC, date_heure_utc
           ) AS rang
    FROM electricity_consumption
)
SELECT * FROM ranked WHERE rang <= 3
ORDER BY substr(date_heure_paris, 1, 10), rang;

-- analyse: moyenne_quatre_mesures
-- 5. Fenêtre glissante sur quatre mesures disponibles (pas forcément une heure complète).
SELECT date_heure_utc, consommation_mw,
       COUNT(*) OVER w AS mesures_dans_fenetre,
       ROUND(AVG(consommation_mw) OVER w, 2) AS moyenne_quatre_mesures_mw
FROM electricity_consumption
WINDOW w AS (ORDER BY date_heure_utc ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY date_heure_utc;

-- analyse: provenance_des_mesures
-- 6. LEFT JOIN : traçabilité et nombre de mesures actuellement issues de chaque collecte.
SELECT r.run_id, r.source_retrieved_at, r.raw_rows, r.clean_rows,
       COUNT(e.date_heure_utc) AS mesures_actuelles_en_base
FROM pipeline_runs r
LEFT JOIN electricity_consumption e ON e.run_id = r.run_id
GROUP BY r.run_id, r.source_retrieved_at, r.raw_rows, r.clean_rows
ORDER BY r.source_retrieved_at;

-- analyse: mix_journalier
-- 7. CTE et agrégation sur les mêmes instants où le mix est complet.
-- Rapport des puissances moyennes, pas une énergie intégrée en MWh.
WITH complete_mix AS (
    SELECT substr(date_heure_paris, 1, 10) AS jour,
           nucleaire_mw, eolien_mw, solaire_mw, hydraulique_mw, production_totale_mw
    FROM electricity_consumption
    WHERE production_totale_mw > 0
)
SELECT jour, COUNT(*) AS mesures_mix_complet,
       ROUND(100 * AVG(nucleaire_mw) / AVG(production_totale_mw), 2) AS part_nucleaire_pct,
       ROUND(100 * AVG(eolien_mw + solaire_mw + hydraulique_mw)
             / AVG(production_totale_mw), 2) AS part_eolien_solaire_hydraulique_pct
FROM complete_mix
GROUP BY jour
ORDER BY jour;
