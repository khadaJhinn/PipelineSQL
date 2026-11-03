-- Mesures de puissance : MW ; ne pas les présenter comme une énergie en MWh.
SELECT date_heure_paris, consommation_mw, production_totale_mw
FROM electricity_consumption
ORDER BY date_heure_utc DESC
LIMIT 96;

-- Statistiques des mesures disponibles par date française (jours parfois incomplets).
SELECT
    substr(date_heure_paris, 1, 10) AS date_france,
    COUNT(*) AS mesures_disponibles,
    ROUND(AVG(consommation_mw), 2) AS consommation_moyenne_mw,
    MIN(consommation_mw) AS consommation_min_mw,
    MAX(consommation_mw) AS consommation_max_mw
FROM electricity_consumption
GROUP BY date_france
ORDER BY date_france;
