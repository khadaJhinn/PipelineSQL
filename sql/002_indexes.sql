-- Recherche sélective des instants où une puissance précise a été observée.
-- scripts/analyze.py mesure le coût avant/après et sauvegarde EXPLAIN ANALYZE.
CREATE INDEX IF NOT EXISTS idx_consommation_mw
ON electricity_consumption(consommation_mw);
