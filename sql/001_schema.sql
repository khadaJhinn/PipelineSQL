-- Base DuckDB locale. Les archives SQLite antérieures restent intactes.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    source_archive VARCHAR NOT NULL,
    source_retrieved_at TIMESTAMPTZ NOT NULL,
    raw_rows BIGINT NOT NULL CHECK (raw_rows >= 0),
    clean_rows BIGINT NOT NULL CHECK (clean_rows > 0),
    loaded_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS electricity_consumption (
    date_heure_utc VARCHAR NOT NULL CHECK (try_cast(date_heure_utc AS TIMESTAMPTZ) IS NOT NULL),
    date_heure_paris VARCHAR NOT NULL CHECK (try_cast(date_heure_paris AS TIMESTAMPTZ) IS NOT NULL),
    perimetre VARCHAR NOT NULL CHECK (perimetre = 'France'),
    nature VARCHAR NOT NULL,
    consommation_mw DOUBLE NOT NULL CHECK (isfinite(consommation_mw) AND consommation_mw >= 0),
    prevision_j1_mw DOUBLE,
    prevision_j_mw DOUBLE,
    fioul_mw DOUBLE,
    charbon_mw DOUBLE,
    gaz_mw DOUBLE,
    nucleaire_mw DOUBLE,
    eolien_mw DOUBLE,
    solaire_mw DOUBLE,
    hydraulique_mw DOUBLE,
    bioenergies_mw DOUBLE,
    pompage_mw DOUBLE,
    echanges_physiques_mw DOUBLE,
    taux_co2_g_kwh DOUBLE,
    production_totale_mw DOUBLE,
    source_dataset VARCHAR NOT NULL,
    source_retrieved_at TIMESTAMPTZ NOT NULL,
    run_id VARCHAR NOT NULL REFERENCES pipeline_runs(run_id),
    PRIMARY KEY (date_heure_utc, perimetre)
);
