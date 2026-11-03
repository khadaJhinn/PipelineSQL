PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    source_archive TEXT NOT NULL,
    source_retrieved_at TEXT NOT NULL,
    raw_rows INTEGER NOT NULL CHECK (raw_rows >= 0),
    clean_rows INTEGER NOT NULL CHECK (clean_rows > 0),
    loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS electricity_consumption (
    date_heure_utc TEXT NOT NULL,
    date_heure_paris TEXT NOT NULL,
    perimetre TEXT NOT NULL CHECK (perimetre = 'France'),
    nature TEXT NOT NULL,
    consommation_mw REAL NOT NULL CHECK (
        typeof(consommation_mw) IN ('real', 'integer') AND consommation_mw >= 0
    ),
    prevision_j1_mw REAL,
    prevision_j_mw REAL,
    fioul_mw REAL,
    charbon_mw REAL,
    gaz_mw REAL,
    nucleaire_mw REAL,
    eolien_mw REAL,
    solaire_mw REAL,
    hydraulique_mw REAL,
    bioenergies_mw REAL,
    pompage_mw REAL,
    echanges_physiques_mw REAL,
    taux_co2_g_kwh REAL,
    production_totale_mw REAL,
    source_dataset TEXT NOT NULL,
    source_retrieved_at TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    PRIMARY KEY (date_heure_utc, perimetre)
);
