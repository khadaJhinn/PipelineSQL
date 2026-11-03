import pytest

from scripts.transform.clean import PRODUCTION, transform_records


def measurement(**overrides):
    return {
        "perimetre": "France",
        "date_heure": "2026-10-25T00:30:00+00:00",
        "consommation": 42000,
        "nucleaire": 30000,
        "fioul": 10,
        "charbon": 20,
        "gaz": 1000,
        "eolien": 5000,
        "solaire": 0,
        "hydraulique": 4000,
        "bioenergies": 300,
        "gaz_ccg": 1000,
        "hydraulique_lacs": 2000,
        "ech_physiques": -1200,
        **overrides,
    }


def test_mw_totals_nulls_forecasts_and_duplicates():
    row = measurement()
    frame, report = transform_records(
        [row, row, measurement(consommation=None)], "eco2mix-national-tr"
    )
    assert report == {"raw_rows": 3, "unmeasured_rows": 1, "duplicate_rows": 1, "clean_rows": 1}
    assert frame.iloc[0]["production_totale_mw"] == sum(frame.iloc[0][PRODUCTION])
    assert frame.iloc[0]["production_totale_mw"] == 40330
    assert frame.iloc[0]["echanges_physiques_mw"] == -1200
    assert frame.iloc[0]["date_heure_paris"] == "2026-10-25T02:30:00+02:00"
    assert frame["taux_co2_g_kwh"].isna().all()


def test_dst_repeated_local_hour_stays_two_distinct_measurements():
    rows = [measurement(), measurement(date_heure="2026-10-25T01:30:00+00:00")]
    frame, _ = transform_records(rows, "test")
    assert len(frame) == 2
    assert frame.iloc[1]["date_heure_paris"] == "2026-10-25T02:30:00+01:00"


@pytest.mark.parametrize(
    "overrides",
    [
        {"consommation": -1},
        {"consommation": "pas un nombre"},
        {"consommation": float("inf")},
        {"consommation": True},
        {"date_heure": "2026-10-25T02:30:00"},
        {"date_heure": "invalide"},
        {"perimetre": "Autre"},
    ],
)
def test_invalid_measures_fail_before_sql(overrides):
    with pytest.raises(ValueError):
        transform_records([measurement(**overrides)], "test")


def test_conflicting_duplicates_are_rejected():
    with pytest.raises(ValueError, match="contradictoires"):
        transform_records([measurement(), measurement(consommation=1)], "test")


def test_missing_production_is_not_zero():
    frame, _ = transform_records([measurement(nucleaire=None)], "test")
    assert frame["production_totale_mw"].isna().all()
