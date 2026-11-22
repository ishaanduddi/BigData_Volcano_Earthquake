from __future__ import annotations

import pytest
import requests


@pytest.mark.network
def test_usgs_count_and_csv_consistency():
    base = "https://earthquake.usgs.gov/fdsnws/event/1/count"
    params = {"starttime": "2024-01-01", "endtime": "2024-01-02", "minmagnitude": 2.5}
    r = requests.get(base, params=params, timeout=60)
    assert r.status_code == 200
    count = int(r.text.strip())
    assert count >= 0
    q = requests.get("https://earthquake.usgs.gov/fdsnws/event/1/query",
                     params={**params, "format": "csv", "limit": 20000}, timeout=60)
    assert q.status_code == 200
    rows = max(0, len(q.text.strip().splitlines()) - 1)
    assert rows == count
