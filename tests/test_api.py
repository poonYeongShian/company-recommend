"""API tests: the real app against real Postgres, with companies placed at known positions.

Each company's description is `angle:<radians>`, which `AngleEmbedder` turns into a unit vector at that angle, so
we know exactly which companies are closest to which (cosine similarity = cos of the angle between them).
"""

import asyncio
import json
import math
from collections.abc import Iterator

import numpy as np
import pytest
from fastapi.testclient import TestClient

from service.api import create_app
from consumer.embedding import DIMENSIONS
from tests.helpers import consume, event, incoming, run

WHEN = "2026-08-14T09:00:00+08:00"


class AngleEmbedder:
    def embed(self, texts: list[str]) -> list[np.ndarray]:
        def vector(text: str) -> np.ndarray:
            angle = float(text.removeprefix("angle:")) if text.startswith("angle:") else 2.0  # fallback texts: far away
            v = np.zeros(DIMENSIONS, dtype=np.float32)
            v[0], v[1] = math.cos(angle), math.sin(angle)
            return v

        return [vector(text) for text in texts]


def company(url: str, reg: str, angle: float | None, **data: str) -> None:
    description = {"natureOfBusiness": f"angle:{angle}"} if angle is not None else {}
    run(consume(url, [event(reg, WHEN, **description, **data)], AngleEmbedder()))


def struck_off(url: str, reg: str) -> None:
    body = {"type": "company-struck-off", "registrationNumber": reg, "updatedAt": "2026-08-14T10:00:00+08:00"}
    run(consume(url, [incoming(json.dumps(body).encode())], AngleEmbedder()))


@pytest.fixture
def api(url: str) -> Iterator[TestClient]:
    # psycopg's async mode needs a selector event loop (not the default on Windows).
    with TestClient(create_app(url), backend_options={"loop_factory": asyncio.SelectorEventLoop}) as client:
        yield client


def similar(api: TestClient, reg: str, **params: int) -> list[str]:
    response = api.get(f"/companies/{reg}/similar", params=params)
    assert response.status_code == 200, response.text
    return [item["registrationNumber"] for item in response.json()["similar"]]


def test_returns_most_similar_first_and_never_the_company_itself(url: str, api: TestClient) -> None:
    for reg, angle in [("SYN-000001", 0.0), ("SYN-000002", 1.0), ("SYN-000003", 0.1), ("SYN-000004", 0.5)]:
        company(url, reg, angle)
    response = api.get("/companies/SYN-000001/similar")
    assert response.status_code == 200
    body = response.json()
    assert body["registrationNumber"] == "SYN-000001"
    assert [item["registrationNumber"] for item in body["similar"]] == ["SYN-000003", "SYN-000004", "SYN-000002"]
    assert [item["similarity"] for item in body["similar"]] == [round(math.cos(a), 4) for a in (0.1, 0.5, 1.0)]


def test_card_fields_and_null_industry_and_state(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    company(url, "SYN-000002", 0.1, companyName="Kopi Maju", industry="Food & Beverage", state="SELANGOR")
    company(url, "SYN-000003", 0.2, companyName="No Details")
    items = api.get("/companies/SYN-000001/similar").json()["similar"]
    assert items[0] == {
        "registrationNumber": "SYN-000002",
        "companyName": "Kopi Maju",
        "industry": "Food & Beverage",
        "state": "Selangor",
        "similarity": round(math.cos(0.1), 4),
    }
    assert items[1]["industry"] is None and items[1]["state"] is None


def test_limit(url: str, api: TestClient) -> None:
    for number in range(1, 8):
        company(url, f"SYN-00000{number}", number / 10)
    assert similar(api, "SYN-000001", limit=2) == ["SYN-000002", "SYN-000003"]
    assert len(similar(api, "SYN-000001")) == 5  # the default


def test_ties_are_broken_by_registration_number(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    for reg in ["SYN-000009", "SYN-000003", "SYN-000005"]:
        company(url, reg, 0.2)  # identical position, so identical similarity
    assert similar(api, "SYN-000001") == ["SYN-000003", "SYN-000005", "SYN-000009"]


def test_struck_off_company_is_not_recommended_but_still_has_a_page(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    company(url, "SYN-000002", 0.1)
    company(url, "SYN-000003", 0.5)
    struck_off(url, "SYN-000002")
    assert similar(api, "SYN-000001") == ["SYN-000003"]
    assert similar(api, "SYN-000002") == ["SYN-000001", "SYN-000003"]


def test_company_without_a_description_is_still_recommendable(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    company(url, "SYN-000002", None, industry="Retail")  # embedded from its name and industry instead
    assert similar(api, "SYN-000001") == ["SYN-000002"]
    assert similar(api, "SYN-000002") == ["SYN-000001"]


def test_known_company_with_nothing_to_compare_gets_an_empty_list(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    response = api.get("/companies/SYN-000001/similar")
    assert response.status_code == 200
    assert response.json() == {"registrationNumber": "SYN-000001", "similar": []}


def test_unknown_company_is_404(url: str, api: TestClient) -> None:
    response = api.get("/companies/SYN-000404/similar")
    assert response.status_code == 404
    assert "SYN-000404" in response.json()["detail"]


def test_struck_off_notice_for_a_company_we_never_saw_is_unknown_and_not_recommended(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    struck_off(url, "SYN-000007")  # placeholder row: no name, no embedding
    assert api.get("/companies/SYN-000007/similar").status_code == 404
    assert similar(api, "SYN-000001") == []


def test_registration_number_is_normalised(url: str, api: TestClient) -> None:
    company(url, "SYN-000001", 0.0)
    assert api.get("/companies/syn-000001/similar").json()["registrationNumber"] == "SYN-000001"


@pytest.mark.parametrize("path", ["/companies/bogus/similar", "/companies/SYN-1/similar", "/companies/SYN-000001/similar?limit=0", "/companies/SYN-000001/similar?limit=51", "/companies/SYN-000001/similar?limit=x"])
def test_bad_requests_are_422(api: TestClient, path: str) -> None:
    assert api.get(path).status_code == 422


def test_health(api: TestClient) -> None:
    assert api.get("/health").json() == {"status": "ok"}


def test_database_outage_is_503_with_retry_after_not_a_crash() -> None:
    unreachable = "postgresql://postgres:postgres@127.0.0.1:1/companies"  # nothing listens on port 1
    with TestClient(create_app(unreachable, pool_timeout=0.3), backend_options={"loop_factory": asyncio.SelectorEventLoop}) as client:
        for path in ["/companies/SYN-000001/similar", "/health"]:
            response = client.get(path)
            assert response.status_code == 503
            assert response.headers["Retry-After"] == "5"
            assert "temporarily unavailable" in response.json()["detail"]
