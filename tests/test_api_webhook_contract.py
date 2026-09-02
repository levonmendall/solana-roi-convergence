from solana_roi.api import app


def _assert_required_body(path: str) -> None:
    route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == path and "POST" in getattr(route, "methods", set())
    )
    assert [field.name for field in route.dependant.body_params] == ["payload"]
    assert "payload" not in [field.name for field in route.dependant.query_params]
    operation = app.openapi()["paths"][path]["post"]
    assert operation["requestBody"]["required"] is True


def test_helius_webhook_payloads_are_required_json_bodies_not_query_parameters():
    _assert_required_body("/v1/ingestion/helius")
    _assert_required_body("/v1/ingestion/helius/pump-raw")
