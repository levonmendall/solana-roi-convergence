from solana_roi.api import app


def test_helius_webhook_payload_is_required_json_body_not_query_parameter():
    route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == "/v1/ingestion/helius" and "POST" in getattr(route, "methods", set())
    )

    assert [field.name for field in route.dependant.body_params] == ["payload"]
    assert "payload" not in [field.name for field in route.dependant.query_params]

    operation = app.openapi()["paths"]["/v1/ingestion/helius"]["post"]
    assert operation["requestBody"]["required"] is True
