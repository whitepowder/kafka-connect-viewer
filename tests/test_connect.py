import pytest

from app.connect import ConnectError, normalize_connector_names, stringify_config, validate_connector_name


def test_connector_names_are_sorted_and_deduplicated():
    assert normalize_connector_names(["zeta", "alpha", "alpha"]) == ["alpha", "zeta"]


def test_expanded_connector_payload_keeps_names_only():
    assert normalize_connector_names({"b": {"status": {}}, "a": {"info": {}}}) == ["a", "b"]


def test_invalid_connector_name_is_rejected():
    with pytest.raises(ConnectError) as raised:
        validate_connector_name("bad/name")
    assert raised.value.status == 400
    assert raised.value.body == {"code": "invalid_connector_name", "params": {}}


def test_config_values_are_stringified():
    assert stringify_config({"tasks.max": 2, "enabled": True}) == {
        "tasks.max": "2",
        "enabled": "true",
    }

import httpx

from app.connect import ConnectClient
from app.settings import Cluster


@pytest.mark.anyio
async def test_masked_secret_is_never_written_back():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/connectors/demo":
            return httpx.Response(200, json={"config": {"connector.class": "Demo", "password": "*****"}})
        pytest.fail(f"unexpected upstream write: {request.method} {request.url}")

    client = ConnectClient(
        Cluster(id="test", name="test", url="http://connect:8083"),
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(ConnectError) as raised:
            await client.update_config("demo", {"connector.class": "Demo", "password": "*****"})
        assert raised.value.status == 400
        assert raised.value.body == {"code": "secret_masked", "params": {"key": "password"}}
    finally:
        await client.aclose()
