import json

import httpx
import pytest

from infrastructure.container_runtime import (
    ContainerRuntime,
    ContainerRuntimeError,
    discover,
)


@pytest.mark.parametrize(
    ("version", "info", "podman", "rootless"),
    [
        ({"Components": [{"Name": "Engine"}]}, {"SecurityOptions": []}, False, False),
        (
            {"Components": [{"Name": "Engine"}]},
            {"SecurityOptions": ["name=rootless"]},
            False,
            True,
        ),
        (
            {"Components": [{"Name": "Podman Engine"}]},
            {"host": {"security": {"rootless": True}}},
            True,
            True,
        ),
    ],
)
async def test_identifies_runtime(version, info, podman, rootless):
    def respond(request):
        if request.url.path == "/_ping":
            return httpx.Response(200, text="OK")
        return httpx.Response(200, json=version if request.url.path == "/version" else info)

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(respond))
    try:
        assert await runtime.ping()
        await runtime.identify()
        assert runtime.is_podman is podman
        assert runtime.rootless is rootless
    finally:
        await runtime.close()


async def test_container_and_volume_requests_are_scoped_and_encoded():
    requests = []

    def respond(request):
        requests.append(request)
        if request.url.path == "/containers/create":
            return httpx.Response(201, json={"Id": "container-1"})
        if request.url.path == "/containers/json":
            return httpx.Response(200, json=[{"Id": "container-1"}])
        if request.url.path == "/volumes/create":
            return httpx.Response(201, json={"Name": "home-1"})
        if request.url.path.endswith("/wait"):
            return httpx.Response(200, json={"StatusCode": 0})
        return httpx.Response(200, json={"Id": "container-1"})

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(respond))
    try:
        assert await runtime.create_container("project-1", {"Image": "ubuntu:24.04"}) == "container-1"
        await runtime.start("container-1")
        await runtime.stop("container-1", timeout=5)
        assert (await runtime.inspect("container-1"))["Id"] == "container-1"
        assert (await runtime.wait("container-1"))["StatusCode"] == 0
        assert await runtime.list_containers({"knoggin.owner": "alice"}) == [
            {"Id": "container-1"}
        ]
        await runtime.remove("container-1")
        assert (await runtime.create_volume("home-1", {"knoggin.owner": "alice"}))["Name"] == "home-1"
        await runtime.remove_volume("home-1")
    finally:
        await runtime.close()

    assert [(r.method, r.url.path) for r in requests] == [
        ("POST", "/containers/create"),
        ("POST", "/containers/container-1/start"),
        ("POST", "/containers/container-1/stop"),
        ("GET", "/containers/container-1/json"),
        ("POST", "/containers/container-1/wait"),
        ("GET", "/containers/json"),
        ("DELETE", "/containers/container-1"),
        ("POST", "/volumes/create"),
        ("DELETE", "/volumes/home-1"),
    ]
    assert requests[0].url.params["name"] == "project-1"
    assert json.loads(requests[0].content) == {"Image": "ubuntu:24.04"}
    assert requests[2].url.params["t"] == "5"
    assert json.loads(requests[5].url.params["filters"]) == {
        "label": ["knoggin.owner=alice"]
    }
    assert requests[6].url.params["force"] == "true"
    assert json.loads(requests[7].content) == {
        "Name": "home-1", "Labels": {"knoggin.owner": "alice"}
    }


async def test_image_lookup_pull_and_api_errors():
    requests = []

    def respond(request):
        requests.append(request)
        if request.method == "GET" and request.url.path.startswith("/images/"):
            return httpx.Response(404, json={"message": "No such image"})
        if request.url.path == "/images/create":
            return httpx.Response(200, text='{"status":"Pulling"}\n{"error":"denied"}\n')
        return httpx.Response(409, json={"message": "name conflict"})

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(respond))
    try:
        assert await runtime.image_exists("ubuntu:24.04") is False
        with pytest.raises(ContainerRuntimeError, match="denied"):
            await runtime.pull("ubuntu:24.04")
        with pytest.raises(ContainerRuntimeError, match="name conflict") as error:
            await runtime.create_container("existing", {"Image": "ubuntu:24.04"})
        assert error.value.status_code == 409
    finally:
        await runtime.close()
    assert requests[1].url.params["fromImage"] == "ubuntu:24.04"


async def test_transport_error_has_no_http_status():
    def fail(request):
        raise httpx.ConnectError("socket unavailable", request=request)

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(fail))
    try:
        with pytest.raises(ContainerRuntimeError, match="socket unavailable") as error:
            await runtime.ping()
        assert error.value.status_code is None
    finally:
        await runtime.close()


async def test_discovery_tries_sockets_in_order(monkeypatch):
    attempted = []

    def make_transport(*, uds):
        attempted.append(uds)

        def respond(request):
            if uds.endswith("podman.sock"):
                raise httpx.ConnectError("unavailable", request=request)
            if request.url.path == "/_ping":
                return httpx.Response(200, text="OK")
            if request.url.path == "/version":
                return httpx.Response(200, json={"Components": [{"Name": "Engine"}]})
            return httpx.Response(200, json={"SecurityOptions": []})

        return httpx.MockTransport(respond)

    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/123")
    monkeypatch.setattr("infrastructure.container_runtime.httpx.AsyncHTTPTransport", make_transport)
    runtime = await discover()
    assert runtime is not None
    try:
        assert runtime.socket_path == "/run/user/123/docker.sock"
        assert attempted == [
            "/run/user/123/podman/podman.sock",
            "/run/user/123/docker.sock",
        ]
    finally:
        await runtime.close()
