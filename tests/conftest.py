"""Serveur HTTP local : tests aiohttp sans appel à RTE et sans mock du transport."""

import inspect
from collections import deque
from dataclasses import replace

import pytest
from aiohttp import web

from scripts.config import Settings


@pytest.fixture
async def api(tmp_path):
    class Api:
        responses = deque()
        requests = []
        handler = None

        def respond(self, payload=None, status=200):
            self.responses.append((payload, status))

    server = Api()

    async def route(request):
        server.requests.append(dict(request.query))
        if server.handler:
            result = server.handler(request)
            return await result if inspect.isawaitable(result) else result
        if not server.responses:
            return web.json_response({"error": "Aucune réponse de test"}, status=500)
        payload, status = server.responses.popleft()
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_get("/{path:.*}", route)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    server.settings = replace(
        Settings(),
        data_dir=tmp_path,
        database=tmp_path / "test.duckdb",
        api_base=f"http://127.0.0.1:{runner.addresses[0][1]}/datasets",
        retries=0,
    )
    try:
        yield server
    finally:
        await runner.cleanup()
