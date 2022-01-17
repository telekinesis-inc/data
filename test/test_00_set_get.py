import asyncio
import pytest

import telekinesis as tk
import telekinesis_data as td

pytestmark = pytest.mark.asyncio
BROKER_PORT = 8800

@pytest.fixture
def event_loop():  # This avoids 'Task was destroyed but it is pending!' message
    yield asyncio.get_event_loop()


async def test_set_get(tmp_path):
    class Registry(dict): pass

    broker = await tk.Broker().serve(port=BROKER_PORT)
    URL = f"ws://localhost:{BROKER_PORT}"
    broker.entrypoint, _ = await tk.create_entrypoint(Registry(), URL)

    u0 = await tk.Entrypoint(URL, str(tmp_path)+'/session_0.pem')
    u0._session.instance_id = 'aaaaAAAA'

    u1 = await tk.Entrypoint(URL, str(tmp_path)+'/session_0.pem')
    u1._session.instance_id = 'BBBBbbbb'

    d0 = td.TelekinesisData(u0._session, str(tmp_path)+'/data_0/').client
    await d0.begin('AAAAaaaaAAAAcccc')

    d1 = td.TelekinesisData(u1._session, str(tmp_path)+'data_1/').client

    await u0.update({'d0': d0})
    td0 = await u1.get('d0')

    await td0.add_peer(d1)

    await d1.set(('x',), 1)

    assert await d0.get(('x',)) == 1
    assert d0._target._registry.get(('AAAAaaaaAAAAcccc', 'x')) == 'AAAABBBBbbbb'

    t, *_ = await d1.list_versions(('x',))

    await d1.set(('x',), 2)

    assert await d1.get(('x',), timestamp=t+0.01) == 1

    assert await d1.get(('x',)) == 2