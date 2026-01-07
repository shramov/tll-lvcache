#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import decorator
import json
import pytest

from tll.channel.mock import Mock
from tll.config import Config
from tll.error import TLLError
from tll.test_util import Accum

SCHEME = '''yamls://
- name: Header
  fields:
    - { name: h0, type: int64 }
- name: First
  id: 10
  fields:
    - { name: f0, type: int32 }
    - { name: f1, type: byte8, options.type: string }
    - { name: f2, type: byte4 }
    - { name: data, type: string }
- name: Second
  id: 20
  fields:
    - { name: f0, type: int64 }
    - { name: header, type: Header }
    - { name: data, type: '*string' }
- name: Third
  id: 30
  fields:
    - { name: f0, type: int32 }
    - { name: data, type: string }
'''

SCHEME_CONTROL = '''yamls://
- name: Header
  fields:
    - { name: header, type: string }
    - { name: value, type: string }

- name: Connect
  id: 1
  fields:
    - { name: method, type: int8 }
    - { name: code, type: int16 }
    - { name: size, type: int64 }
    - { name: path, type: string }
    - { name: headers, type: '*Header' }

- name: Disconnect
  id: 2
  fields:
    - { name: code, type: int16 }
    - { name: error, type: string }
'''

@decorator.decorator
def asyncloop_run(f, asyncloop, *a, **kw):
    asyncloop.run(f(asyncloop, *a, **kw))

@asyncloop_run
async def test(asyncloop, tmp_path):
    cfg = Config.load(f'''yamls://
mock:
  input:
    url: direct://
  output:
    url: direct://
channel:
  tll.proto: lvrest
  tll.channel:
    input: input
    output: output
  message:
    First: f0,f1,f2
    Second: header.h0
  filter:
    First: First.f0
    Second: Second.header.h0
  name: mock
  filename: {tmp_path / "file.dat"}
''')
    cfg['channel.scheme'] = SCHEME
    cfg['mock.output.scheme-control'] = SCHEME_CONTROL
    cfg['mock.input.scheme'] = SCHEME
    mock = Mock(asyncloop, cfg)
    mock.open()

    r, w = mock.io('output', 'input')

    for i in range(10):
        for k in range(3):
            w.post({'f0': k, 'f1': 'k0', 'f2': 'b0', 'data': f'f-{i}-{k}'}, name='First', seq=100 * i + 10 * k)
            w.post({'header': {'h0': k}, 'data': [f's-{i}-{k}']}, name='Second', seq=100 * i + 10 * k + 1)
            w.post({'f0': k, 'data': f't-{i}-{k}'}, name='Third', seq=100 * i + 10 * k + 2)

        assert mock.channel.dcaps == mock.channel.DCaps.Zero
        r.post({}, name='Connect', type=w.Type.Control, addr=10)
        assert mock.channel.dcaps == mock.channel.DCaps.Pending | mock.channel.DCaps.Process

        data = b''
        for _ in range(30):
            if data.endswith(b']'): break
            m = await r.recv(0.001)
            if m.type == m.Type.Data:
                data += m.data.tobytes()

        compare = [{'header': {'h0': k}, 'f0': 0, 'data': [f's-{i}-{k}']} for k in range(3)] + \
                [{'f0': k, 'f1': 'k0', 'f2': 'YjAAAA==', 'data': f'f-{i}-{k}'} for k in range(3)];

        assert sorted(json.loads(data), key=lambda x: str(x.get('data'))) == compare

    r.post({'path': '/path/?Second=2'}, name='Connect', type=w.Type.Control, addr=10)
    data = b''
    for _ in range(30):
        if data.endswith(b']'): break
        m = await r.recv(0.001)
        if m.type == m.Type.Data:
            data += m.data.tobytes()

    compare = [{'header': {'h0': k}, 'f0': 0, 'data': [f's-{i}-{k}']} for k in range(3)] + \
            [{'f0': k, 'f1': 'k0', 'f2': 'YjAAAA==', 'data': f'f-{i}-{k}'} for k in range(3)]

    assert sorted(json.loads(data), key=lambda x: str(x.get('data'))) == [{'header': {'h0': k}, 'f0': 0, 'data': [f's-{i}-{k}']} for k in [2]]
