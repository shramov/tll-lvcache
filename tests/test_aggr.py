#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

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

class FileReader:
    def __init__(self, context, fn):
        self.channel = Accum(f'file://{fn}', name='file', dump='frame', context=context)

    def read(self):
        try:
            self.channel.result = []
            self.channel.open()
            scheme = self.channel.scheme
            for _ in range(10):
                self.channel.process()
            return [scheme.unpack(m) for m in self.channel.result if m.type == m.Type.Data]
        finally:
            self.channel.close()

def test(context, tmp_path):
    w = context.Channel(f'lvcache://;message.First.key=f0,f1,f2;message.Second=header.h0', name='writer', dir='w', scheme=SCHEME, dump='yes', filename=tmp_path / 'file.dat')
    w.open()
    reader = Accum('lvcache://', name='reader', master=w, dump='yes', context=context)
    f = FileReader(context, tmp_path / "file.dat")
    for i in range(10):
        for k in range(3):
            w.post({'f0': k, 'f1': 'k0', 'f2': 'b0', 'data': f'f-{i}-{k}'}, name='First', seq=100 * i + 10 * k)
            w.post({'header': {'h0': k}, 'data': [f's-{i}-{k}']}, name='Second', seq=100 * i + 10 * k + 1)
            w.post({'f0': k, 'data': f't-{i}-{k}'}, name='Third', seq=100 * i + 10 * k + 2)
        w.post({}, name='Block', type=w.Type.Control)
        r = f.read()
        compare = {
            'First': [{'f0': k, 'f1': 'k0', 'f2': b'b0\0\0', 'data': f'f-{i}-{k}'} for k in range(3)],
            'Second': [{'header': {'h0': k}, 'f0': 0, 'data': [f's-{i}-{k}']} for k in range(3)],
        }
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'First'], key=lambda x: x['f0']) == compare['First']
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'Second'], key=lambda x: x['header']['h0']) == compare['Second']
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'Third'], key=lambda x: x['f0']) == []
        for _ in range(2):
            reader.result = []
            reader.open()
            for _ in range(10):
                reader.process()
            reader.close()
    reader.close()
    reader.result = []
    reader.open(filter='First')
    for _ in range(10):
        reader.process()
    r = [reader.unpack(m) for m in reader.result if m.type == m.Type.Data]
    reader.close()
    assert sorted([m.as_dict() for m in r], key=lambda x: x['f0']) == compare['First']

def test_remove(context, tmp_path):
    scheme = '''yamls://
- enums:
    Action: {type: int8, enum: {Update: 0, Add: 1, Remove: 2}}
- name: Header
  fields:
    - { name: h0, type: int64 }
    - { name: action, type: Action }
- name: First
  id: 10
  fields:
    - { name: header, type: Header }
    - { name: f0, type: int32 }
- name: Second
  id: 20
  fields:
    - { name: header, type: Header }
    - { name: f0, type: double }
'''

    cfg = Config.load_data('yamls', f'''
tll.proto: lvcache
message.First:
  key: f0
  erase-key: header.action
  erase-value: Remove
message.Second:
  key: header.h0
  erase-key: header.action
  erase-value: Remove
''')
    w = context.Channel(cfg, name='writer', dir='w', scheme=scheme, dump='yes', filename=tmp_path / 'file.dat')
    reader = Accum('lvcache://', name='reader', master=w, dump='yes', context=context)
    f = FileReader(context, tmp_path / "file.dat")

    w.open()
    Action = w.scheme.enums['Action'].klass

    for i in range(2):
        for k in range(3):
            w.post({'header': {'action': 'Update', 'h0': i}, 'f0': k}, name='First', seq=100 * i + 10 * k)
            w.post({'header': {'action': 'Update', 'h0': k}, 'f0': i + 0.5}, name='Second', seq=100 * i + 10 * k + 1)

        w.post({}, name='Block', type=w.Type.Control)

        r = f.read()
        compare = {
            'First': [{'header': {'action': Action.Update, 'h0': i}, 'f0': k} for k in range(3)],
            'Second': [{'header': {'action': Action.Update, 'h0': k}, 'f0': i + 0.5} for k in range(3)],
        }
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'First'], key=lambda x: x['f0']) == compare['First']
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'Second'], key=lambda x: x['header']['h0']) == compare['Second']
        reader.result = []
        reader.open(filter='First')
        for _ in range(10):
            reader.process()
        r = [reader.unpack(m) for m in reader.result if m.type == m.Type.Data]
        reader.close()
        assert sorted([m.as_dict() for m in r], key=lambda x: x['f0']) == compare['First']

        k = 1
        w.post({'header': {'action': 'Remove', 'h0': i}, 'f0': k}, name='First', seq=1000 + 100 * i + 10 * k)
        w.post({'header': {'action': 'Remove', 'h0': k}, 'f0': i + 0.5}, name='Second', seq=1000 + 100 * i + 10 * k + 1)

        reader.result = []
        reader.open()
        for _ in range(10):
            reader.process()
        r = [reader.unpack(m) for m in reader.result if m.type == m.Type.Data]
        reader.close()
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'First'], key=lambda x: x['f0']) == [m for m in compare['First'] if m['f0'] != k]
        assert sorted([m.as_dict() for m in r if m.SCHEME.name == 'Second'], key=lambda x: x['header']['h0']) == [m for m in compare['Second'] if m['header']['h0'] != k]
