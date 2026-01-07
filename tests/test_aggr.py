#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import csv
import pytest

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

def test(context, tmp_path):
    w = context.Channel(f'lvcache://;message.First=f0,f1,f2;message.Second=header.h0', name='writer', dir='w', scheme=SCHEME, dump='yes', filename=tmp_path / 'file.dat')
    w.open()
    reader = Accum('lvcache://', name='reader', master=w, dump='yes', context=context)
    f = Accum(f'file://{tmp_path / "file.dat"}', name='file', autoclose='no', context=context)
    for i in range(10):
        for k in range(3):
            w.post({'f0': k, 'f1': 'k0', 'f2': 'b0', 'data': f'f-{i}-{k}'}, name='First', seq=100 * i + 10 * k)
            w.post({'header': {'h0': k}, 'data': [f's-{i}-{k}']}, name='Second', seq=100 * i + 10 * k + 1)
            w.post({'f0': k, 'data': f't-{i}-{k}'}, name='Third', seq=100 * i + 10 * k + 2)
        w.post({}, name='Block', type=w.Type.Control)
        f.result = []
        f.open()
        for _ in range(10):
            f.process()
        r = [f.unpack(m) for m in f.result if m.type == f.Type.Data]
        f.close()
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
    r = [reader.unpack(m) for m in reader.result if m.type == f.Type.Data]
    reader.close()
    assert sorted([m.as_dict() for m in r], key=lambda x: x['f0']) == compare['First']
