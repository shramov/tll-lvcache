#!/usr/bin/env python3
# vim: sts=4 sw=4 et

import pytest

import os

import tll.logger
tll.logger.init()

from tll import asynctll
from tll.channel import Context
from tll.config import Config

@pytest.fixture
def context():
    ctx = Context(Config.from_dict({}))
    ctx.load(os.path.join(os.environ.get("BUILD_DIR", "target/debug"), "tll_lvcache"))
    return ctx

@pytest.fixture
def asyncloop(context):
    loop = asynctll.Loop(context)
    yield loop
    loop.destroy()
    loop = None
