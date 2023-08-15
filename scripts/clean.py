#!/usr/bin/env python
import pathlib, shutil
for p in pathlib.Path('.').rglob('__pycache__'):
    shutil.rmtree(p)
