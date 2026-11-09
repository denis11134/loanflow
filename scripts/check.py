#!/usr/bin/env python3
"""Единая команда: обычный запуск остаётся красным при известных дефектах контрактов."""
from pathlib import Path
import argparse
import os
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--technical', action='store_true', help='Только технические проверки; не вердикт готовности')
args = parser.parse_args()
os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH', str(root / '.cache/ms-playwright'))
command = [sys.executable, '-m', 'pytest', '-q', '--tb=short', '--junitxml=test-results/checks.xml']
if args.technical:
    command += ['-m', 'not contracts']
else:
    command += ['--require-ready']
raise SystemExit(subprocess.call(command, cwd=root))
