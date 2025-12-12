#!/usr/bin/env python
"""
Standalone script to run all metric calculations and analyses
"""
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from run_metrics import main

if __name__ == "__main__":
    main()

