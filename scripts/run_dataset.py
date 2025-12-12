#!/usr/bin/env python
"""
Standalone script to run milestone 3: create dataset and statistics
"""
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from run_dataset import main

if __name__ == "__main__":
    main()

