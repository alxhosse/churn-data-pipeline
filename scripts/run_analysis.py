#!/usr/bin/env python
"""
Standalone script to run analysis
Can be run directly without package installation
"""
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from run_analysis import main

if __name__ == "__main__":
    main()

