
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.load import DataLoader
import logging

logging.basicConfig(level=logging.INFO)

def main():
    loader = DataLoader()
    loader.refresh_marts()

if __name__ == "__main__":
    main()