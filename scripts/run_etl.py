
import sys
import argparse
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.pipeline import IPLDataPipeline

def main():
    parser = argparse.ArgumentParser(description='Run IPL Data Warehouse ETL')
    parser.add_argument('csv_file', help='Path to IPL CSV file')
    parser.add_argument('--skip-dimensions', action='store_true', 
                       help='Skip loading dimensions')
    parser.add_argument('--skip-facts', action='store_true',
                       help='Skip loading facts')
    parser.add_argument('--skip-marts', action='store_true',
                       help='Skip refreshing marts')
    
    args = parser.parse_args()
    

    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)
    

    pipeline = IPLDataPipeline(str(csv_path))
    success = pipeline.run(
        load_dimensions=not args.skip_dimensions,
        load_facts=not args.skip_facts,
        refresh_marts=not args.skip_marts
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()