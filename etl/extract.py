
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataExtractor:
    
    def __init__(self, csv_path):
        self.csv_path = Path(csv_path)
        
    def extract(self):

        logger.info(f"Extracting data from {self.csv_path}")
        
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
        
        # Read CSV with proper dtypes
        dtype_dict = {
            'match_id': 'int32',
            'innings': 'int8',
            'over': 'int8',
            'ball': 'int8',
            'ball_no': 'int16',
            'runs_batter': 'int8',
            'runs_extras': 'int8',
            'runs_total': 'int8',
            'valid_ball': 'bool',
            'team_runs': 'int16',
            'team_balls': 'int16',
            'team_wicket': 'int8'
        }
        
        df = pd.read_csv(
            self.csv_path,
            parse_dates=['date'],
            low_memory=False
        )
        
        logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")
        
        self._validate_data(df)
        
        return df
    
    def _validate_data(self, df):
        logger.info("Validating extracted data...")
        
        required_columns = [
            'match_id', 'date', 'innings', 'batter', 'bowler',
            'batting_team', 'bowling_team', 'runs_total', 'venue'
        ]
        
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values found:\n{null_counts[null_counts > 0]}")
        
        logger.info("Data validation completed")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extractor = DataExtractor("data/raw/IPL.csv")
    df = extractor.extract()
    print(df.head())
    df.info()