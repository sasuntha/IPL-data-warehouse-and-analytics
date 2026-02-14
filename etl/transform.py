
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class DataTransformer:
    
    def __init__(self, df):
        self.df = df.copy()
        
    def transform(self):
        logger.info("Starting data transformations...")
        
        self._create_ball_sequence() 
        self._add_calculated_fields()
        self._classify_match_phases()
        self._calculate_pressure_metrics()
        self._add_flags()
        self._clean_data()
        
        logger.info("Transformations completed")
        return self.df
    
    def _add_calculated_fields(self):
        logger.info("Adding calculated fields...")
        
        # Date components
        self.df['day'] = self.df['date'].dt.day
        self.df['month'] = self.df['date'].dt.month
        self.df['year'] = self.df['date'].dt.year
        self.df['day_of_week'] = self.df['date'].dt.day_name()
        self.df['week_of_year'] = self.df['date'].dt.isocalendar().week
        self.df['quarter'] = self.df['date'].dt.quarter
        self.df['is_weekend'] = self.df['date'].dt.dayofweek.isin([5, 6])

        self.df['balls_remaining'] = np.where(
            self.df['innings'] == 2,
            120 - self.df['team_balls'],
            0
        )

        self.df['runs_required'] = np.where(
            self.df['innings'] == 2,
            self.df['runs_target'] - self.df['team_runs'],
            0
        )
            
        self.df['current_run_rate'] = np.where(
            self.df['team_balls'] > 0,
            (self.df['team_runs'] * 6.0) / self.df['team_balls'],
            0
        )   
        
        self.df['required_run_rate'] = np.where(
            (self.df['innings'] == 2) & (self.df['balls_remaining'] > 0),
            (self.df['runs_required'] * 6.0) / self.df['balls_remaining'],
            0  
        )
        
        self.df['current_run_rate'] = self.df['current_run_rate'].clip(upper=99.99)
        self.df['required_run_rate'] = self.df['required_run_rate'].clip(upper=99.99)
    
    def _classify_match_phases(self):
        logger.info("Classifying match phases...")
        
        self.df['match_phase'] = pd.cut(
            self.df['over'],
            bins=[0, 6, 15, 20],
            labels=['Powerplay', 'Middle', 'Death'],
            include_lowest=True
        )
        
        self.df['match_phase'] = self.df['match_phase'].astype(str)
    
    def _calculate_pressure_metrics(self):
        logger.info("Calculating pressure metrics...")
        
        self.df['pressure_index'] = np.where(
            (self.df['innings'] == 2) & (self.df['balls_remaining'] > 0),
            np.minimum(  # Cap at 999.99
                (self.df['required_run_rate'] * 10) + 
                (10 / (self.df['balls_remaining'] + 1)) +
                (self.df['team_wicket'] * 5),
                999.99
            ),
            0
        )
    
    def _add_flags(self):
        logger.info("Adding flags...")
        
        self.df['is_wicket'] = self.df['wicket_kind'].notna()
        self.df['is_boundary'] = self.df['runs_batter'].isin([4, 6])
        self.df['is_six'] = self.df['runs_batter'] == 6
        self.df['is_four'] = self.df['runs_batter'] == 4
        self.df['is_dot_ball'] = (self.df['runs_total'] == 0) & (self.df['valid_ball'] == True)
        self.df['is_new_batter'] = self.df.get('new_batter', False).fillna(False)
        self.df['is_striker_out'] = self.df.get('striker_out', False).fillna(False)
        self.df['is_valid_ball'] = self.df['valid_ball'].fillna(True)
    
    def _clean_data(self):
        logger.info("Cleaning data...")
        
        string_cols = self.df.select_dtypes(include=['object']).columns
        for col in string_cols:
            if col != 'date': 
                self.df[col] = self.df[col].str.strip()
        
        self.df['extra_type'] = self.df['extra_type'].fillna('none')
        self.df['wicket_kind'] = self.df['wicket_kind'].fillna('not out')
        
        numeric_cols = ['runs_batter', 'runs_extras', 'runs_total', 'team_runs', 'team_wicket']
        for col in numeric_cols:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
    
    def _create_ball_sequence(self):
        logger.info("Creating ball sequence...")
        
        if 'over' not in self.df.columns or 'ball' not in self.df.columns:
            logger.error("Missing 'over' or 'ball' columns")
            raise ValueError("Cannot create ball_sequence without over and ball columns")
        
        self.df['ball_no'] = self.df['over'] + (self.df['ball'] / 10.0)
        
        self.df = self.df.sort_values(['match_id', 'innings', 'ball_no'])
        
        self.df['ball_sequence'] = self.df.groupby(['match_id', 'innings']).cumcount() + 1
        
        logger.info(f"Ball sequence created. Range: {self.df['ball_sequence'].min()} to {self.df['ball_sequence'].max()}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = pd.read_csv("data/raw/ipl.csv", nrows=1000, parse_dates=['date'])
    transformer = DataTransformer(df)
    transformed_df = transformer.transform()
    print(transformed_df[['match_phase', 'pressure_index', 'is_boundary']].head(20))