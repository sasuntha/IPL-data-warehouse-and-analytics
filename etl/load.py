
import pandas as pd
import logging
from sqlalchemy import text
from config.database import db_config

logger = logging.getLogger(__name__)

class DataLoader:
    
    def __init__(self):
        self.engine = db_config.get_engine()
        self.batch_size = 10000
        self.dimension_batch_size = 100  
        
    def load_dimensions(self, df):
        """Load all dimension tables"""
        logger.info("Loading dimension tables...")
        
        self._truncate_dimensions()
        
        self._load_dim_date(df)
        self._load_dim_player(df)
        self._load_dim_team(df)
        self._load_dim_venue(df)
        self._load_dim_event(df)
        self._load_dim_umpire(df)
        self._load_dim_match(df)
        
        logger.info("All dimensions loaded successfully")
    
    def _truncate_dimensions(self):
        logger.info("Truncating dimension tables...")
        
        dimension_tables = [
            'dim_date', 'dim_player', 'dim_team', 'dim_venue',
            'dim_event', 'dim_umpire', 'dim_match'
        ]
        
        with self.engine.begin() as conn:
            for table in dimension_tables:
                try:
                    conn.execute(text(f"TRUNCATE TABLE ipl_analytics.{table} CASCADE"))
                    logger.debug(f"Truncated {table}")
                except Exception as e:
                    logger.warning(f"Could not truncate {table}: {e}")
        
        logger.info("Dimension tables truncated")
    
    def _load_dim_date(self, df):
        logger.info("Loading dim_date...")
        
        dates_df = df[['date', 'day', 'month', 'year', 'season', 
                       'day_of_week', 'week_of_year', 'quarter', 'is_weekend']].copy()
        dates_df = dates_df.drop_duplicates(subset=['date'])
        
        dates_df['date_id'] = dates_df['date'].dt.strftime('%Y%m%d').astype(int)
        dates_df['full_date'] = dates_df['date']
        dates_df['day_name'] = dates_df['day_of_week']
        dates_df['month_name'] = dates_df['date'].dt.month_name()
        dates_df['is_holiday'] = False
        
        dates_df = dates_df[[
            'date_id', 'full_date', 'day', 'month', 'year', 'season',
            'day_of_week', 'day_name', 'week_of_year', 'month_name',
            'quarter', 'is_weekend', 'is_holiday'
        ]]
        
        total_rows = len(dates_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = dates_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_date',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(dates_df)} dates")
    
    def _load_dim_player(self, df):

        logger.info("Loading dim_player...")
        
        players = set()
        for col in ['batter', 'bowler', 'non_striker', 'player_out', 'next_batter', 'player_of_match']:
            if col in df.columns:
                players.update(df[col].dropna().unique())
        
        players_df = pd.DataFrame({
            'player_name': sorted(list(players))
        })
        
        players_df['player_role'] = None
        players_df['nationality'] = 'India'
        players_df['batting_style'] = None
        players_df['bowling_style'] = None
        players_df['is_active'] = True
        players_df['debut_year'] = None
        
        total_rows = len(players_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = players_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_player',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(players_df)} players")
    
    def _load_dim_team(self, df):

        logger.info("Loading dim_team...")
        
        teams = set()
        for col in ['batting_team', 'bowling_team', 'toss_winner', 'match_won_by']:
            if col in df.columns:
                teams.update(df[col].dropna().unique())
        
        teams_df = pd.DataFrame({
            'team_name': sorted(list(teams))
        })
        
        teams_df['team_short_name'] = teams_df['team_name'].str[:3].str.upper()
        teams_df['home_city'] = None
        teams_df['team_color'] = None
        teams_df['franchise_owner'] = None
        teams_df['established_year'] = None
        teams_df['is_active'] = True
        teams_df['championships_won'] = 0
        
        total_rows = len(teams_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = teams_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_team',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(teams_df)} teams")
    
    def _load_dim_venue(self, df):
        logger.info("Loading dim_venue...")
        
        venues_df = df[['venue', 'city']].copy()
        venues_df = venues_df.dropna(subset=['venue']).drop_duplicates()
        
        venues_df.columns = ['venue_name', 'city']
        venues_df['state'] = None
        venues_df['country'] = 'India'
        venues_df['capacity'] = None
        venues_df['established_year'] = None
        venues_df['pitch_type'] = None
        venues_df['typical_score'] = None
        

        total_rows = len(venues_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = venues_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_venue',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(venues_df)} venues")
    
    def _load_dim_event(self, df):
        logger.info("Loading dim_event...")
        
        if 'event_name' not in df.columns:
            logger.warning("event_name column not found, skipping dim_event")
            return
        
        events_df = df[['event_name', 'year']].copy()
        events_df = events_df.dropna(subset=['event_name']).drop_duplicates()
        
        events_df.columns = ['event_name', 'event_year']
        events_df['event_type'] = 'League'
        events_df['total_matches'] = None
        events_df['start_date'] = None
        events_df['end_date'] = None
        
        total_rows = len(events_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = events_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_event',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(events_df)} events")
    
    def _load_dim_umpire(self, df):
        logger.info("Loading dim_umpire...")
        
        if 'umpire' not in df.columns:
            logger.warning("umpire column not found, skipping dim_umpire")
            return
        
        umpires = df['umpire'].dropna().unique()
        umpires_df = pd.DataFrame({
            'umpire_name': sorted(umpires)
        })
        
        umpires_df['nationality'] = None
        umpires_df['experience_years'] = None
        umpires_df['is_elite_panel'] = False
        umpires_df['total_matches'] = 0
        
        total_rows = len(umpires_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = umpires_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_umpire',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(umpires_df)} umpires")
    
    def _load_dim_match(self, df):
        logger.info("Loading dim_match...")
        
        matches_df = df[['match_id', 'match_type', 'balls_per_over', 
                        'gender', 'team_type', 'match_number']].copy()
        
        if 'stage' in df.columns:
            matches_df['event_stage'] = df['stage']
        else:
            matches_df['event_stage'] = None
        
        matches_df['match_number'] = matches_df['match_number'].replace('Unknown', None)
        matches_df['match_number'] = pd.to_numeric(matches_df['match_number'], errors='coerce')
        
        matches_df['gender'] = matches_df['gender'].str.capitalize()
            
        matches_df = matches_df.drop_duplicates(subset=['match_id'])
        
        total_rows = len(matches_df)
        for i in range(0, total_rows, self.dimension_batch_size):
            batch = matches_df.iloc[i:i+self.dimension_batch_size]
            batch.to_sql(
                'dim_match',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
        
        logger.info(f"Loaded {len(matches_df)} matches")
    
    def _validate_fact_data(self, df):

        logger.info("Validating fact data...")
        

        duplicates = df.groupby(['match_id', 'innings', 'ball_sequence']).size()
        duplicates = duplicates[duplicates > 1]
        
        if len(duplicates) > 0:
            logger.error(f"Found {len(duplicates)} duplicate keys in data!")
            logger.error(f"Sample duplicates:\n{duplicates.head()}")
            raise ValueError("Duplicate keys found in source data")
        

        initial_count = len(df)
        invalid_innings = df[~df['innings'].isin([1, 2])]
        
        if len(invalid_innings) > 0:
            logger.warning(f"Found {len(invalid_innings)} rows with invalid innings (not 1 or 2)")
            logger.warning(f"Innings values found: {invalid_innings['innings'].unique()}")
            logger.warning(f"Sample matches: {invalid_innings['match_id'].unique()[:5]}")
            df = df[df['innings'].isin([1, 2])].copy()
            logger.info(f"Filtered invalid innings: {initial_count} -> {len(df)} rows")
        
        logger.info("Fact data validation passed")
        return df
    
    def load_facts(self, df):
        logger.info("Loading fact tables...")
        

        df = self._validate_fact_data(df)  
        

        self._truncate_facts()
        

        lookups = self._get_dimension_lookups()
        
        # Load ball delivery fact
        self._load_fact_ball_delivery(df, lookups)  
        

        self._load_fact_innings_summary(df, lookups)
        self._load_fact_match_summary(df, lookups)
        
        logger.info("All facts loaded successfully")
    
    def _truncate_facts(self):

        logger.info("Truncating fact tables...")
        
        fact_tables = [
            'fact_ball_delivery', 'fact_innings_summary', 'fact_match_summary'
        ]
        
        with self.engine.begin() as conn:
            for table in fact_tables:
                try:
                    sql = text(f"TRUNCATE TABLE ipl_analytics.{table} CASCADE")
                    conn.execute(sql)
                    logger.info(f"Successfully truncated {table}")
                except Exception as e:
                    logger.error(f"Failed to truncate {table}: {e}")
                    raise  
        
        logger.info("Fact tables truncated successfully")
    
    def _get_dimension_lookups(self):
        logger.info("Creating dimension lookups...")
        
        lookups = {}
        
        # Date lookup
        date_df = pd.read_sql("SELECT date_id, full_date FROM ipl_analytics.dim_date", self.engine)

        date_df['full_date'] = pd.to_datetime(date_df['full_date'])
        lookups['date'] = dict(zip(date_df['full_date'].dt.date, date_df['date_id']))
        
  
        player_df = pd.read_sql("SELECT player_id, player_name FROM ipl_analytics.dim_player", self.engine)
        lookups['player'] = dict(zip(player_df['player_name'], player_df['player_id']))
        
        # Team lookup
        team_df = pd.read_sql("SELECT team_id, team_name FROM ipl_analytics.dim_team", self.engine)
        lookups['team'] = dict(zip(team_df['team_name'], team_df['team_id']))
        
        # Venue lookup
        venue_df = pd.read_sql("SELECT venue_id, venue_name, city FROM ipl_analytics.dim_venue", self.engine)
        lookups['venue'] = {(row['venue_name'], row['city']): row['venue_id'] 
                           for _, row in venue_df.iterrows()}
        
        # Umpire lookup (if exists)
        try:
            umpire_df = pd.read_sql("SELECT umpire_id, umpire_name FROM ipl_analytics.dim_umpire", self.engine)
            lookups['umpire'] = dict(zip(umpire_df['umpire_name'], umpire_df['umpire_id']))
        except:
            lookups['umpire'] = {}
        
        return lookups
    
    def _load_fact_ball_delivery(self, df, lookups):
        """Load ball delivery fact table"""
        logger.info("Loading fact_ball_delivery...")
        
        fact_df = df.copy()
        
        # Map foreign keys - using vectorized operations for speed
        # Convert date to date object first (vectorized)
        fact_df['date_key'] = fact_df['date'].dt.date
        fact_df['date_id'] = fact_df['date_key'].map(lookups['date'])
        
        fact_df['batter_id'] = fact_df['batter'].map(lookups['player'])
        fact_df['bowler_id'] = fact_df['bowler'].map(lookups['player'])
        fact_df['non_striker_id'] = fact_df['non_striker'].map(lookups['player'])
        fact_df['batting_team_id'] = fact_df['batting_team'].map(lookups['team'])
        fact_df['bowling_team_id'] = fact_df['bowling_team'].map(lookups['team'])
        fact_df['player_out_id'] = fact_df['player_out'].map(lookups['player'])
        fact_df['next_batter_id'] = fact_df.get('next_batter', pd.Series()).map(lookups['player'])
        fact_df['umpire_id'] = fact_df.get('umpire', pd.Series()).map(lookups['umpire'])
        
        # Venue lookup - use merge for better performance
        venue_df_lookup = pd.DataFrame([
            {'venue': k[0], 'city': k[1], 'venue_id': v} 
            for k, v in lookups['venue'].items()
        ])
        fact_df = fact_df.merge(venue_df_lookup, on=['venue', 'city'], how='left')
        
        # Rename columns to match schema
        # Rename columns to match schema
        # Rename columns to match schema
        column_mapping = {
            'over': 'over_number',
            'ball': 'ball_number',
            # ball_sequence is already created in transform, no need to rename
            'bat_pos': 'bat_position',
            'non_striker_pos': 'non_striker_position',
            'runs_batter': 'runs_scored',
            'runs_bowler': 'runs_bowler',
            'team_wicket': 'team_wickets',
            'bowler_wicket': 'bowler_wickets'
        }
        fact_df = fact_df.rename(columns=column_mapping)
        
        # Select columns for fact table
        fact_columns = [
            'match_id', 'date_id', 'batter_id', 'bowler_id', 'non_striker_id',
            'batting_team_id', 'bowling_team_id', 'venue_id', 'umpire_id',
            'innings', 'over_number', 'ball_number', 'ball_sequence',
            'bat_position', 'non_striker_position', 'match_phase',
            'runs_scored', 'runs_extras', 'runs_total', 'runs_bowler',
            'balls_faced', 'runs_target', 'runs_required', 'balls_remaining',
            'team_runs', 'team_balls', 'team_wickets',
            'batter_runs', 'batter_balls', 'bowler_wickets',
            'current_run_rate', 'required_run_rate', 'pressure_index',
            'is_valid_ball', 'is_wicket', 'is_boundary', 'is_six', 'is_four',
            'is_dot_ball', 'is_new_batter', 'is_striker_out',
            'extra_type', 'wicket_kind', 'player_out_id', 'fielders',
            'batting_partners', 'next_batter_id'
        ]
        
        # Filter to existing columns
        fact_columns = [col for col in fact_columns if col in fact_df.columns]
        fact_df = fact_df[fact_columns]
        
        # Convert integer boolean columns to actual booleans
        bool_cols = ['is_valid_ball', 'is_wicket', 'is_boundary', 'is_six', 'is_four',
                     'is_dot_ball', 'is_new_batter', 'is_striker_out']
        for col in bool_cols:
            if col in fact_df.columns:
                fact_df[col] = fact_df[col].astype(bool)
        
        # Load in batches (smaller batches and no 'multi' to avoid parameter limits)
        total_rows = len(fact_df)
        batch_size = 1000  # Smaller batch size for fact tables
        for i in range(0, total_rows, batch_size):
            batch = fact_df.iloc[i:i+batch_size]
            batch.to_sql(
                'fact_ball_delivery',
                self.engine,
                schema='ipl_analytics',
                if_exists='append',
                index=False
            )
            logger.info(f"Loaded batch {i//batch_size + 1}/{(total_rows//batch_size) + 1}")
        
        logger.info(f"Loaded {total_rows} ball delivery records")
    
    def _load_fact_innings_summary(self, df, lookups):
        """Load innings summary from ball delivery data"""
        logger.info("Aggregating and loading fact_innings_summary...")
        
        # This would be aggregated from fact_ball_delivery
        # For now, we'll create from source data
        
        innings_agg = df.groupby(['match_id', 'innings', 'batting_team', 'bowling_team']).agg({
            'runs_total': 'sum',
            'ball_no': 'count',
            'is_wicket': 'sum',
            'is_boundary': 'sum',
            'is_six': 'sum',
            'is_four': 'sum',
            'is_dot_ball': 'sum',
            'runs_extras': 'sum'
        }).reset_index()
        
        # Map to schema
        innings_agg['batting_team_id'] = innings_agg['batting_team'].map(lookups['team'])
        innings_agg['bowling_team_id'] = innings_agg['bowling_team'].map(lookups['team'])
        innings_agg['innings_number'] = innings_agg['innings']
        innings_agg['total_runs'] = innings_agg['runs_total']
        innings_agg['total_wickets'] = innings_agg['is_wicket']
        innings_agg['total_balls'] = innings_agg['ball_no']
        innings_agg['total_overs'] = innings_agg['total_balls'] / 6.0
        
        # Add date and venue
        match_info = df.groupby('match_id').first()[['date', 'venue', 'city']].reset_index()
        innings_agg = innings_agg.merge(match_info, on='match_id')
        
        # Vectorized lookups
        innings_agg['date_key'] = innings_agg['date'].dt.date
        innings_agg['date_id'] = innings_agg['date_key'].map(lookups['date'])
        
        # Venue lookup via merge
        venue_df_lookup = pd.DataFrame([
            {'venue': k[0], 'city': k[1], 'venue_id': v} 
            for k, v in lookups['venue'].items()
        ])
        innings_agg = innings_agg.merge(venue_df_lookup, on=['venue', 'city'], how='left')
        
        # Select final columns
        innings_cols = [
            'match_id', 'innings_number', 'batting_team_id', 'bowling_team_id',
            'date_id', 'venue_id', 'total_runs', 'total_wickets', 'total_overs', 'total_balls'
        ]
        innings_final = innings_agg[[col for col in innings_cols if col in innings_agg.columns]]
        
        innings_final.to_sql(
            'fact_innings_summary',
            self.engine,
            schema='ipl_analytics',
            if_exists='append',
            index=False
        )
        
        logger.info(f"Loaded {len(innings_final)} innings summaries")
    
    def _load_fact_match_summary(self, df, lookups):
        """Load match summary"""
        logger.info("Loading fact_match_summary...")
        
        # Get first row per match for match-level data
        match_df = df.groupby('match_id').first().reset_index()
        
        # Map foreign keys - vectorized lookups
        match_df['date_key'] = match_df['date'].dt.date
        match_df['date_id'] = match_df['date_key'].map(lookups['date'])
        
        # Venue lookup via merge
        venue_df_lookup = pd.DataFrame([
            {'venue': k[0], 'city': k[1], 'venue_id': v} 
            for k, v in lookups['venue'].items()
        ])
        match_df = match_df.merge(venue_df_lookup, on=['venue', 'city'], how='left')
        
        # Get teams (first batting/bowling teams)
        first_ball = df.groupby('match_id').first().reset_index()
        match_df['team1_id'] = first_ball['batting_team'].map(lookups['team'])
        match_df['team2_id'] = first_ball['bowling_team'].map(lookups['team'])
        
        # Map other IDs
        if 'toss_winner' in match_df.columns:
            match_df['toss_winner_id'] = match_df['toss_winner'].map(lookups['team'])
        if 'match_won_by' in match_df.columns:
            match_df['match_winner_id'] = match_df['match_won_by'].map(lookups['team'])
        if 'player_of_match' in match_df.columns:
            match_df['player_of_match_id'] = match_df['player_of_match'].map(lookups['player'])
        
        # Calculate team totals from innings
        # FIX: Use 'team_wicket' (singular) not 'team_wickets' (plural)
        team_totals = df.groupby(['match_id', 'innings']).agg({
            'team_runs': 'max',
            'team_wicket': 'max',  # Changed from team_wickets to team_wicket
            'team_balls': 'max'
        }).reset_index()
        
        # Pivot to get team1 and team2 stats
        team1_stats = team_totals[team_totals['innings'] == 1].rename(columns={
            'team_runs': 'team1_score',
            'team_wicket': 'team1_wickets',  # Changed from team_wickets
            'team_balls': 'team1_balls'
        })[['match_id', 'team1_score', 'team1_wickets', 'team1_balls']]
        
        team2_stats = team_totals[team_totals['innings'] == 2].rename(columns={
            'team_runs': 'team2_score',
            'team_wicket': 'team2_wickets',  # Changed from team_wickets
            'team_balls': 'team2_balls'
        })[['match_id', 'team2_score', 'team2_wickets', 'team2_balls']]
        
        match_df = match_df.merge(team1_stats, on='match_id', how='left')
        match_df = match_df.merge(team2_stats, on='match_id', how='left')
        
        # Calculate overs
        match_df['team1_overs'] = match_df['team1_balls'] / 6.0
        match_df['team2_overs'] = match_df['team2_balls'] / 6.0
        
        # Select columns
        summary_cols = [
            'match_id', 'date_id', 'venue_id', 'team1_id', 'team2_id',
            'toss_winner_id', 'match_winner_id', 'player_of_match_id',
            'team1_score', 'team1_wickets', 'team1_overs',
            'team2_score', 'team2_wickets', 'team2_overs'
        ]
        
        match_final = match_df[[col for col in summary_cols if col in match_df.columns]]
        
        match_final.to_sql(
            'fact_match_summary',
            self.engine,
            schema='ipl_analytics',
            if_exists='append',
            index=False
        )
        
        logger.info(f"Loaded {len(match_final)} match summaries")
    
    def refresh_marts(self):
        """Refresh all materialized views"""
        logger.info("Refreshing analytical marts...")
        
        marts = [
            'mart_death_over_specialists',
            'mart_powerplay_performers',
            'mart_pressure_performance',
            'mart_partnership_analysis',
            'mart_venue_analytics',
            'mart_player_stats'
        ]
        
        for mart in marts:
            try:
                logger.info(f"Refreshing {mart}...")
                with self.engine.connect() as conn:
                    conn.execute(text(f"REFRESH MATERIALIZED VIEW ipl_analytics.{mart}"))
                    conn.commit()
                logger.info(f"✓ {mart} refreshed")
            except Exception as e:
                logger.error(f"✗ Error refreshing {mart}: {e}")
        
        logger.info("All marts refreshed successfully")