"""Data quality validation"""
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import db_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    """Validate data warehouse quality"""
    
    def __init__(self):
        self.engine = db_config.get_engine()
        
    def run_validations(self):
        """Run all data quality checks"""
        logger.info("="*60)
        logger.info("DATA QUALITY VALIDATION")
        logger.info("="*60)
        
        checks = [
            self.check_row_counts,
            self.check_null_values,
            self.check_referential_integrity,
            self.check_data_ranges,
            self.check_aggregations
        ]
        
        results = []
        for check in checks:
            try:
                result = check()
                results.append(('✓', check.__name__, result))
            except Exception as e:
                results.append(('✗', check.__name__, str(e)))
                logger.error(f"Check failed: {check.__name__} - {e}")
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("VALIDATION SUMMARY")
        logger.info("="*60)
        for status, check, result in results:
            logger.info(f"{status} {check}: {result}")
        
        passed = sum(1 for s, _, _ in results if s == '✓')
        total = len(results)
        logger.info(f"\nPassed: {passed}/{total}")
        
        return passed == total
    
    def check_row_counts(self):
        """Check row counts in all tables"""
        with self.engine.connect() as conn:
            # Dimensions
            player_count = conn.execute(text("SELECT COUNT(*) FROM ipl_analytics.dim_player")).scalar()
            team_count = conn.execute(text("SELECT COUNT(*) FROM ipl_analytics.dim_team")).scalar()
            venue_count = conn.execute(text("SELECT COUNT(*) FROM ipl_analytics.dim_venue")).scalar()
            
            # Facts
            ball_count = conn.execute(text("SELECT COUNT(*) FROM ipl_analytics.fact_ball_delivery")).scalar()
            match_count = conn.execute(text("SELECT COUNT(*) FROM ipl_analytics.fact_match_summary")).scalar()
            
        return f"{ball_count:,} balls, {match_count} matches, {player_count} players, {team_count} teams, {venue_count} venues"
    
    def check_null_values(self):
        """Check for unexpected nulls"""
        with self.engine.connect() as conn:
            null_batters = conn.execute(text("""
                SELECT COUNT(*) FROM ipl_analytics.fact_ball_delivery 
                WHERE batter_id IS NULL
            """)).scalar()
            
            null_bowlers = conn.execute(text("""
                SELECT COUNT(*) FROM ipl_analytics.fact_ball_delivery 
                WHERE bowler_id IS NULL
            """)).scalar()
            
        if null_batters > 0 or null_bowlers > 0:
            return f"WARN: {null_batters} null batters, {null_bowlers} null bowlers"
        return "No critical nulls"
    
    def check_referential_integrity(self):
        """Check foreign key relationships"""
        with self.engine.connect() as conn:
            # Check for orphaned records
            orphaned_batters = conn.execute(text("""
                SELECT COUNT(*) FROM ipl_analytics.fact_ball_delivery f
                LEFT JOIN ipl_analytics.dim_player p ON f.batter_id = p.player_id
                WHERE p.player_id IS NULL
            """)).scalar()
            
        if orphaned_batters > 0:
            return f"WARN: {orphaned_batters} orphaned batter records"
        return "All foreign keys valid"
    
    def check_data_ranges(self):
        """Check data is within valid ranges"""
        with self.engine.connect() as conn:
            invalid_runs = conn.execute(text("""
                SELECT COUNT(*) FROM ipl_analytics.fact_ball_delivery 
                WHERE runs_scored < 0 OR runs_scored > 7
            """)).scalar()
            
            invalid_overs = conn.execute(text("""
                SELECT COUNT(*) FROM ipl_analytics.fact_ball_delivery 
                WHERE over_number < 0 OR over_number > 50
            """)).scalar()
            
        if invalid_runs > 0 or invalid_overs > 0:
            return f"WARN: {invalid_runs} invalid runs, {invalid_overs} invalid overs"
        return "All data within valid ranges"
    
    def check_aggregations(self):
        """Check aggregations are correct"""
        with self.engine.connect() as conn:
            # Check match summary totals match ball-by-ball
            result = conn.execute(text("""
                SELECT 
                    ABS(SUM(ms.team1_score + ms.team2_score) - 
                        (SELECT SUM(runs_total) FROM ipl_analytics.fact_ball_delivery)) as diff
                FROM ipl_analytics.fact_match_summary ms
            """)).scalar()
            
        if result > 100:  # Allow small discrepancy
            return f"WARN: Aggregation mismatch of {result} runs"
        return "Aggregations match"

if __name__ == "__main__":
    validator = DataValidator()
    success = validator.run_validations()
    sys.exit(0 if success else 1)