
import logging
from pathlib import Path
from sqlalchemy import text
from config.database import db_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseSetup:

    
    def __init__(self):
        self.engine = db_config.get_engine()
        self.sql_dir = Path('sql')
        
    def run_sql_file(self, filepath):

        logger.info(f"Running {filepath.name}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            sql = f.read()
        

        statements = []
        for line in sql.split('\n'):
            line = line.strip()

            if line.startswith('--') or not line:
                continue
            statements.append(line)
        
        full_sql = ' '.join(statements)
        statements = [s.strip() for s in full_sql.split(';') if s.strip()]
        
        success_count = 0
        error_count = 0
        
        for stmt in statements:
            if not stmt or stmt.startswith('--'):
                continue
                
            try:
                with self.engine.begin() as conn: 
                    conn.execute(text(stmt))
                success_count += 1
            except Exception as e:
                error_count += 1
                if 'already exists' in str(e).lower() or 'does not exist' in str(e).lower():
                    logger.debug(f"Skipped (expected): {str(e)[:100]}")
                else:
                    logger.warning(f"Error executing statement: {str(e)[:200]}")
                    logger.debug(f"Failed statement: {stmt[:100]}...")
        
        logger.info(f"✓ {filepath.name} completed ({success_count} statements executed, {error_count} skipped/failed)")
    
    def setup_database(self):
        logger.info("="*60)
        logger.info("IPL DATA WAREHOUSE - DATABASE SETUP")
        logger.info("="*60)
        
        sql_files = [
            'create_schema.sql',
            'create_dimentions.sql',
            'create_facts.sql',
            'create_marts.sql'
        ]
        
        for sql_file in sql_files:
            filepath = self.sql_dir / sql_file
            if filepath.exists():
                self.run_sql_file(filepath)
            else:
                logger.warning(f"File not found: {filepath}")
        
        logger.info("\n" + "="*60)
        logger.info("DATABASE SETUP COMPLETED!")
        logger.info("="*60)
        

        self.verify_setup()
    
    def verify_setup(self):
        logger.info("\nVerifying database setup...")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'ipl_analytics'
                    AND table_name LIKE 'dim_%'
                    ORDER BY table_name
                """))
                dims = [row[0] for row in result]
                logger.info(f"✓ Dimension tables ({len(dims)}): {', '.join(dims)}")
                
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'ipl_analytics'
                    AND table_name LIKE 'fact_%'
                    ORDER BY table_name
                """))
                facts = [row[0] for row in result]
                logger.info(f"✓ Fact tables ({len(facts)}): {', '.join(facts)}")
                
                result = conn.execute(text("""
                    SELECT matviewname 
                    FROM pg_matviews 
                    WHERE schemaname = 'ipl_analytics'
                    ORDER BY matviewname
                """))
                marts = [row[0] for row in result]
                logger.info(f"✓ Analytical marts ({len(marts)}): {', '.join(marts)}")
                
                if not dims and not facts and not marts:
                    logger.error("❌ No tables were created! Check the SQL files.")
                    return False
                    
                return True
                
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False

    def reset_database(self):
        """Drop and recreate schema (useful for testing)"""
        logger.warning("Dropping existing schema...")
        try:
            with self.engine.begin() as conn:
                conn.execute(text("DROP SCHEMA IF EXISTS ipl_analytics CASCADE"))
                logger.info("✓ Schema dropped")
        except Exception as e:
            logger.error(f"Failed to drop schema: {e}")

if __name__ == "__main__":
    import sys
    
    setup = DatabaseSetup()
    
    # Check for reset flag
    if len(sys.argv) > 1 and sys.argv[1] == '--reset':
        setup.reset_database()
    
    success = setup.setup_database()
    sys.exit(0 if success else 1)