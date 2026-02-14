
import logging
from datetime import datetime
from pathlib import Path

from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'data/logs/etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class IPLDataPipeline:
    
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.extractor = DataExtractor(csv_path)
        self.loader = DataLoader()
        
    def run(self, load_dimensions=True, load_facts=True, refresh_marts=True):
        start_time = datetime.now()
        logger.info("="*60)
        logger.info("IPL DATA WAREHOUSE ETL PIPELINE")
        logger.info("="*60)
        
        try:
    
            logger.info("\n[STEP 1/5] EXTRACTING DATA")
            df = self.extractor.extract()
            
            logger.info("\n[STEP 2/5] TRANSFORMING DATA")
            transformer = DataTransformer(df)
            transformed_df = transformer.transform()
            
            if load_dimensions:
                logger.info("\n[STEP 3/5] LOADING DIMENSIONS")
                self.loader.load_dimensions(transformed_df)
            else:
                logger.info("\n[STEP 3/5] SKIPPING DIMENSIONS")

            if load_facts:
                logger.info("\n[STEP 4/5] LOADING FACTS")
                self.loader.load_facts(transformed_df)
            else:
                logger.info("\n[STEP 4/5] SKIPPING FACTS")
            

            if refresh_marts:
                logger.info("\n[STEP 5/5] REFRESHING ANALYTICAL MARTS")
                self.loader.refresh_marts()
            else:
                logger.info("\n[STEP 5/5] SKIPPING MARTS")
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info("\n" + "="*60)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"Total rows processed: {len(transformed_df):,}")
            logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"\n✗ PIPELINE FAILED: {e}", exc_info=True)
            return False

if __name__ == "__main__":
    import sys
    
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/ipl.csv"
    
    pipeline = IPLDataPipeline(csv_path)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)