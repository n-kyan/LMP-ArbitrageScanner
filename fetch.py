import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import os
import json
from dotenv import load_dotenv

# Used for more informed console messages. Will pring timestamp along with message.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
# Makes the logger object and ties it to this program.
logger = logging.getLogger(__name__)


class PJMFetcher:
    # Fetches PJM Real Time and Day Ahead LMP data by zone
    
    BASE_URL = "https://api.pjm.com/api/v1"
    MIN_INTERVAL = 15  # 6 requests/minute rate limit
    CHECKPOINT_FILE = 'fetch_checkpoint.json'
    DATA_FILE_RT = 'rt_data_partial.csv'
    DATA_FILE_DA = 'da_data_partial.csv'

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.last_request_time = 0
        self.requests_made = 0
        self.checkpoint = self._load_checkpoint()
    
    # internal helper functions
    def _load_checkpoint(self) -> dict:
        if os.path.exists(self.CHECKPOINT_FILE):
            with open(self.CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                logger.info(f"📁 Loaded checkpoint: {checkpoint}")
                return checkpoint
        return {
            'rt_complete': False,
            'rt_start_row': 1,
            'da_complete': False,
            'da_start_row': 1,
            'start_date': None,
            'end_date': None
        }

    def _save_checkpoint(self):
        with open(self.CHECKPOINT_FILE, 'w') as f:
            json.dump(self.checkpoint, f, indent=2)
        logger.info(f"💾 Checkpoint saved: RT row {self.checkpoint['rt_start_row']}, "f"DA row {self.checkpoint['da_start_row']}")

    def _append_to_csv(self, df: pd.DataFrame, filepath: str):
        if os.path.exists(filepath):
            df.to_csv(filepath, mode='a', header=False, index=False)
        else:
            df.to_csv(filepath, mode='w', header=True, index=False)

    def _wait_rate_limit(self):
        elapsed = time.time() - self.last_request_time # returns the current time as a float representing seconds since January 1, 1970
        if elapsed < self.MIN_INTERVAL:
            time.sleep(self.MIN_INTERVAL - elapsed)
        self.last_request_time = time.time()
        self.requests_made += 1
    
    # coverts start and end datetime objects into the proper format for the API
    def _format_dates(self, start: datetime, end: datetime) -> str:
        start_str = start.strftime("%m/%d/%Y %H:%M").lstrip("0").replace(" 0", " ")
        end_str = end.strftime("%m/%d/%Y %H:%M").lstrip("0").replace(" 0", " ")
        return f"{start_str} to {end_str}"
    
    def fetch_data_paginated(self, start: datetime, end: datetime, market: str) -> pd.DataFrame:
            # Fetch all nodes for date range with pagination
            endpoint = 'da_hrl_lmps' if market == 'da' else 'rt_hrl_lmps'
            start_row = 1
            chunk_num = 0
            temp_files = []
            rows = 50000

            if market == 'rt':
                if self.checkpoint['rt_complete']:
                    logger.info(f"✅ RT data already complete, skipping fetch")
                    return pd.read_csv(self.DATA_FILE_RT)
                start_row = self.checkpoint['rt_start_row']
                data_file = self.DATA_FILE_RT
            else:  # DA
                if self.checkpoint['da_complete']:
                    logger.info(f"✅ DA data already complete, skipping fetch")
                    return pd.read_csv(self.DATA_FILE_DA)
                start_row = self.checkpoint['da_start_row']
                data_file = self.DATA_FILE_DA

            # Store date range in checkpoint for validation
            if self.checkpoint['start_date'] is None:
                self.checkpoint['start_date'] = start.isoformat()
                self.checkpoint['end_date'] = end.isoformat()
                self._save_checkpoint()
            else:
                # Verify we're resuming the same date range
                if (self.checkpoint['start_date'] != start.isoformat() or 
                    self.checkpoint['end_date'] != end.isoformat()):
                    raise ValueError("Date range mismatch! Delete checkpoint file to start fresh.")

            logger.info(f"{market.upper()}: Starting from row {start_row:,}...")
            
            try:
                while True:
                    logger.info(f"{market.upper()}: Fetching rows {start_row:,}+...")
                    
                    params = {
                        'rowCount': rows,
                        'startRow': start_row,
                        'datetime_beginning_ept': self._format_dates(start, end)
                    }
                    
                    self._wait_rate_limit()
                    
                    try:
                        response = requests.get(
                            f"{self.BASE_URL}/{endpoint}",
                            params=params,
                            headers={'Ocp-Apim-Subscription-Key': self.api_key},
                            timeout=(30, 600)
                        )
                        response.raise_for_status()
                        data = response.json()
                        
                        if not data.get('items'):
                            logger.info(f"{market.upper()}: No more data")
                            break

                        batch = pd.DataFrame(data['items'])
                        # Standardize column names
                        batch = batch.rename(columns={
                            'total_lmp_rt': 'total_lmp', 'total_lmp_da': 'total_lmp',
                            'congestion_price_rt': 'congestion', 'congestion_price_da': 'congestion',
                            'marginal_loss_price_rt': 'loss', 'marginal_loss_price_da': 'loss',
                            'system_energy_price_rt': 'energy', 'system_energy_price_da': 'energy'
                        })

                        batch['datetime'] = pd.to_datetime(batch['datetime_beginning_ept'])

                        # Keep only essential columns
                        batch = batch[['datetime', 'pnode_id', 'pnode_name', 'type', 'zone', 'voltage', 'total_lmp', 'congestion', 'loss', 'energy']]

                        # Append to CSV immediately (streaming approach)
                        self._append_to_csv(batch, data_file)

                        rows_fetched = len(batch)
                        logger.info(f"  ✓ Got {rows_fetched:,} records, appended to {data_file}")

                        # Update Checkpoint
                        start_row += rows_fetched
                        if market == 'rt':
                            self.checkpoint['rt_start_row'] = start_row
                        else:
                            self.checkpoint['da_start_row'] = start_row
                        self._save_checkpoint()

                        # Old way with temp files
                        # temp_file = f'data/temp_{market}_{chunk_num}.csv'
                        # batch = pd.DataFrame(data['items'])
                        # batch.to_csv(temp_file, index=False)
                        # temp_files.append(temp_file)
                        # chunk_num += 1
                        
                        if rows_fetched < rows:
                            logger.info(f"{market.upper()}: ✅ Complete!")
                            break
                            
                        
                    except Exception as e:
                        logger.error(f"Error during fetch: {e}")
                        logger.info(f"Progress saved. You can resume by running the script again.")
                        raise  # Re-raise to stop execution
                
                # if not temp_files:
                #     return pd.DataFrame()
                
                # logger.info(f"Combining {len(temp_files)} temp files...")
                # df = pd.concat([pd.read_csv(f) for f in temp_files], ignore_index=True)
        
                # for f in temp_files:
                #     os.remove(f)
        
                # Standardize column names
                # df = df.rename(columns={
                #     'total_lmp_rt': 'total_lmp', 'total_lmp_da': 'total_lmp',
                #     'congestion_price_rt': 'congestion', 'congestion_price_da': 'congestion',
                #     'marginal_loss_price_rt': 'loss', 'marginal_loss_price_da': 'loss',
                #     'system_energy_price_rt': 'energy', 'system_energy_price_da': 'energy'
                # })
                
                # df['datetime'] = pd.to_datetime(df['datetime_beginning_ept'])
                
                # return df[['datetime', 'pnode_id', 'pnode_name', 'type', 'zone', 
                #         'voltage', 'total_lmp', 'congestion', 'loss', 'energy']]

            # Mark as complete
                if market == 'rt':
                    self.checkpoint['rt_complete'] = True
                else:
                    self.checkpoint['da_complete'] = True
                self._save_checkpoint()
                
                # Load full dataset from CSV
                df = pd.read_csv(data_file)
                return df
    
            except KeyboardInterrupt:
                logger.warning(f"\n⚠️  Interrupted! Progress saved at row {start_row:,}")
                logger.info(f"Run script again to resume from this point.")
            raise


def main():
    API_KEY = os.getenv("PJM_API_KEY")
    if not API_KEY:
        raise ValueError("PJM_API_KEY not found in environment")
    
    logger.info("="*70)
    logger.info("PJM DATA FETCH")
    logger.info("="*70)
    
    fetcher = PJMFetcher(API_KEY)
    start_time = time.time()
    
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=365)

    # on month 1
    # months = 0
    # day_offset = months * 30
    
    # end_date = datetime.now() - timedelta(days=3+day_offset)
    # start_date = end_date - timedelta(days=30+day_offset)

    end_date = datetime(2025, 8, 31, 23, 0)
    start_date = datetime(2025, 6, 1, 0, 0)
    logger.info(f"Date range: {start_date} to {end_date}")


    try:
        logger.info("\n" + "="*70)
        logger.info("\nFetching Real-Time data...")
        rt = fetcher.fetch_data_paginated(start_date, end_date, market='rt')
        
        if rt.empty:
            logger.error("Failed to fetch RT data")
            return
        logger.info(f"RT data: {len(rt):,} records, {rt['pnode_id'].nunique():,} nodes")
        
        time.sleep(12)

        logger.info("\n" + "="*70)
        logger.info("\nFetching Day-Ahead data...")
        da = fetcher.fetch_data_paginated(start_date, end_date, market='da')
        
        if da.empty:
            logger.error("Failed to fetch DA data")
            return
        logger.info(f"DA data: {len(da):,} records, {da['pnode_id'].nunique():,} nodes")

        
        # Merge on datetime + pnode_id
        logger.info("\n" + "="*70)
        logger.info("\nMerging DA and RT data...")
        df = pd.merge(
            da.add_suffix('_da'),
            rt.add_suffix('_rt'),
            left_on=['datetime_da', 'pnode_id_da'],
            right_on=['datetime_rt', 'pnode_id_rt'],
            how='inner'
        )
        
        # Clean up column names and calculate spread
        df['datetime'] = df['datetime_da']
        df['pnode_id'] = df['pnode_id_da']
        df['zone'] = df['zone_da']
        df['spread'] = df['total_lmp_da'] - df['total_lmp_rt']

        # Save final merged file
        output_file = 'lmp_data_merged.csv'
        df.to_csv(output_file, index=False)

        # Clean up partial files and checkpoint
        os.remove(fetcher.DATA_FILE_RT)
        os.remove(fetcher.DATA_FILE_DA)
        os.remove(fetcher.CHECKPOINT_FILE)
        logger.info("🗑️  Cleaned up checkpoint and partial files")
        
        # # Keep essential columns
        # df = df[['datetime', 'pnode_id', 'pnode_name_da', 'zone', 
        #          'total_lmp_da', 'total_lmp_rt', 'spread',
        #          'congestion_da', 'congestion_rt',
        #          'energy_da', 'energy_rt',
        #          'loss_da', 'loss_rt']]
        
        elapsed = time.time() - start_time
        
        # Save (old)
        # output_file = 'lmp_data_merged.csv'
        # df.to_csv(output_file, index=False)
        
        # Summary (rest of your code is fine)
        logger.info("\n" + "="*70)
        logger.info("RESULTS")
        logger.info("="*70)
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Unique nodes: {df['pnode_id'].nunique():,}")
        # logger.info(f"Zones: {sorted(df['zone'].unique())}")
        logger.info(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
        logger.info(f"API requests: {fetcher.requests_made}")
        logger.info(f"Time elapsed: {elapsed/60:.1f} minutes")
        logger.info(f"Saved to: {output_file}")
        logger.info("="*70)

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Script interrupted - progress saved!")
        logger.info("Run the script again to resume from checkpoint.")
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.info("Progress has been saved. Fix the error and run again to resume.")
    
    # Spread stats
    # logger.info("\nSpread Statistics:")
    # logger.info(f"  Mean: ${df['spread'].mean():.2f}/MWh")
    # logger.info(f"  Std: ${df['spread'].std():.2f}/MWh")
    # logger.info(f"  Win rate: {(df['spread'] > 0).sum() / len(df) * 100:.1f}%")
    
    # Top nodes by Sharpe
    # logger.info("\nTop 15 Nodes by Sharpe Ratio:")
    # node_stats = df.groupby('pnode_id')['spread'].agg(['mean', 'std', 'count'])
    # node_stats['sharpe'] = node_stats['mean'] / node_stats['std'].replace(0, 1)
    # node_stats = node_stats[node_stats['count'] > 500]
    # top = node_stats.nlargest(15, 'sharpe')
    # for idx, (node, row) in enumerate(top.iterrows(), 1):
    #     print(f"  {idx:2d}. {node}: Sharpe={row['sharpe']:6.2f}, "
    #           f"mean=${row['mean']:6.2f}, n={int(row['count']):,}")
        
if __name__ == '__main__':
    main()