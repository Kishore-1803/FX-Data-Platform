"""
Replay Driver Module:
Simulates batch data arrival by releasing historical slices to a landing area.
Documented strictly as a historical replay driver (NOT a real-time feed).
"""

import os
import glob
import logging
import pandas as pd
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

class HistoricalReplayDriver:
    """
    Simulates new market data arrivals by slicing historical CSV datasets
    and writing incremental arrival batches into a landing directory.
    
    NOTE: This is a replay of historical data for simulation and backtesting purposes.
    """
    def __init__(
        self,
        source_dir: str = "data/external",
        landing_dir: str = "data/landing",
        state_file: str = "data/landing/.replay_state.json"
    ):
        self.source_dir = os.path.abspath(source_dir)
        self.landing_dir = os.path.abspath(landing_dir)
        self.state_file = os.path.abspath(state_file)
        os.makedirs(self.landing_dir, exist_ok=True)
    
    def _load_state(self) -> Dict[str, str]:
        import json
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_state(self, state: Dict[str, str]) -> None:
        import json
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def release_next_slice(
        self,
        slice_size_hours: int = 168, # 1 week of hourly data per slice by default
        pair_filter: Optional[str] = None
    ) -> List[str]:
        """
        Releases the next historical time slice for each pair into the landing area.
        Returns list of landing file paths generated.
        """
        state = self._load_state()
        pattern = os.path.join(self.source_dir, "*_1h_*_histdata.csv")
        files = glob.glob(pattern)
        if not files:
            # Fallback to any CSV in source dir
            files = glob.glob(os.path.join(self.source_dir, "*.csv"))
            
        released_files = []
        for f in sorted(files):
            pair = os.path.basename(f).split("_")[0]
            if pair_filter and pair != pair_filter:
                continue
                
            df = pd.read_csv(f, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            last_timestamp = state.get(pair)
            
            if last_timestamp is not None:
                df_remaining = df[df['Date'] > pd.to_datetime(last_timestamp)]
            else:
                df_remaining = df
                
            if df_remaining.empty:
                logger.info(f"Replay for {pair}: All historical data has already been fully released.")
                continue
                
            slice_df = df_remaining.iloc[:slice_size_hours].copy()
            if slice_df.empty:
                continue
                
            latest_in_slice = str(slice_df['Date'].max())
            state[pair] = latest_in_slice
            
            # Write to landing directory
            start_str = slice_df['Date'].min().strftime('%Y%m%d%H')
            end_str = slice_df['Date'].max().strftime('%Y%m%d%H')
            out_filename = f"{pair}_replay_{start_str}_{end_str}.csv"
            out_path = os.path.join(self.landing_dir, out_filename)
            slice_df.to_csv(out_path, index=False)
            released_files.append(out_path)
            logger.info(f"Replay released {len(slice_df)} rows for {pair} [{start_str} to {end_str}] -> {out_filename}")

        self._save_state(state)
        return released_files

    def release_full_history(self) -> List[str]:
        """
        Releases all historical data for full-dataset initialization runs.
        """
        files = glob.glob(os.path.join(self.source_dir, "*_1h_*_histdata.csv"))
        released = []
        state = self._load_state()
        for f in sorted(files):
            pair = os.path.basename(f).split("_")[0]
            df = pd.read_csv(f, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            out_path = os.path.join(self.landing_dir, f"{pair}_landing_full.csv")
            df.to_csv(out_path, index=False)
            state[pair] = str(df['Date'].max())
            released.append(out_path)
        self._save_state(state)
        return released

