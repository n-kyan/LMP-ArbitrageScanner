# LMP Arbitrage Scanner

C++ analysis engine for identifying profitable DA-RT LMP spread opportunities in PJM electricity markets.

## Features

**Tier 1 (Core Metrics)**:
- Per-node Sharpe ratios (risk-adjusted returns)
- Hit rates, mean/std spreads
- Transaction cost filtering ($0.75/MWh)
- Zone-level aggregations

**Tier 2 (Advanced Analysis)**:
- Component decomposition (congestion vs energy spreads)
- Hourly pattern detection
- Best trading hours per node

## Build Instructions

```bash
cd lmp_scanner
mkdir build && cd build
cmake ..
make
```

## Usage

```bash
# From build directory
./lmp_scanner ../lmp_data_merged.csv 0.75

# Arguments:
#   1. Path to merged CSV file
#   2. Transaction cost ($/MWh) - default 0.75
```

## Output Files

- `node_rankings.csv` - Top 100 nodes by Sharpe ratio
- `zone_summary.csv` - Zone-level aggregations
- `component_analysis.csv` - Congestion/energy breakdown
- `hourly_patterns.csv` - Time-of-day spread patterns
- `summary_report.txt` - Human-readable summary

## Performance

- Streaming CSV parser (constant ~3MB memory footprint)
- Welford's online algorithm for statistics
- Processes 31M rows in ~5-6 minutes on modern hardware

## Technical Notes

- **Memory-efficient**: Uses online statistics, never loads full dataset
- **Single-pass**: Calculates all metrics in one stream through data
- **Transaction cost aware**: Filters unprofitable opportunities
- **Annualized Sharpe**: Assumes hourly data (sqrt(8760) factor)