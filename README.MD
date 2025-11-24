# Statement of Work: FTR Mispricing Analyzer

## Project Overview

Build a C++ tool that identifies systematically mispriced Financial Transmission Rights (FTRs) by comparing historical auction clearing prices against realized congestion revenue. This project demonstrates understanding of electricity market microstructure and connects to the interviewer's dissertation research on wealth effects in FTR pricing.

---

## Educational Context

### Financial Transmission Rights (FTRs) Fundamentals

**What are FTRs?**

FTRs are financial derivatives that hedge or speculate on congestion costs in electricity transmission networks. When transmission lines reach capacity limits, electricity becomes more expensive at locations downstream of the constraint. FTRs pay holders the congestion component of the LMP differential between two grid nodes.

**Payout Mechanics:**

```
FTR Payout = (Sink LMP - Source LMP) Ã— MW Ã— Hours
```

**Example Calculation:**
- 10 MW FTR from West Texas wind region (source) to Houston load center (sink)
- Houston LMP = $50/MWh, West Texas LMP = $30/MWh
- Hourly payout = ($50 - $30) Ã— 10 MW = $200
- Monthly value (730 hours) = $146,000

**Acquisition Mechanism:**

ISOs (Independent System Operators) auction FTRs on monthly and annual timeframes:

1. ISO publishes available paths with capacity limits based on transmission constraints
2. Market participants submit bids: maximum willingness-to-pay per MW for specific paths
3. Clearing algorithm awards FTRs to highest bidders up to physical transmission limits
4. Clearing price = marginal accepted bid (uniform price auction for each path)
5. Winners pay clearing price and receive monthly congestion rent settlements

**Theoretical Pricing:**

In efficient markets with rational, well-capitalized participants:

```
FTR Auction Price = E[Î£(Sink LMP - Source LMP) over contract period]
```

The auction price should equal the risk-adjusted expected present value of congestion revenue.

**Market Participants and Motivations:**

- **Physical hedgers** (utilities, generators): Lock in transmission cost certainty to protect against volatile congestion charges
- **Financial speculators**: Profit from superior congestion forecasting models
- **Arbitrageurs**: Exploit temporary mispricings between related paths or time periods

**Role in Electricity Markets:**

- Provide price signals for transmission investment needs
- Enable risk transfer from risk-averse entities to speculators
- Improve market efficiency through price discovery on future congestion patterns
- Generate ISO revenue that offsets transmission costs for consumers

### Market Structure and Data Flow

**Two-Settlement System Context:**

FTRs settle against day-ahead market LMPs:
1. Day-ahead market clears 24 hours before delivery â†’ establishes DA-LMPs for all nodes
2. FTR holders receive congestion rent = (Sink DA-LMP - Source DA-LMP) Ã— MW
3. Settlement occurs monthly with payments based on accumulated hourly differences

**Key ISOs and FTR Markets:**

- **PJM** (Mid-Atlantic/Midwest): Largest FTR market by volume (~$2-3 billion annually)
- **MISO** (Midwest): ~$1 billion annual auction revenue
- **ERCOT** (Texas): Doubled FTR volumes over past decade due to wind integration
- **CAISO** (California): Congestion Revenue Rights (CRRs) - similar mechanism
- **NYISO** (New York): Active market due to transmission constraints into NYC

### The Core Thesis: Systematic Mispricing

**Why FTRs Systematically Misprice:**

1. **Capital constraints**: Limited institutional participation prevents full arbitrage (interviewer's dissertation focus - wealth shocks move FTR prices independent of fundamentals)

2. **Information asymmetry**: 
   - Physical participants know planned outages, transmission upgrades, generation retirements
   - Financial traders lack access to utility operational planning
   - Renewable developers know build-out schedules affecting future congestion patterns

3. **Analytical complexity**:
   - Thousands of potential paths with interdependencies
   - Requires sophisticated power flow modeling (DC optimal power flow)
   - Weather-dependent generation patterns (wind/solar) create non-stationary price dynamics

4. **Behavioral factors**:
   - Risk-averse hedgers may systematically overpay for certainty value
   - Speculators may underweight tail risks (extreme weather events)
   - Herding behavior in auction bidding

5. **Market structure frictions**:
   - Credit requirements limit participation
   - Auction timing misaligned with information revelation
   - Path portfolio effects not fully priced (correlations between paths)

**Interviewer's Key Finding:**

PhD dissertation documented: "A 1% increase in wealth added to FTR markets results in 0.16% increase in clearing prices, with no statistically significant movements in underlying spot markets."

**Implication**: FTR prices are partially determined by capital availability rather than pure fundamental value expectations. This suggests:
- Prices can diverge from fair value when capital is constrained
- Well-capitalized traders with good models can systematically profit
- Market efficiency varies with participation levels

**Trading Opportunity:**

If auction prices systematically diverge from realized congestion values:
- **Long opportunity**: Paths consistently clearing below realized value (underpriced hedges or mispriced fundamentals)
- **Short opportunity**: Paths consistently clearing above realized value (overpriced due to behavioral/hedging demand)
- **Portfolio strategy**: Diversified long underpriced + short overpriced paths to reduce basis risk

---

## Project Objectives

### Primary Goal

Build a quantitative tool demonstrating ability to:
1. Rapidly learn domain-specific financial concepts (FTRs, LMP, congestion pricing)
2. Implement practical analysis connecting academic research to trading strategy
3. Handle real-world financial data processing in C++
4. Generate actionable trading insights from market inefficiency

### Success Criteria

A completed FTR Mispricing Analyzer must achieve:

**1. Historical Mispricing Identification**
- Calculate realized FTR returns vs auction clearing prices for 50+ major paths
- Minimum 12 months historical data coverage
- Accurate settlement calculations matching ISO published results (validation)

**2. Statistical Pattern Detection**
- Flag paths with persistent mispricing (>1.5Ïƒ from mean over multiple auctions)
- Calculate hit rate: % of auctions where path outperforms/underperforms clearing price
- Identify whether mispricing is path-specific or market-wide phenomenon

**3. Quantified Opportunity Sizing**
- Estimate gross profit from hypothetical strategy buying top 10 underpriced paths
- Calculate Sharpe ratio of mispricing-based strategy
- Compare returns to market-wide average FTR performance

**4. Market Efficiency Analysis**
- Test correlation between auction participation metrics and pricing efficiency
- Examine whether high-volume auctions show tighter price-to-value relationships
- Time-series analysis: are mispricings arbitraged away or persistent?

**5. Actionable Output**
- Ranked list of paths by expected mispricing magnitude
- Summary statistics enabling quick opportunity assessment
- Specific examples ready for interview discussion

### Core Capabilities

**Data Processing Pipeline:**
- Parse PJM (or other ISO) FTR auction results: clearing prices, awarded MW, participant counts
- Import corresponding day-ahead LMP data for congestion settlement calculations
- Handle multi-month datasets (1000+ paths Ã— 12+ auctions Ã— 720 hours settlement data)
- Data validation: cross-check calculations against published ISO settlement reports

**Analysis Engine:**
- Calculate realized FTR value: `Î£(Sink_LMP - Source_LMP) Ã— MW Ã— hours` over contract period
- Compute mispricing metric: `Realized_Value - Auction_Price`
- Normalize by auction price: `Return = (Realized - Auction) / Auction`
- Statistical aggregation: mean return, standard deviation, Sharpe ratio per path
- Consistency testing: identify paths with persistent positive/negative mispricing across multiple auctions

**Market Microstructure Testing:**
- Proxy variables for market wealth/participation: total auction volume, participant count, clearing price levels
- Correlation analysis: Does higher participation â†’ lower absolute mispricing?
- Regression framework: `|Mispricing| = Î²â‚€ + Î²â‚(Participation) + Î²â‚‚(Controls) + Îµ`
- Temporal analysis: persistence vs mean reversion of mispricings

**Output and Visualization:**
- Command-line interface with configurable parameters
- Ranked tables: top N underpriced/overpriced paths with supporting statistics
- Console-based summary: overall market efficiency metrics
- Optional: CSV export for external plotting (scatter: auction price vs realized value)

---

## Implementation Milestones

### Checkpoint 1: Data Infrastructure and Validation (6-8 hours)

**Objective**: Build robust data ingestion pipeline with validated calculations

**Tasks:**
1. **Data source identification**
   - Navigate PJM Data Miner (or selected ISO data portal)
   - Download FTR auction results: Monthly Balance of Planning Period reports
   - Download day-ahead LMP data: Hourly nodal prices for contract period
   - Document data format specifications and quirks

2. **CSV parsing implementation**
   - Build flexible parser handling ISO-specific formatting (header rows, date formats, null handling)
   - Error handling for malformed data
   - Efficient memory management for large datasets

3. **Data structure design**
   ```cpp
   class FTRPath {
       string path_id, source_node, sink_node;
       double auction_price;       // $/MW for contract period
       double mw_quantity;         // Awarded MW
       vector<double> hourly_congestion;  // Hourly (sink - source) LMP
       // ... methods for calculations
   };
   
   class AuctionData {
       map<string, vector<FTRPath>> paths_by_auction;
       // ... aggregation and analysis methods
   };
   ```

4. **Validation protocol**
   - Select 5 high-volume paths with published settlement data
   - Manually verify program calculations match ISO reports
   - Debug any discrepancies (timezone handling, MW vs MWh confusion, etc.)

**Deliverable**: Working program that correctly loads and processes 12 months of data

**Success Metric**: 
- Loads â‰¥50 paths Ã— 12 auctions successfully
- Calculation validation: <0.1% error vs published settlements for test cases
- Clean console output confirming data load: "Loaded 624 path-auctions, validated 5 samples"

---

### Checkpoint 2: Core Mispricing Analysis (6-8 hours)

**Objective**: Implement statistical analysis identifying systematic mispricings

**Tasks:**
1. **Realized value calculation**
   ```
   For each FTRPath:
       realized_value = 0
       for hour in contract_period:
           sink_lmp = get_lmp(sink_node, hour)
           source_lmp = get_lmp(source_node, hour)
           congestion_rent = (sink_lmp - source_lmp) * mw_quantity
           realized_value += congestion_rent
   ```

2. **Mispricing metrics**
   ```
   mispricing = realized_value - auction_price
   return_pct = (realized_value / auction_price - 1) * 100
   ```

3. **Statistical aggregation**
   - Per path across all auctions: mean return, std dev, Sharpe ratio
   - Hit rate: % auctions where path was profitable (realized > auction)
   - Consistency: paths with positive returns in â‰¥80% of auctions

4. **Ranking and filtering**
   - Sort paths by mean return
   - Filter for statistical significance: t-statistic on mean return
   - Identify top 20 underpriced (positive mispricing) and overpriced (negative)

**Deliverable**: Analysis engine producing ranked opportunity list

**Success Metric**:
- Output top 20 underpriced paths with statistics:
  ```
  Rank | Path              | Avg Return | Std Dev | Sharpe | Hit Rate
  1    | WESTTX -> HOUSTON | +18.3%    | 12.1%   | 1.51   | 91.7%
  ...
  ```
- At least 10 paths showing statistically significant mispricing (t-stat > 2)

---

### Checkpoint 3: Market Efficiency Testing (4-6 hours)

**Objective**: Test whether participation/capital affects pricing efficiency (echoing interviewer's dissertation)

**Tasks:**
1. **Proxy variable construction**
   - Total auction volume (MW cleared) as wealth proxy
   - Participant count (if available in data)
   - Average clearing price level as demand indicator

2. **Efficiency metric definition**
   ```
   Pricing_Error = |Realized_Value - Auction_Price| / Auction_Price
   ```

3. **Correlation analysis**
   - Scatter plot data: X = participation metric, Y = absolute pricing error
   - Calculate Pearson correlation coefficient
   - Hypothesis: Higher participation â†’ Lower pricing error (more efficient)

4. **Temporal analysis**
   - Does mispricing for a specific path persist over time or mean-revert?
   - Autocorrelation of returns: Are past mispricings predictive?

5. **Simple regression** (optional sophistication)
   ```
   Pricing_Error = Î²â‚€ + Î²â‚(log(Volume)) + Î²â‚‚(Month_FE) + Îµ
   ```
   Test if Î²â‚ < 0 (more volume â†’ less mispricing)

**Deliverable**: Statistical evidence on market efficiency drivers

**Success Metric**:
- Correlation coefficient between participation and pricing efficiency
- Console output: "Higher auction volumes associated with r=-0.34 reduction in mispricing"
- 2-3 sentence interpretation connecting to interviewer's wealth effect finding

---

### Checkpoint 4: Polish and Interview Readiness (2-4 hours)

**Objective**: Professional presentation layer and demonstration preparation

**Tasks:**
1. **Command-line interface**
   ```bash
   ./ftr_analyzer --data pjm_2024.csv --top 20 --min-auctions 6
   ./ftr_analyzer --path "WESTTX->HOUSTON" --detailed
   ```

2. **Output formatting**
   - Clean tabular displays with aligned columns
   - Color coding (if terminal supports): green for underpriced, red for overpriced
   - Summary statistics box at start
   - Execution time and data coverage report

3. **Specific example preparation**
   - Select 2-3 compelling examples for discussion:
     * Path with largest absolute mispricing
     * Path with most consistent mispricing (high hit rate)
     * Path showing correlation with external factor (e.g., seasonal pattern)

4. **Visualization export** (optional)
   - CSV output for scatter plot: auction_price, realized_value, path_name
   - Import to Python/Excel for quick visualization if needed during interview

5. **Documentation**
   - README with build instructions and usage examples
   - Brief methodology note explaining calculations
   - Data source citations

**Deliverable**: Demo-ready tool with rehearsed talking points

**Success Metric**:
- Can execute full analysis and explain key finding in <5 minutes
- Output is clear enough for non-technical interviewer to understand opportunity
- 3 specific examples memorized with context:
  * "The West Texas to Houston path cleared at $8/MW but generated $12/MW in congestion revenue, a 50% return. This likely reflects underestimation of summer cooling demand combined with transmission constraints from remote wind resources."

---

## Technical Specifications

### Development Environment

**Language**: C++ (C++17 or later)

**Rationale**: Demonstrates core programming competency, performance for data processing, industry-relevant for algorithmic trading systems

**Required Libraries**:
- Standard Library: `<iostream>`, `<fstream>`, `<vector>`, `<map>`, `<algorithm>`, `<cmath>`
- CSV parsing: Custom implementation or lightweight header-only library
- Statistical functions: Custom implementation (mean, std dev, correlation)

**Optional Libraries**:
- Eigen: Linear algebra for regression analysis (if implementing sophisticated statistics)
- Boost: Program_options for CLI parsing, Date_time for timestamp handling

### Data Specifications

**Input Data Format**:
- FTR auction results: CSV with columns [auction_date, path_id, source, sink, mw, clearing_price]
- LMP data: CSV with columns [datetime, node_id, lmp, energy_component, congestion_component, loss_component]

**Data Scale Estimates**:
- Paths per auction: ~1,000 major tradeable paths
- Auctions: 12 monthly auctions
- Hours per contract: 720 (30-day month)
- Total LMP records: ~1,000 nodes Ã— 720 hours Ã— 12 months = 8.6M rows

**Memory Management**: With efficient data structures, entire dataset should fit in <1GB RAM, easily manageable for modern systems

### Performance Targets

- Data loading: <30 seconds for full annual dataset
- Analysis execution: <10 seconds for complete mispricing calculation
- Total runtime: <1 minute for end-to-end analysis

### Code Quality Standards

- Modular design: Separate classes for data loading, calculations, analysis, output
- Error handling: Graceful handling of missing data, parsing errors
- Comments: Clear documentation of calculation formulas and assumptions
- Validation: Unit tests for core calculation functions (time permitting)

---

## Interview Strategy

### Opening Hook

**Setup**: "I read your dissertation on wealth effects in FTR pricing. To better understand the mechanism, I built a tool that identifies systematically mispriced FTR paths in PJM auctions."

**Why this works**: 
- Shows you researched interviewer specifically (not generic prep)
- Demonstrates intellectual curiosity extending beyond surface learning
- Bridges theory (his research) to practice (trading tool)

### Demonstration Flow (3-5 minutes)

**1. Quick context** (30 seconds)
"FTRs should theoretically clear at expected congestion value, but your research showed prices move with capital availability independent of fundamentals. I wanted to see if that creates exploitable patterns."

**2. Show the tool** (1 minute)
- Execute: `./ftr_analyzer --data pjm_2024.csv --top 10`
- Walk through output: "Here are the 10 most underpriced paths by historical performance"

**3. Specific example** (1-2 minutes)
"This West Texas to Houston path is particularly interesting. It cleared at an average of $8/MW across 12 auctions but generated $12/MW in congestion revenue - a 50% return. 

The mispricing likely reflects:
- West Texas wind is highly volatile, hard to forecast accurately
- Most auction participants are local utilities focused on specific paths, leaving remote wind paths underanalyzed
- Summer peak demand in Houston consistently higher than auction participants expect

This persistence across multiple auctions supports your finding - it's not just a one-time forecast error, but suggests capital or information constraints preventing arbitrage."

**4. Market efficiency connection** (1 minute)
"I also tested whether mispricing correlates with participation levels. Higher auction volumes showed 34% lower pricing errors on average, consistent with your wealth effect finding. When more capital enters the market, price discovery improves."

### Discussion Extensions

**If interviewer engages technically:**
- Discuss data challenges: LMP data quality, auction format complexity
- Statistical methodology: Why Sharpe ratio vs absolute return, significance testing
- Extensions: "Next steps would be adding weather forecasting, modeling transmission topology changes, or building congestion forecast model"

**If interviewer probes trading strategy:**
- Portfolio approach: Long underpriced + short overpriced to hedge basis risk
- Capacity constraints: Position sizing based on available auction volume
- Risk management: Tail events (extreme weather) can overwhelm historical patterns
- Competitive dynamics: If this is obvious, why doesn't it get arbitraged? â†’ Capital constraints, information barriers, operational complexity

**If interviewer questions feasibility:**
- Acknowledge limitations: "This is historical analysis, not predictive. Real strategy needs forward-looking congestion model"
- Data access: "I used public PJM data, but institutional traders have better information on outages, transmission upgrades, generation build-out"
- Transaction costs: "Auction participation requires credit posting, market impact for large positions"

### Key Message

**Core positioning**: "I'm someone who engages deeply with market structure research and can translate academic insights into practical trading tools. I built this in 2-3 days to learn the domain - imagine what I could do with institutional data and full-time focus."

---

## Timeline and Resource Allocation

### Time Budget: 18-24 hours over 2-3 days

**Day 1: Foundation** (8-10 hours)
- Data source research and download: 1-2 hours
- CSV parser and data structures: 3-4 hours
- Initial calculation implementation: 2-3 hours
- Validation testing: 2 hours

**Day 2: Analysis** (6-8 hours)
- Statistical analysis implementation: 3-4 hours
- Market efficiency testing: 2-3 hours
- Debugging and refinement: 1-2 hours

**Day 3: Polish** (4-6 hours)
- CLI and output formatting: 2 hours
- Documentation: 1 hour
- Example preparation and rehearsal: 2-3 hours

### Risk Mitigation

**Data availability issues:**
- Backup plan: Use ERCOT or CAISO if PJM data access problematic
- Fallback: Reduce scope to 6 months if 12-month dataset too large

**Technical complexity:**
- Minimum viable product: Skip regression analysis, focus on descriptive statistics
- Simplification: Analyze top 50 paths only instead of full universe

**Time constraints:**
- Priority order: Checkpoint 1 > Checkpoint 2 > Checkpoint 4 > Checkpoint 3
- Can skip market efficiency testing if time-constrained (nice-to-have, not core demo)

---

## Success Indicators

### Technical Success
- âœ… Program compiles and runs without errors
- âœ… Calculations validated against published ISO data
- âœ… Identifies at least 10 paths with statistically significant mispricing
- âœ… Generates output in <1 minute runtime

### Interview Success
- âœ… Can explain FTR mechanics clearly in 60 seconds
- âœ… Can connect analysis to interviewer's research naturally
- âœ… Has 2-3 specific examples memorized with market context
- âœ… Demonstrates genuine understanding, not just tool-building

### Differentiation Success
- âœ… Shows domain knowledge beyond generic quant prep
- âœ… Demonstrates intellectual curiosity and rapid learning
- âœ… Connects academic theory to practical trading opportunity
- âœ… Positions as someone who creates tools to understand markets, not just follows instructions

---

## Appendix: Additional Resources

### Data Sources
- **PJM Data Miner**: https://dataminer2.pjm.com/
  - FTR auction results under "FTR" section
  - Day-ahead LMP data under "Markets" â†’ "Pricing"
  
- **ERCOT Public Reports**: http://www.ercot.com/mktinfo
  - CRR auction results
  - Day-ahead market prices

### Reference Materials
- Interviewer's dissertation: Focus on Section 3 (wealth shock identification) and Table 4 (main results)
- PJM Manual 6: Financial Transmission Rights (auction rules and settlement)
- Academic papers: "Financial Arbitrage and Market Efficiency" literature on convergence trading

### Tools for Visualization (Post-Analysis)
- Python matplotlib/seaborn for scatter plots if needed
- Excel for quick charts during interview prep
- Online tools: Desmos, Plotly for interactive visualization

---

## Document Version Control

**Version**: 1.0  
**Last Updated**: November 2025  
**Author**: Kyan Nelson  
**Purpose**: Interview preparation for Viribus Fund algorithmic electricity trading role  
**Related Documents**: Resume, Interview Prep Guide, FTR Analysis Codebase