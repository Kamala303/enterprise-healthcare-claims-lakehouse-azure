# Power BI Dashboard KPI Definitions

## Executive Overview Dashboard KPIs

### Primary KPI Cards
1. **Total Claims Processed**
   - Definition: Count of all claims processed in the selected time period
   - Source: `analytics.fct_claims`
   - Calculation: `COUNT(DISTINCT claim_id)`
   - Format: Number with comma separators
   - Trend: Month-over-month percentage change

2. **Total Paid Amount**
   - Definition: Sum of all paid claim amounts
   - Source: `analytics.fct_claims`
   - Calculation: `SUM(paid_amount)`
   - Format: Currency ($)
   - Trend: Quarter-over-quarter percentage change

3. **Denial Rate**
   - Definition: Percentage of claims that were denied
   - Source: `analytics.fct_claims`
   - Calculation: `COUNT(DISTINCT claim_id WHERE claim_status = 'DENIED') / COUNT(DISTINCT claim_id) * 100`
   - Format: Percentage (%)
   - Trend: Year-over-year comparison
   - Threshold Alert: > 15% (Red), 10-15% (Yellow), < 10% (Green)

4. **Active Providers**
   - Definition: Number of providers with claims in the period
   - Source: `analytics.mart_provider_performance`
   - Calculation: `COUNT(DISTINCT provider_id)`
   - Format: Number
   - Trend: Month-over-month change

5. **Active Members**
   - Definition: Number of unique members with claims
   - Source: `analytics.fct_claims`
   - Calculation: `COUNT(DISTINCT member_sk)`
   - Format: Number
   - Trend: Month-over-month change

### Supporting Metrics
- **Average Claim Amount**: `AVG(claim_amount)`
- **Pending Claims**: `COUNT(DISTINCT claim_id WHERE claim_status = 'PENDING')`
- **Processing Time**: `AVG(processing_delay_days)`
- **High-Value Claims**: `COUNT(DISTINCT claim_id WHERE claim_amount > 1000)`

## Provider Performance Dashboard KPIs

### Provider-Level Metrics
1. **Claim Volume**
   - Total claims per provider
   - Claims per member ratio
   - Year-over-year growth

2. **Financial Performance**
   - Total claim amount
   - Average claim amount
   - Payment rate (%)

3. **Quality Metrics**
   - Denial rate
   - Adjusted claim rate
   - Processing delay days

4. **Patient Mix**
   - Unique member count
   - Member age distribution
   - Geographic distribution

### Provider Rankings
- **Top 10 by Claim Volume**
- **Top 10 by Claim Amount**
- **Highest Denial Rates**
- **Best Payment Rates**

## Denial Analysis Dashboard KPIs

### Denial Metrics
1. **Denial Volume**
   - Total denied claims
   - Total denied amount
   - Denial rate by period

2. **Denial Reasons**
   - Top denial reasons
   - Denial reason trends
   - Impact by reason

3. **Provider Denial Analysis**
   - Providers with highest denial rates
   - Denial rate by specialty
   - Regional denial patterns

4. **Payer Analysis**
   - Denial rates by payer type
   - Denial amounts by payer
   - Payer-specific trends

## Member Utilization Dashboard KPIs

### Utilization Metrics
1. **Member Activity**
   - Active members
   - Claims per member
   - Average member spend

2. **Demographics**
   - Age group utilization
   - Gender distribution
   - Geographic utilization

3. **Service Patterns**
   - Service type utilization
   - Seasonal patterns
   - High-utilization members

4. **Cost Analysis**
   - Member cost distribution
   - High-cost members
   - Cost trends by demographics

## Anomaly Detection Dashboard KPIs

### Anomaly Metrics
1. **Anomaly Volume**
   - Total anomalies detected
   - Anomaly rate (% of total claims)
   - High-risk anomalies

2. **Anomaly Types**
   - Duplicate claims
   - Amount outliers
   - Suspicious patterns

3. **Risk Assessment**
   - High-risk providers
   - High-risk members
   - Investigation queue

4. **Trend Analysis**
   - Anomaly trends over time
   - Geographic hotspots
   - Specialty-specific patterns

## Data Freshness Indicators

### Real-time Indicators
- **Last Data Update**: Timestamp of last pipeline run
- **Stream Status**: Event Hubs processing status
- **Data Quality**: Quality check pass rate
- **Pipeline Health**: Overall system health status

### Alert Thresholds
- **Data Latency**: > 2 hours (Alert)
- **Quality Failures**: > 5% (Alert)
- **Stream Failures**: Any failure (Critical)
- **Pipeline Failures**: Any failure (Critical)

## Calculation Formulas

### Key Formulas
```sql
-- Denial Rate
denial_rate = (denied_claims / total_claims) * 100

-- Payment Rate
payment_rate = (paid_amount / total_claim_amount) * 100

-- Average Processing Delay
avg_processing_delay = AVG(processing_delay_days)

-- Member Utilization Rate
utilization_rate = (claims_per_member / time_period_days)

-- Provider Performance Score
performance_score = 
  (payment_rate * 0.4) + 
  ((1 - denial_rate) * 0.3) + 
  (volume_score * 0.2) + 
  (quality_score * 0.1)
```

## Visual Design Guidelines

### Color Schemes
- **Green**: Good performance, within targets
- **Yellow**: Warning, approaching limits
- **Red**: Poor performance, exceeds thresholds
- **Blue**: Neutral information
- **Gray**: Historical/comparison data

### Chart Types
- **KPI Cards**: Large numbers with trend indicators
- **Line Charts**: Time series trends
- **Bar Charts**: Comparisons and rankings
- **Pie Charts**: Distribution analysis
- **Heat Maps**: Geographic and pattern analysis
- **Scatter Plots**: Correlation analysis

### Interactivity
- **Date Range Selection**: All dashboards support date filtering
- **Drill-through**: Click on any element for detailed analysis
- **Cross-filtering**: Selections filter all related visuals
- **Tooltips**: Detailed information on hover
- **Export Options**: PDF, Excel, and image export
