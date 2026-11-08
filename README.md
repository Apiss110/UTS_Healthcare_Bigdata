# <p align = 'center'>HealthCare Insurance Claims Analysis (2017-2018)</p>

## PROJECT OVERVIEW

This project analyzes healthcare insurance claims data from an Accountable Care Organization (ACO) associated with ABC Healthcare Company. The focus is on examining spending trends for patients enrolled in High Deductible Health Plan (HDHP) insurance products. By leveraging Tableau for visualization, this analysis aims to identify key spending drivers, compare product lines (Commercial vs. Medicare), and understand payment dynamics across different medical specialties and regions.

## DATA DESCRIPTION

The analysis works with a claims dataset covering the period from **2017 to 2018**. The data includes information on:
*   **Provider Specialties**: Classifications of medical service providers.
*   **Insurance Products**: Comparison between Commercial and Medicare plans.
*   **Financials**: Insurance payouts versus patient copayments.
*   **Temporal Data**: Monthly and quarterly timestamps for claim processing.
*   **Geography**: Patient location data (State level).

## METHODOLOGY

*   **Data Processing**: Cleaning and aggregating claims data to ensure accuracy in financial reporting.
*   **Visualization**: Using **Tableau** to create an interactive dashboard that allows for dynamic exploration of the data.
*   **Analysis**:
    *   Aggregating total spending by provider specialty to identify high-cost areas.
    *   Time-series analysis to track monthly spending trends for Medicare and Commercial products.
    *   Correlation analysis between insurance payout and patient copayment.
    *   Geospatial analysis to map spending distribution across states.

## RESULTS

The analysis yielded several key insights, visualized in the Tableau dashboard:

### 1. Top Spending Specialties
**Internal Medicine** is the leading area of expenditure, with total insurance payouts exceeding **$120K**. Other high-cost specialties include **Nurse Practitioners**, **Orthopedics**, and **Dermatology**. Behavioral Health and Gastroenterology represent lower total payouts.

### 2. Commercial vs. Medicare Trends
*   **Medicare** spending (Total: ~$266K) is slightly higher than **Commercial** spending (Total: ~$243K).
*   Both product lines show volatility, but Medicare experienced a significant spike in spending around **February 2018**, reaching approximately **$43K** in a single month.

### 3. Payment Distribution
A scatter plot analysis reveals that the majority of claims involve insurance payouts of under **$600**, distributed across a copayment range of **$0 - $40**.

### 4. Geographical & Seasonal Variations
Quarterly analysis highlights significant regional variations. For instance, **Massachusetts (MA)** saw a notable surge in insurance payouts during **Q3 2017** ($46K), indicating a localized event or seasonal increase in claims.

### Dashboard Snapshot

![Healthcare Insurance Claims Analysis Dashboard](Images/tableau_dashboard.png)

[**View Interactive Dashboard on Tableau Public**](https://public.tableau.com/app/profile/aditya.agarwal1269/viz/HealthInsuranceClaimsAnlaysis2017-2018/Dashboard1)

## CONCLUSION

This project highlights the critical drivers of insurance spending for ABC Healthcare Company. The dominance of Internal Medicine and the specific seasonal spikes in Medicare payments suggest targeted areas for cost management or audit. Furthermore, regional anomalies like the 2017 spike in Massachusetts warrant further investigation to understand underlying causes. The Tableau dashboard serves as a vital tool for continuous monitoring of these metrics.
