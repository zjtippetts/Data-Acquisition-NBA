# NBA Data Acquisition Project

A data science project that scrapes, cleans, and analyzes NBA player statistics from Basketball-Reference.com for the 2021-2025 seasons.

## Project Overview

This project implements a complete data pipeline:

1. **Data Acquisition**: Web scraping of NBA player statistics from Basketball-Reference.com
2. **Data Cleaning**: Merging datasets, parsing awards, and handling multi-team players
3. **Exploratory Data Analysis**: Analysis of VORP (Value Over Replacement Player) by position and statistical relationships

**Motivating Question:** How do player statistics and advanced metrics (particularly VORP) vary across different positions in the NBA?

## Project Structure

```
Data-Acquisition-NBA/
├── data/
│   ├── raw/                 # Raw scraped data
│   └── processed/           # Cleaned and merged data
├── src/
│   ├── data_acquisition.py  # Web scraping
│   └── data_cleaning.py    # Data cleaning
├── notebooks/
│   └── eda.ipynb           # Exploratory data analysis
└── figures/                # Generated visualizations
```

## Dataset Information

- **Data Source:** [Basketball-Reference.com](https://www.basketball-reference.com/)
- **Sample Size:** 2,825 player-season combinations
- **Features:** 63 columns (per-game stats, advanced metrics, awards, player metadata)
- **Time Period:** 2021-2025 NBA seasons

## Usage

### Data Acquisition
```bash
python src/data_acquisition.py
```
Scrapes per-game and advanced statistics for 2021-2025, saves to `data/raw/`.

### Data Cleaning
```bash
python src/data_cleaning.py
```
Merges datasets, parses awards, handles multi-team players, saves to `data/processed/`.

### Exploratory Data Analysis
```bash
jupyter notebook notebooks/eda.ipynb
```
Includes VORP analysis by position, outlier detection, and correlation heatmaps.

## Key Findings

- **VORP by Position**: Centers and Point Guards tend to have higher average VORP values
- **Yearly Trends**: VORP patterns remain relatively consistent across 2021-2025, although Centers have steadily rose.
- **Statistical Relationships**: Strong correlations between traditional stats and advanced metrics, with position-specific differences
- **Outliers**: Elite players identified using IQR method show significantly higher VORP than replacement-level players

## Data Sources

- **Basketball-Reference.com**: Primary source for NBA statistics

## Notes

- The scraper includes rate limiting and error handling
- Large CSV files in `data/` may not be tracked in git
- Generated figures are saved in `figures/`
