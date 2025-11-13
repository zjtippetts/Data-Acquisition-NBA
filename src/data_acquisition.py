import requests  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
import pandas as pd
import time
from pathlib import Path
from typing import Optional
import urllib.request
import gzip


class BasketballReferenceScraper:
    """Scraper for basketball-reference.com NBA statistics."""
    
    def __init__(self):
        """Initialize scraper with appropriate headers."""
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        self.session.headers.update(self.headers)
        self.base_url = "https://www.basketball-reference.com"
        self.delay = 2  # Delay between requests (seconds)
    
    def fetch_html(self, url: str, save_path: Optional[str] = None) -> str:
        """Fetch HTML content from URL and optionally save it."""
        print(f"Fetching: {url}")
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            html_content = response.text
            
            # Save HTML if path provided
            if save_path:
                Path(save_path).parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                print(f"HTML saved to: {save_path}")
            
            return html_content
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            raise
    
    def _extract_player_links(self, html_content: str, table_id: str = None) -> pd.Series:
        """Extract player links from HTML content. Returns Series of full player URLs."""
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Try to find the table - check multiple possible IDs
        table = None
        if table_id:
            table = soup.find('table', id=table_id)
        else:
            # Try common table IDs
            for tid in ['per_game_stats', 'advanced']:
                table = soup.find('table', id=tid)
                if table:
                    break
        
        if table is None:
            return pd.Series(dtype=str)
        
        player_links = []
        # Find all player links in the table (links to /players/ pages)
        # These are in td elements with data-stat="player" or in th elements
        all_links = table.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            # Check if this is a player link
            if '/players/' in href and href.endswith('.html'):
                # Construct full URL
                if href.startswith('/'):
                    full_url = f"{self.base_url}{href}"
                else:
                    full_url = href
                player_links.append(full_url)
        
        return pd.Series(player_links)
    
    def scrape_per_game_stats_from_url(self, season: str = "2025", save_html: bool = True) -> pd.DataFrame:
        """Scrape per game statistics from basketball-reference.com. Returns DataFrame with Player_URL column."""
        url = f"{self.base_url}/leagues/NBA_{season}_per_game.html"
        html_content = None
        
        try:
            # Try using pandas read_html directly (sometimes works better)
            print(f"Attempting to read table directly from URL for season {season}...")
            df = pd.read_html(url, attrs={'id': 'per_game_stats'})[0]
            
            # Try to get HTML for extracting links
            html_content = None
            try:
                html_content = self.fetch_html(url, save_path=f"data/raw/nba_{season}_per_game.html" if save_html else None)
            except requests.exceptions.RequestException:
                print("Could not fetch HTML via requests, trying urllib...")
                # Try urllib as alternative (sometimes works when requests is blocked)
                try:
                    req = urllib.request.Request(url, headers=self.headers)
                    with urllib.request.urlopen(req, timeout=10) as response:
                        data = response.read()
                        # Check if content is gzipped by looking at first bytes
                        if data[:2] == b'\x1f\x8b':  # Gzip magic number
                            html_content = gzip.decompress(data).decode('utf-8')
                        else:
                            html_content = data.decode('utf-8')
                    print("Successfully fetched HTML using urllib")
                except Exception as e:
                    print(f"Could not fetch HTML for link extraction: {e}")
                    # Try a simpler request without saving
                    try:
                        response = self.session.get(url, timeout=10)
                        if response.status_code == 200:
                            html_content = response.text
                    except Exception:
                        pass
            
        except Exception as e:
            print(f"Direct URL read failed: {e}")
            print("Trying alternative method...")
            
            # Alternative: fetch HTML and parse
            html_path = f"data/raw/nba_{season}_per_game.html"
            html_content = self.fetch_html(url, save_path=html_path if save_html else None)
            
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Find the stats table
            table = soup.find('table', id='per_game_stats')
            
            if table is None:
                raise ValueError("Could not find the per game stats table on the page")
            
            # Use pandas to read the HTML table
            df = pd.read_html(str(table))[0]
        
        # Extract player links if we have HTML content
        if html_content:
            print("Extracting player links...")
            player_links = self._extract_player_links(html_content)
            
            # Add player links to dataframe
            # Handle case where we might have header rows or different number of rows
            if len(player_links) > 0:
                # Filter out header rows (they typically have 'Rk' column as string or NaN)
                # Player links should align with actual player rows
                df['Player_URL'] = None
                
                # Find where actual data starts (skip header rows)
                start_idx = 0
                for idx, row in df.iterrows():
                    # Check if this looks like a header row
                    if pd.isna(row.get('Rk')) or str(row.get('Rk')).strip() == 'Rk':
                        start_idx = idx + 1
                        continue
                    break
                
                # Assign links starting from data rows
                link_idx = 0
                for idx in range(start_idx, len(df)):
                    if link_idx < len(player_links):
                        df.loc[idx, 'Player_URL'] = player_links.iloc[link_idx]
                        link_idx += 1
        else:
            print("Warning: Could not extract player links (HTML not available)")
            df['Player_URL'] = None
        
        # Add season column
        df['Season'] = season
        
        # Add delay between requests (good scraping practice)
        time.sleep(self.delay)
        
        return df
    
    def scrape_advanced_stats_from_url(self, season: str = "2025", save_html: bool = True) -> pd.DataFrame:
        """Scrape advanced statistics from basketball-reference.com. Returns DataFrame with Player_URL column."""
        url = f"{self.base_url}/leagues/NBA_{season}_advanced.html"
        html_content = None
        
        try:
            # Try using pandas read_html directly (sometimes works better)
            print(f"Attempting to read advanced stats table directly from URL for season {season}...")
            df = pd.read_html(url, attrs={'id': 'advanced'})[0]
            
            # Try to get HTML for extracting links
            html_content = None
            try:
                html_content = self.fetch_html(url, save_path=f"data/raw/nba_{season}_advanced.html" if save_html else None)
            except requests.exceptions.RequestException:
                print("Could not fetch HTML via requests, trying urllib...")
                # Try urllib as alternative (sometimes works when requests is blocked)
                try:
                    req = urllib.request.Request(url, headers=self.headers)
                    with urllib.request.urlopen(req, timeout=10) as response:
                        data = response.read()
                        # Check if content is gzipped by looking at first bytes
                        if data[:2] == b'\x1f\x8b':  # Gzip magic number
                            html_content = gzip.decompress(data).decode('utf-8')
                        else:
                            html_content = data.decode('utf-8')
                    print("Successfully fetched HTML using urllib")
                except Exception as e:
                    print(f"Could not fetch HTML for link extraction: {e}")
                    # Try a simpler request without saving
                    try:
                        response = self.session.get(url, timeout=10)
                        if response.status_code == 200:
                            html_content = response.text
                    except Exception:
                        pass
            
        except Exception as e:
            print(f"Direct URL read failed: {e}")
            print("Trying alternative method with urllib...")
            
            # Alternative: use urllib to fetch HTML and parse
            try:
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req, timeout=10) as response:
                    data = response.read()
                    # Check if content is gzipped by looking at first bytes
                    if data[:2] == b'\x1f\x8b':  # Gzip magic number
                        html_content = gzip.decompress(data).decode('utf-8')
                    else:
                        html_content = data.decode('utf-8')
                
                # Save HTML if requested
                if save_html:
                    html_path = f"data/raw/nba_{season}_advanced.html"
                    Path(html_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    print(f"HTML saved to: {html_path}")
                
                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(html_content, 'lxml')
                
                # Find the stats table
                table = soup.find('table', id='advanced')
                
                if table is None:
                    raise ValueError("Could not find the advanced stats table on the page")
                
                # Use pandas to read the HTML table
                df = pd.read_html(str(table))[0]
            except Exception as e2:
                raise Exception(f"Both methods failed. Last error: {e2}")
        
        # Extract player links if we have HTML content
        if html_content:
            print("Extracting player links...")
            player_links = self._extract_player_links(html_content, table_id='advanced')
            
            # Add player links to dataframe
            if len(player_links) > 0:
                df['Player_URL'] = None
                
                # Find where actual data starts (skip header rows)
                start_idx = 0
                for idx, row in df.iterrows():
                    # Check if this looks like a header row
                    if pd.isna(row.get('Rk')) or str(row.get('Rk')).strip() == 'Rk':
                        start_idx = idx + 1
                        continue
                    break
                
                # Assign links starting from data rows
                link_idx = 0
                for idx in range(start_idx, len(df)):
                    if link_idx < len(player_links):
                        df.loc[idx, 'Player_URL'] = player_links.iloc[link_idx]
                        link_idx += 1
        else:
            print("Warning: Could not extract player links (HTML not available)")
            df['Player_URL'] = None
        
        # Add season column
        df['Season'] = season
        
        # Add delay between requests (good scraping practice)
        time.sleep(self.delay)
        
        return df
    
    def scrape_per_game_stats_from_file(self, html_filepath: str, season: Optional[str] = None) -> pd.DataFrame:
        """Scrape per game statistics from saved HTML file. Returns DataFrame with Player_URL column."""
        print(f"Reading from saved HTML file: {html_filepath}")
        
        with open(html_filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Find the stats table
        table = soup.find('table', id='per_game_stats')
        
        if table is None:
            raise ValueError("Could not find the per game stats table in the HTML file")
        
        # Use pandas to read the HTML table
        df = pd.read_html(str(table))[0]
        
        # Extract player links
        print("Extracting player links...")
        player_links = self._extract_player_links(html_content)
        
        # Add player links to dataframe
        if len(player_links) > 0:
            df['Player_URL'] = None
            start_idx = 0
            for idx, row in df.iterrows():
                if pd.isna(row.get('Rk')) or str(row.get('Rk')).strip() == 'Rk':
                    start_idx = idx + 1
                    continue
                break
            
            link_idx = 0
            for idx in range(start_idx, len(df)):
                if link_idx < len(player_links):
                    df.loc[idx, 'Player_URL'] = player_links.iloc[link_idx]
                    link_idx += 1
        else:
            df['Player_URL'] = None
        
        # Add season if provided
        if season:
            df['Season'] = season
        
        return df
    
    def save_raw_data(self, data: pd.DataFrame, filename: str):
        """Save raw data to data/raw/ directory as CSV."""
        filepath = f"data/raw/{filename}"
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")


def main():
    """Main data acquisition pipeline. Scrapes data for 2021-2025."""
    scraper = BasketballReferenceScraper()
    
    # Years to scrape (past 5 years)
    years = ["2021", "2022", "2023", "2024", "2025"]
    
    print("=" * 60)
    print("Scraping NBA per game statistics for past 5 years")
    print("=" * 60)
    print(f"Years: {', '.join(years)}\n")
    
    all_dataframes = []
    failed_years = []
    
    for year in years:
        print(f"\n{'='*60}")
        print(f"Processing season {year}...")
        print(f"{'='*60}")
        
        try:
            df = scraper.scrape_per_game_stats_from_url(season=year, save_html=False)
            print(f"✓ Successfully scraped {len(df)} rows for season {year}")
            all_dataframes.append(df)
        except Exception as e:
            print(f"✗ Failed to scrape season {year}: {e}")
            failed_years.append(year)
            continue
    
    if not all_dataframes:
        print("\n" + "=" * 60)
        print("ERROR: Could not scrape any data")
        print("=" * 60)
        print("\nAlternative: Use manually saved HTML files")
        print("You can save HTML files and use:")
        print("  scraper.scrape_per_game_stats_from_file('data/raw/nba_2021_per_game.html', season='2021')")
        return
    
    # Combine all years into one dataframe
    print(f"\n{'='*60}")
    print("Combining data from all seasons...")
    print(f"{'='*60}")
    
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Reorder columns to put Season and Player_URL near the beginning
    cols = combined_df.columns.tolist()
    priority_cols = ['Rk', 'Player', 'Player_URL', 'Season', 'Age', 'Team', 'Pos']
    other_cols = [c for c in cols if c not in priority_cols]
    combined_df = combined_df[priority_cols + other_cols]
    
    print(f"✓ Combined dataset: {len(combined_df)} total rows")
    print(f"  Seasons included: {sorted(combined_df['Season'].unique())}")
    
    if failed_years:
        print(f"  Warning: Failed to scrape {len(failed_years)} year(s): {', '.join(failed_years)}")
    
    # Display results
    print(f"\nColumns ({len(combined_df.columns)}): {list(combined_df.columns)}")
    print("\nFirst few rows:")
    print(combined_df.head(10))
    
    # Show summary by season
    print(f"\n{'='*60}")
    print("Summary by Season:")
    print(f"{'='*60}")
    season_summary = combined_df.groupby('Season').size().reset_index(name='Count')
    print(season_summary.to_string(index=False))
    
    # Save the scraped data
    scraper.save_raw_data(combined_df, "nba_per_game_2021_2025_raw.csv")
    
    print("\n" + "=" * 60)
    print("✓ Data acquisition complete!")
    print("=" * 60)
    print("Data saved to: data/raw/nba_per_game_2021_2025_raw.csv")
    
    # Note about player links
    if 'Player_URL' in combined_df.columns:
        if combined_df['Player_URL'].isna().all() or (combined_df['Player_URL'].isnull().all()):
            print("\nNote: Player URLs could not be extracted due to website restrictions.")
            print("The data table was successfully scraped, but HTML access was blocked.")
            print("Player links can be manually added later if needed.")
    
    # ====================================================================
    # Scrape Advanced Stats
    # ====================================================================
    print("\n\n" + "=" * 60)
    print("Scraping NBA advanced statistics for past 5 years")
    print("=" * 60)
    print(f"Years: {', '.join(years)}\n")
    
    all_advanced_dataframes = []
    failed_advanced_years = []
    
    for year in years:
        print(f"\n{'='*60}")
        print(f"Processing advanced stats for season {year}...")
        print(f"{'='*60}")
        
        try:
            df_advanced = scraper.scrape_advanced_stats_from_url(season=year, save_html=False)
            print(f"✓ Successfully scraped {len(df_advanced)} rows for season {year}")
            all_advanced_dataframes.append(df_advanced)
        except Exception as e:
            print(f"✗ Failed to scrape advanced stats for season {year}: {e}")
            failed_advanced_years.append(year)
            continue
    
    if not all_advanced_dataframes:
        print("\n" + "=" * 60)
        print("WARNING: Could not scrape any advanced stats data")
        print("=" * 60)
    else:
        # Combine all years into one dataframe
        print(f"\n{'='*60}")
        print("Combining advanced stats from all seasons...")
        print(f"{'='*60}")
        
        combined_advanced_df = pd.concat(all_advanced_dataframes, ignore_index=True)
        
        # Reorder columns to put Season and Player_URL near the beginning
        cols = combined_advanced_df.columns.tolist()
        priority_cols = ['Rk', 'Player', 'Player_URL', 'Season', 'Age', 'Team', 'Pos']
        other_cols = [c for c in cols if c not in priority_cols]
        combined_advanced_df = combined_advanced_df[priority_cols + other_cols]
        
        print(f"✓ Combined advanced stats dataset: {len(combined_advanced_df)} total rows")
        print(f"  Seasons included: {sorted(combined_advanced_df['Season'].unique())}")
        
        if failed_advanced_years:
            print(f"  Warning: Failed to scrape {len(failed_advanced_years)} year(s): {', '.join(failed_advanced_years)}")
        
        # Display results
        print(f"\nColumns ({len(combined_advanced_df.columns)}): {list(combined_advanced_df.columns)}")
        print("\nFirst few rows:")
        print(combined_advanced_df.head(10))
        
        # Show summary by season
        print(f"\n{'='*60}")
        print("Advanced Stats Summary by Season:")
        print(f"{'='*60}")
        season_summary_advanced = combined_advanced_df.groupby('Season').size().reset_index(name='Count')
        print(season_summary_advanced.to_string(index=False))
        
        # Save the scraped advanced stats data
        scraper.save_raw_data(combined_advanced_df, "nba_advanced_2021_2025_raw.csv")
        
        print("\n" + "=" * 60)
        print("✓ Advanced stats data acquisition complete!")
        print("=" * 60)
        print("Data saved to: data/raw/nba_advanced_2021_2025_raw.csv")


if __name__ == "__main__":
    main()
