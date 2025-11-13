"""
Helper script to save HTML from Basketball Reference.

Since Basketball Reference blocks automated requests for HTML,
this script provides instructions for manually saving the HTML.
"""

from src.data_acquisition import BasketballReferenceScraper
from pathlib import Path

def main():
    """
    Instructions for saving HTML manually.
    """
    print("=" * 70)
    print("HTML SAVING HELPER")
    print("=" * 70)
    print("\nBasketball Reference blocks automated HTML downloads.")
    print("To save the HTML for analysis, please follow these steps:\n")
    print("1. Open your web browser")
    print("2. Navigate to: https://www.basketball-reference.com/leagues/NBA_2025_per_game.html")
    print("3. Right-click on the page and select 'Save As' (or File > Save As)")
    print("4. Save the file as: data/raw/nba_2025_per_game.html")
    print("   (Make sure to save as 'Web Page, Complete' or 'Web Page, HTML Only')")
    print("\n" + "=" * 70)
    print("After saving, you can test the HTML file with:")
    print("=" * 70)
    print("\n  from src.data_acquisition import BasketballReferenceScraper")
    print("  scraper = BasketballReferenceScraper()")
    print("  df = scraper.scrape_per_game_stats_from_file('data/raw/nba_2025_per_game.html')")
    print("  print(df.head())")
    print("\n" + "=" * 70)
    
    # Check if HTML file already exists
    html_path = Path("data/raw/nba_2025_per_game.html")
    if html_path.exists():
        print("\n✓ Found existing HTML file!")
        print(f"  Location: {html_path}")
        print("\nTesting the file...")
        
        try:
            scraper = BasketballReferenceScraper()
            df = scraper.scrape_per_game_stats_from_file(str(html_path))
            print(f"\n✓ Successfully parsed HTML file!")
            print(f"  Rows: {len(df)}")
            print(f"  Columns: {len(df.columns)}")
            print("\nFirst few rows:")
            print(df.head())
        except Exception as e:
            print(f"\n✗ Error parsing HTML file: {e}")
    else:
        print(f"\nHTML file not found at: {html_path}")
        print("Please save the HTML file manually as described above.")

if __name__ == "__main__":
    main()

