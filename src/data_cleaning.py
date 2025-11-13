import pandas as pd
from pathlib import Path


def load_raw_data(filename: str) -> pd.DataFrame:
    """Load raw data from data/raw/ directory."""
    filepath = f"data/raw/{filename}"
    return pd.read_csv(filepath)


def remove_league_average(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where Player is 'League Average'."""
    df_clean = df.copy()
    initial_count = len(df_clean)
    
    # Remove League Average rows
    df_clean = df_clean[df_clean['Player'] != 'League Average'].copy()
    
    removed_count = initial_count - len(df_clean)
    if removed_count > 0:
        print(f"Removed {removed_count} 'League Average' row(s)")
    
    return df_clean


def parse_awards_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse Awards column into individual columns.
    Awards with numbers (e.g., "MVP-3") become "MVP_VOTING" with the number.
    Awards without numbers (e.g., "AS") become "AS" with "yes" if present.
    """
    df_parsed = df.copy()
    
    if 'Awards' not in df_parsed.columns:
        print("No 'Awards' column found, skipping award parsing")
        return df_parsed
    
    # Get all unique awards across all rows
    all_awards = set()
    for awards_str in df_parsed['Awards'].dropna():
        if pd.notna(awards_str) and str(awards_str).strip():
            # Split by comma and space
            awards_list = [a.strip() for a in str(awards_str).split(',')]
            for award in awards_list:
                if award:
                    # Extract base award name (before dash if present)
                    if '-' in award:
                        base_award = award.split('-')[0]
                        all_awards.add(base_award)
                    else:
                        all_awards.add(award)
    
    print(f"\nFound {len(all_awards)} unique award types: {sorted(all_awards)}")
    
    # Create columns for awards without voting numbers
    simple_awards = []
    voting_awards = []
    
    for award in all_awards:
        # Check if any row has this award with a number
        has_voting = False
        for awards_str in df_parsed['Awards'].dropna():
            if pd.notna(awards_str):
                awards_list = [a.strip() for a in str(awards_str).split(',')]
                for a in awards_list:
                    if a.startswith(award + '-'):
                        has_voting = True
                        break
                if has_voting:
                    break
        
        if has_voting:
            voting_awards.append(award)
        else:
            simple_awards.append(award)
    
    # Initialize all award columns
    for award in simple_awards:
        df_parsed[award] = ''
    
    for award in voting_awards:
        df_parsed[f"{award}_VOTING"] = None
    
    # Fill in the values
    for idx, row in df_parsed.iterrows():
        awards_str = row.get('Awards')
        
        if pd.notna(awards_str) and str(awards_str).strip():
            awards_list = [a.strip() for a in str(awards_str).split(',')]
            
            for award_entry in awards_list:
                if not award_entry:
                    continue
                
                # Check if it has a voting number
                if '-' in award_entry:
                    parts = award_entry.split('-')
                    base_award = parts[0]
                    voting_num = parts[1] if len(parts) > 1 else None
                    
                    if base_award in voting_awards and voting_num:
                        try:
                            # Convert to integer
                            voting_value = int(voting_num)
                            df_parsed.loc[idx, f"{base_award}_VOTING"] = voting_value
                        except ValueError:
                            # If conversion fails, just mark as present
                            df_parsed.loc[idx, f"{base_award}_VOTING"] = voting_num
                else:
                    # Simple award without voting
                    if award_entry in simple_awards:
                        df_parsed.loc[idx, award_entry] = 'yes'
    
    print(f"Created {len(simple_awards)} simple award columns and {len(voting_awards)} voting award columns")
    
    # Drop the original Awards column since we've parsed it into individual columns
    df_parsed = df_parsed.drop(columns=['Awards'])
    print("Dropped original 'Awards' column")
    
    return df_parsed


def handle_multi_team_players(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only total row (2TM, 3TM, etc.) for multi-team players.
    Updates Team column to include team codes, e.g., "2TM (HOU, BRK)".
    """
    df_clean = df.copy()
    initial_count = len(df_clean)
    
    # Group by Player_URL and Season
    groups = df_clean.groupby(['Player_URL', 'Season'])
    
    rows_to_keep = []
    rows_to_drop = []
    
    for (player_url, season), group in groups:
        if len(group) == 1:
            # Only one row, keep it
            rows_to_keep.append(group.index[0])
        else:
            # Multiple rows - check for TM row
            tm_row = None
            individual_teams = []
            
            for idx, row in group.iterrows():
                team = str(row['Team']) if pd.notna(row['Team']) else ''
                if 'TM' in team:
                    tm_row = idx
                else:
                    # Individual team row
                    if team and team not in individual_teams:
                        individual_teams.append(team)
            
            if tm_row is not None:
                # Found TM row - update it with team codes
                tm_team_value = df_clean.loc[tm_row, 'Team']
                if individual_teams:
                    # Update Team to include team codes
                    team_codes = ', '.join(sorted(individual_teams))
                    df_clean.loc[tm_row, 'Team'] = f"{tm_team_value} ({team_codes})"
                
                rows_to_keep.append(tm_row)
                # Mark individual team rows for deletion
                for idx in group.index:
                    if idx != tm_row:
                        rows_to_drop.append(idx)
            else:
                # No TM row found - keep the first one (or could keep the one with most games)
                # For now, keep the first row
                rows_to_keep.append(group.index[0])
                for idx in group.index[1:]:
                    rows_to_drop.append(idx)
    
    # Drop the individual team rows
    df_clean = df_clean.drop(index=rows_to_drop)
    
    removed_count = initial_count - len(df_clean)
    if removed_count > 0:
        print(f"Removed {removed_count} duplicate player-season row(s) (kept total rows for multi-team players)")
    
    return df_clean


def merge_per_game_and_advanced(per_game_df: pd.DataFrame, advanced_df: pd.DataFrame) -> pd.DataFrame:
    """Merge per game stats and advanced stats dataframes."""
    # Remove League Average from both
    per_game_clean = remove_league_average(per_game_df)
    advanced_clean = remove_league_average(advanced_df)
    
    # Remove Awards column from advanced stats (per game already has it)
    if 'Awards' in advanced_clean.columns:
        advanced_clean = advanced_clean.drop(columns=['Awards'])
        print("Removed 'Awards' column from advanced stats")
    
    # Identify columns that appear in both dataframes (excluding merge keys)
    merge_keys = ['Player_URL', 'Season', 'Team']
    per_game_cols = set(per_game_clean.columns)
    advanced_cols = set(advanced_clean.columns)
    
    # Columns that are in both (excluding merge keys)
    common_cols = (per_game_cols & advanced_cols) - set(merge_keys)
    
    # For merge, we'll keep columns from per_game and add unique columns from advanced
    # Columns that are only in advanced (excluding merge keys)
    advanced_only_cols = advanced_cols - per_game_cols - set(merge_keys)
    
    print(f"\nCommon columns (will be deduplicated): {sorted(common_cols)}")
    print(f"Advanced-only columns (will be added): {sorted(advanced_only_cols)}")
    
    # Merge on Player_URL, Season, and Team
    # Use suffixes to handle duplicate column names, but we'll drop duplicates after
    merged_df = pd.merge(
        per_game_clean,
        advanced_clean,
        on=merge_keys,
        how='inner',  # Only keep rows that exist in both
        suffixes=('_per_game', '_advanced')
    )
    
    print(f"\nMerged dataset: {len(merged_df)} rows")
    print(f"  Per game rows: {len(per_game_clean)}")
    print(f"  Advanced rows: {len(advanced_clean)}")
    
    # Handle duplicate columns - keep per_game version for common columns
    # (except for the merge keys which are already handled)
    for col in common_cols:
        per_game_col = f"{col}_per_game"
        advanced_col = f"{col}_advanced"
        
        if per_game_col in merged_df.columns and advanced_col in merged_df.columns:
            # Keep per_game version, drop advanced version
            merged_df = merged_df.drop(columns=[advanced_col])
            # Rename per_game version back to original name
            merged_df = merged_df.rename(columns={per_game_col: col})
    
    # Parse awards column into individual columns
    print(f"\n{'='*60}")
    print("Parsing Awards column...")
    print(f"{'='*60}")
    merged_df = parse_awards_column(merged_df)
    
    # Handle multi-team players (keep only total rows)
    print(f"\n{'='*60}")
    print("Handling multi-team players...")
    print(f"{'='*60}")
    merged_df = handle_multi_team_players(merged_df)
    
    # Remove Rk column
    if 'Rk' in merged_df.columns:
        merged_df = merged_df.drop(columns=['Rk'])
        print("Removed 'Rk' column")
    
    return merged_df


def save_processed_data(df: pd.DataFrame, filename: str):
    """Save processed data to data/processed/ directory as CSV."""
    filepath = f"data/processed/{filename}"
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"Processed data saved to {filepath}")


def main():
    """Main data cleaning pipeline."""
    print("=" * 60)
    print("NBA Data Cleaning Pipeline")
    print("=" * 60)
    
    # Load raw data
    print("\nLoading raw data...")
    per_game_df = load_raw_data("nba_per_game_2021_2025_raw.csv")
    advanced_df = load_raw_data("nba_advanced_2021_2025_raw.csv")
    
    print(f"Per game stats: {len(per_game_df)} rows, {len(per_game_df.columns)} columns")
    print(f"Advanced stats: {len(advanced_df)} rows, {len(advanced_df.columns)} columns")
    
    # Merge the datasets
    print("\n" + "=" * 60)
    print("Merging datasets...")
    print("=" * 60)
    merged_df = merge_per_game_and_advanced(per_game_df, advanced_df)
    
    # Display summary
    print(f"\n{'='*60}")
    print("Merged Dataset Summary")
    print(f"{'='*60}")
    print(f"Total rows: {len(merged_df)}")
    print(f"Total columns: {len(merged_df.columns)}")
    print(f"\nColumns: {list(merged_df.columns)}")
    print("\nFirst few rows:")
    print(merged_df.head())
    
    # Save processed data
    print(f"\n{'='*60}")
    save_processed_data(merged_df, "nba_merged_2021_2025.csv")
    print(f"{'='*60}")
    print("✓ Data cleaning complete!")


if __name__ == "__main__":
    main()

