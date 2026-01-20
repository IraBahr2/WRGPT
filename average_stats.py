import sqlite3
import argparse
from typing import Dict, List
import requests
from bs4 import BeautifulSoup
from Stats import PokerStatsCalculator, PlayerStats

class AverageStatsCalculator:
    def __init__(self, db_path: str = "poker_analysis.db"):
        """Initialize calculator with path to SQLite database."""
        self.db_path = db_path
        self.calculator = PokerStatsCalculator(db_path)
        self.conn = None

    def connect(self):
        """Establish database connection."""
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_active_players_from_standings(self, url: str = "http://www.wrgpt.org/wrgpt_standings.php") -> List[str]:
        """
        Scrape the WRGPT standings page to get list of active (non-eliminated) players.
        
        Args:
            url: URL of the WRGPT standings page
            
        Returns:
            List of active player names
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the standings table - look for rows with player data
            active_players = []
            
            # The active players have a "Status" column, eliminated players have "Table - Hand - Prize"
            # We'll look for rows that contain status indicators like "in", "folded", "AWOL", "Gone"
            rows = soup.find_all('tr')
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5:  # Should have rank, player, bankroll, table, pot, status columns
                    # Check if this looks like an active player row (has numeric rank, player name, and status)
                    rank_text = cols[0].get_text(strip=True)
                    player_name = cols[1].get_text(strip=True)
                    
                    # Active players have numeric ranks and status in later columns
                    # Eliminated players have ranks but different format
                    if rank_text.isdigit() and len(cols) >= 6:
                        status_col = cols[5].get_text(strip=True)
                        # Active players have status like "in", "folded", "AWOL", "Gone"
                        if status_col in ['in', 'folded', 'AWOL', 'Gone']:
                            active_players.append(player_name)
            
            if not active_players:
                raise ValueError("No active players found on standings page")
            
            print(f"Found {len(active_players)} active players from standings")
            return active_players
            
        except requests.RequestException as e:
            raise ValueError(f"Failed to fetch standings from {url}: {e}")
        except Exception as e:
            raise ValueError(f"Failed to parse standings page: {e}")

    def calculate_average_stats(self, use_active_only: bool = False, standings_url: str = None) -> PlayerStats:
        """
        Calculate average statistics across players.
        
        Args:
            use_active_only: If True, only include active players from standings
            standings_url: URL to fetch active players from (if use_active_only is True)
            
        Returns:
            PlayerStats object representing the average player
        """
        # Get player list based on filter
        if use_active_only:
            url = standings_url or "http://www.wrgpt.org/wrgpt_standings.php"
            players_to_analyze = self.get_active_players_from_standings(url)
        else:
            # Get all players from database
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute("SELECT DISTINCT name FROM players ORDER BY name")
            players_to_analyze = [row[0] for row in cursor.fetchall()]
            self.close()
        
        if not players_to_analyze:
            raise ValueError("No players found")
        
        # Calculate stats for all players
        all_stats = self.calculator.calculate_stats(players_to_analyze)
        
        # Filter out players with no stats (might not be in DB yet)
        qualified_stats = {
            name: stats for name, stats in all_stats.items() 
            if stats.total_hands > 0
        }
        
        if not qualified_stats:
            raise ValueError("No players found with hand history")
        
        num_players = len(qualified_stats)
        print(f"\nCalculating averages across {num_players} players")
        
        # Calculate averages
        avg_total_hands = sum(s.total_hands for s in qualified_stats.values()) / num_players
        avg_vpip_hands = sum(s.vpip_hands for s in qualified_stats.values()) / num_players
        avg_vpip_percentage = sum(s.vpip_percentage for s in qualified_stats.values()) / num_players
        avg_threeb_opportunities = sum(s.threeb_opportunities for s in qualified_stats.values()) / num_players
        avg_threeb_count = sum(s.threeb_count for s in qualified_stats.values()) / num_players
        avg_river_reached = sum(s.river_reached for s in qualified_stats.values()) / num_players
        avg_showdown_count = sum(s.showdown_count for s in qualified_stats.values()) / num_players
        avg_showdown_percentage = sum(s.showdown_percentage for s in qualified_stats.values()) / num_players
        avg_won_at_showdown = sum(s.won_at_showdown for s in qualified_stats.values()) / num_players
        avg_wtsd_percentage = sum(s.wtsd_percentage for s in qualified_stats.values()) / num_players
        avg_w_sd_percentage = sum(s.w_sd_percentage for s in qualified_stats.values()) / num_players
        avg_rfi_opportunities = sum(s.rfi_opportunities for s in qualified_stats.values()) / num_players
        avg_rfi_count = sum(s.rfi_count for s in qualified_stats.values()) / num_players
        avg_rfi_percentage = sum(s.rfi_percentage for s in qualified_stats.values()) / num_players
        avg_steal_opportunities = sum(s.steal_opportunities for s in qualified_stats.values()) / num_players
        avg_steal_attempts = sum(s.steal_attempts for s in qualified_stats.values()) / num_players
        avg_iso_opportunities = sum(s.iso_opportunities for s in qualified_stats.values()) / num_players
        avg_iso_attempts = sum(s.iso_attempts for s in qualified_stats.values()) / num_players
        avg_iso_percentage = sum(s.iso_percentage for s in qualified_stats.values()) / num_players
        
        # Create average PlayerStats object
        avg_stats = PlayerStats(
            name="Average Player",
            total_hands=int(round(avg_total_hands)),
            vpip_hands=int(round(avg_vpip_hands)),
            vpip_percentage=round(avg_vpip_percentage, 1),
            threeb_opportunities=int(round(avg_threeb_opportunities)),
            threeb_count=int(round(avg_threeb_count)),
            river_reached=int(round(avg_river_reached)),
            showdown_count=int(round(avg_showdown_count)),
            showdown_percentage=round(avg_showdown_percentage, 1),
            won_at_showdown=int(round(avg_won_at_showdown)),
            wtsd_percentage=round(avg_wtsd_percentage, 1),
            w_sd_percentage=round(avg_w_sd_percentage, 1),
            rfi_opportunities=int(round(avg_rfi_opportunities)),
            rfi_count=int(round(avg_rfi_count)),
            rfi_percentage=round(avg_rfi_percentage, 1),
            steal_opportunities=int(round(avg_steal_opportunities)),
            steal_attempts=int(round(avg_steal_attempts)),
            iso_opportunities=int(round(avg_iso_opportunities)),
            iso_attempts=int(round(avg_iso_attempts)),
            iso_percentage=round(avg_iso_percentage, 1),
            showdown_details=None,  # No details for average
            rfi_details=None  # No details for average
        )
        
        return avg_stats, qualified_stats

def print_average_stats(stats: PlayerStats):
    """Pretty print average statistics (without detail sections)."""
    print("\nAverage Player Statistics:")
    print("-" * 130)
    print(f"{'Player':<16} {'Hands':<8} {'VPIP':<8} {'3Bet':<8} {'RFI':<8} {'ISO':<8} {'Steal':<10} {'WTSD':<8} {'Won@SD':<8} {'ShowDn':<8} {'Rivers':<8}")
    print("-" * 130)
    
    threeb_display = f"{stats.threeb_count}/{stats.threeb_opportunities}"
    steal_display = f"{stats.steal_attempts}/{stats.steal_opportunities}"
    
    print(f"{stats.name:<16} "
          f"{stats.total_hands:<8} "
          f"{stats.vpip_percentage:<8.1f} "
          f"{threeb_display:<8} "
          f"{stats.rfi_percentage:<8.1f} "
          f"{stats.iso_percentage:<8.1f} "
          f"{steal_display:<10} "
          f"{stats.wtsd_percentage:<8.1f} "
          f"{stats.w_sd_percentage:<8.1f} "
          f"{stats.showdown_count:<8} "
          f"{stats.river_reached:<8}")
    print()

def print_top_players(stats_dict: Dict[str, PlayerStats], num_players: int = 10):
    """Print full statistics for top N players by VPIP."""
    # Sort players by VPIP percentage (descending)
    sorted_players = sorted(stats_dict.items(), key=lambda x: x[1].vpip_percentage, reverse=True)
    top_players = sorted_players[:num_players]
    
    print(f"\nTop {len(top_players)} Players by VPIP:")
    print("-" * 130)
    print(f"{'Player':<16} {'Hands':<8} {'VPIP':<8} {'3Bet':<8} {'RFI':<8} {'ISO':<8} {'Steal':<10} {'WTSD':<8} {'Won@SD':<8} {'ShowDn':<8} {'Rivers':<8}")
    print("-" * 130)
    
    for name, player_stats in top_players:
        threeb_display = f"{player_stats.threeb_count}/{player_stats.threeb_opportunities}"
        steal_display = f"{player_stats.steal_attempts}/{player_stats.steal_opportunities}"
        
        print(f"{player_stats.name:<16} "
              f"{player_stats.total_hands:<8} "
              f"{player_stats.vpip_percentage:<8.1f} "
              f"{threeb_display:<8} "
              f"{player_stats.rfi_percentage:<8.1f} "
              f"{player_stats.iso_percentage:<8.1f} "
              f"{steal_display:<10} "
              f"{player_stats.wtsd_percentage:<8.1f} "
              f"{player_stats.w_sd_percentage:<8.1f} "
              f"{player_stats.showdown_count:<8} "
              f"{player_stats.river_reached:<8}")
    print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calculate average poker statistics across players')
    parser.add_argument('--active-only', action='store_true',
                        help='Only include active (non-eliminated) players from WRGPT standings')
    parser.add_argument('--standings-url', default='http://www.wrgpt.org/wrgpt_standings.php',
                        help='URL of the WRGPT standings page (default: http://www.wrgpt.org/wrgpt_standings.php)')
    parser.add_argument('--db', default='poker_analysis.db', 
                        help='Path to the database file (default: poker_analysis.db)')
    
    args = parser.parse_args()
    
    try:
        calculator = AverageStatsCalculator(db_path=args.db)
        avg_stats, qualified_stats = calculator.calculate_average_stats(
            use_active_only=args.active_only,
            standings_url=args.standings_url
        )
        print_average_stats(avg_stats)
        print_top_players(qualified_stats, num_players=10)
        
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
