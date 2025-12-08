import sqlite3
from typing import Union, List, Dict
from dataclasses import dataclass
import argparse

@dataclass
class PlayerStats:
    name: str
    total_hands: int
    vpip_hands: int
    vpip_percentage: float
    threeb_opportunities: int
    threeb_count: int
    threeb_percentage: float
    river_reached: int
    showdown_count: int
    showdown_percentage: float
    won_at_showdown: int
    wtsd_percentage: float
    w_sd_percentage: float
    rfi_opportunities: int
    rfi_count: int
    rfi_percentage: float
    steal_opportunities: int  # New field
    steal_attempts: int      # New field
    steal_percentage: float  # New field
    showdown_details: List[tuple] = None

class PokerStatsCalculator:
    def __init__(self, db_path: str = "poker_analysis.db"):
        """Initialize calculator with path to SQLite database."""
        self.db_path = db_path
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

    def calculate_stats(self, players: Union[str, List[str]]) -> Dict[str, PlayerStats]:
        """
        Calculate comprehensive poker statistics for one or more players.
        """
        if isinstance(players, str):
            players = [players]
            
        self.connect()
        
        query = """
        WITH PlayerHands AS (
            -- Get all hands where player was dealt cards
            SELECT 
                p.name,
                hp.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN hands h ON hp.hand_id = h.hand_id
            WHERE p.name IN ({})
            GROUP BY p.name, hp.hand_id
        ),
        VPIPHands AS (
            -- Get hands where player voluntarily put money in
            SELECT DISTINCT
                p.name,
                hp.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a ON hp.hand_id = a.hand_id 
                AND hp.player_id = a.player_id
            WHERE p.name IN ({})
                AND a.street = 'preflop'
                AND (
                    a.action_type IN ('raise', 'call')
                    OR (a.action_type = 'bet' AND a.amount > 0)
                    OR (
                        a.action_type = 'blind'
                        AND EXISTS (
                            SELECT 1 
                            FROM actions a2 
                            WHERE a2.hand_id = a.hand_id 
                                AND a2.player_id = a.player_id
                                AND a2.street = 'preflop'
                                AND a2.action_type IN ('call', 'raise', 'check')
                        )
                    )
                )
            GROUP BY p.name, hp.hand_id
        ),
StealOpportunities AS (
            -- Hands where player could steal from BTN, CO, or HJ (using relative positions)
            SELECT DISTINCT
                p.name,
                hp.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a ON hp.hand_id = a.hand_id 
                AND hp.player_id = a.player_id
                AND a.street = 'preflop'
            JOIN (
                SELECT 
                    h.hand_id,
                    h.total_players,
                    hp_sb.position as sb_position,
                    CASE 
                        WHEN hp_sb.position - 3 < 1 THEN h.total_players - (3 - hp_sb.position)
                        ELSE hp_sb.position - 3
                    END as hj_position,
                    CASE 
                        WHEN hp_sb.position - 2 < 1 THEN h.total_players - (2 - hp_sb.position)
                        ELSE hp_sb.position - 2
                    END as co_position,
                    CASE 
                        WHEN hp_sb.position - 1 < 1 THEN h.total_players
                        ELSE hp_sb.position - 1
                    END as btn_position
                FROM hands h
                JOIN hand_players hp_sb ON h.hand_id = hp_sb.hand_id
                JOIN actions a_sb ON hp_sb.hand_id = a_sb.hand_id 
                    AND hp_sb.player_id = a_sb.player_id
                    AND a_sb.action_type = 'blind'
                WHERE a_sb.sequence_number = 1
            ) positions ON hp.hand_id = positions.hand_id
            WHERE p.name IN ({})
                AND hp.position IN (positions.hj_position, positions.co_position, positions.btn_position)
                AND NOT EXISTS (
                    SELECT 1 
                    FROM actions a2
                    WHERE a2.hand_id = a.hand_id
                        AND a2.sequence_number < a.sequence_number
                        AND a2.action_type IN ('call', 'raise')
                )
            GROUP BY p.name, hp.hand_id
        ),
        StealAttempts AS (
            -- Actual steal attempts from late position
            SELECT DISTINCT
                p.name,
                hp.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a ON hp.hand_id = a.hand_id 
                AND hp.player_id = a.player_id
                AND a.street = 'preflop'
                AND a.action_type = 'raise'
            JOIN (
                SELECT 
                    h.hand_id,
                    h.total_players,
                    hp_sb.position as sb_position,
                    CASE 
                        WHEN hp_sb.position - 3 < 1 THEN h.total_players - (3 - hp_sb.position)
                        ELSE hp_sb.position - 3
                    END as hj_position,
                    CASE 
                        WHEN hp_sb.position - 2 < 1 THEN h.total_players - (2 - hp_sb.position)
                        ELSE hp_sb.position - 2
                    END as co_position,
                    CASE 
                        WHEN hp_sb.position - 1 < 1 THEN h.total_players
                        ELSE hp_sb.position - 1
                    END as btn_position
                FROM hands h
                JOIN hand_players hp_sb ON h.hand_id = hp_sb.hand_id
                JOIN actions a_sb ON hp_sb.hand_id = a_sb.hand_id 
                    AND hp_sb.player_id = a_sb.player_id
                    AND a_sb.action_type = 'blind'
                WHERE a_sb.sequence_number = 1
            ) positions ON hp.hand_id = positions.hand_id
            WHERE p.name IN ({})
                AND hp.position IN (positions.hj_position, positions.co_position, positions.btn_position)
                AND NOT EXISTS (
                    SELECT 1 
                    FROM actions a2
                    WHERE a2.hand_id = a.hand_id
                        AND a2.sequence_number < a.sequence_number
                        AND a2.action_type IN ('call', 'raise')
                )
            GROUP BY p.name, hp.hand_id
        ),ThreeBetOpportunities AS (
            -- Hands where there was a raise before the player acted
            SELECT DISTINCT
                p.name,
                a1.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a1 ON hp.hand_id = a1.hand_id 
                AND hp.player_id = a1.player_id
                AND a1.street = 'preflop'
            WHERE p.name IN ({})
                AND EXISTS (
                    SELECT 1 
                    FROM actions a2
                    JOIN players p2 ON a2.player_id = p2.player_id
                    WHERE a2.hand_id = a1.hand_id 
                        AND p2.name != p.name
                        AND a2.sequence_number < a1.sequence_number
                        AND a2.action_type = 'raise'
                        AND a2.street = 'preflop'
                )
            GROUP BY p.name, a1.hand_id
        ),
        ThreeBets AS (
            -- Actual 3bets made by player
            SELECT DISTINCT
                p.name,
                a1.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a1 ON hp.hand_id = a1.hand_id 
                AND hp.player_id = a1.player_id
                AND a1.action_type = 'raise'
                AND a1.street = 'preflop'
            WHERE p.name IN ({})
                AND EXISTS (
                    SELECT 1 
                    FROM actions a2
                    JOIN players p2 ON a2.player_id = p2.player_id
                    WHERE a2.hand_id = a1.hand_id 
                        AND p2.name != p.name
                        AND a2.sequence_number < a1.sequence_number
                        AND a2.action_type = 'raise'
                        AND a2.street = 'preflop'
                )
            GROUP BY p.name, a1.hand_id
        ),
        RiverHands AS (
            -- Hands that reached the river
            SELECT DISTINCT
                p.name,
                CASE 
                    WHEN a.hand_id IS NOT NULL THEN a.hand_id
                    ELSE hp.hand_id
                END as hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            LEFT JOIN actions a ON hp.hand_id = a.hand_id
                AND hp.player_id = a.player_id
                AND a.street = 'river'
            WHERE p.name IN ({})
                AND (a.hand_id IS NOT NULL OR hp.cards_shown IS NOT NULL)
        ),
        Showdowns AS (
            -- Hands where cards were shown
            SELECT DISTINCT
                p.name,
                hp.hand_id,
                hp.cards_shown,
                h.board_cards,
                hp.net_result
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN hands h ON hp.hand_id = h.hand_id
            WHERE p.name IN ({})
                AND hp.cards_shown IS NOT NULL
        ),
        WonHands AS (
            -- Hands won at showdown
            SELECT 
                s.name,
                s.hand_id
            FROM Showdowns s
            JOIN hand_players hp ON s.hand_id = hp.hand_id
            JOIN players p ON hp.player_id = p.player_id
            WHERE p.name = s.name
                AND hp.net_result > 0
        ),
        RFIOpportunities AS (
            -- Get hands where player had opportunity to raise first in
            SELECT DISTINCT
                p.name,
                hp.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a ON hp.hand_id = a.hand_id 
                AND hp.player_id = a.player_id
                AND a.street = 'preflop'
            WHERE p.name IN ({})
                AND NOT EXISTS (
                    SELECT 1 
                    FROM actions a2
                    WHERE a2.hand_id = a.hand_id
                        AND a2.sequence_number < a.sequence_number
                        AND a2.action_type = 'raise'
                )
            GROUP BY p.name, hp.hand_id
        ),
        RFITaken AS (
            -- Get hands where player actually raised first in
            SELECT DISTINCT
                p.name,
                a1.hand_id
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN actions a1 ON hp.hand_id = a1.hand_id 
                AND hp.player_id = a1.player_id
                AND a1.action_type = 'raise'
                AND a1.street = 'preflop'
            WHERE p.name IN ({})
                AND NOT EXISTS (
                    SELECT 1 
                    FROM actions a2
                    WHERE a2.hand_id = a1.hand_id
                        AND a2.sequence_number < a1.sequence_number
                        AND a2.action_type = 'raise'
                )
            GROUP BY p.name, a1.hand_id
        )
        SELECT 
            ph.name,
            COUNT(DISTINCT ph.hand_id) as total_hands,
            COUNT(DISTINCT v.hand_id) as vpip_hands,
            COUNT(DISTINCT tbo.hand_id) as threeb_opportunities,
            COUNT(DISTINCT tb.hand_id) as threeb_count,
            COUNT(DISTINCT r.hand_id) as river_hands,
            COUNT(DISTINCT s.hand_id) as showdown_hands,
            COUNT(DISTINCT w.hand_id) as won_hands,
            COUNT(DISTINCT rfi_opp.hand_id) as rfi_opportunities,
            COUNT(DISTINCT rfi.hand_id) as rfi_count,
            COUNT(DISTINCT st_opp.hand_id) as steal_opportunities,
            COUNT(DISTINCT st.hand_id) as steal_attempts,
            GROUP_CONCAT(DISTINCT 
                CASE 
                    WHEN s.hand_id IS NOT NULL 
                    THEN s.hand_id || '|' || COALESCE(s.cards_shown, '') || '|' || 
                         COALESCE(s.board_cards, '') || '|' || COALESCE(s.net_result, '')
                END
            ) as showdown_details
        FROM PlayerHands ph
        LEFT JOIN VPIPHands v ON ph.name = v.name AND ph.hand_id = v.hand_id
        LEFT JOIN ThreeBetOpportunities tbo ON ph.name = tbo.name AND ph.hand_id = tbo.hand_id
        LEFT JOIN ThreeBets tb ON ph.name = tb.name AND ph.hand_id = tb.hand_id
        LEFT JOIN RiverHands r ON ph.name = r.name AND ph.hand_id = r.hand_id
        LEFT JOIN Showdowns s ON ph.name = s.name AND ph.hand_id = s.hand_id
        LEFT JOIN WonHands w ON ph.name = w.name AND ph.hand_id = w.hand_id
        LEFT JOIN RFIOpportunities rfi_opp ON ph.name = rfi_opp.name AND ph.hand_id = rfi_opp.hand_id
        LEFT JOIN RFITaken rfi ON ph.name = rfi.name AND ph.hand_id = rfi.hand_id
        LEFT JOIN StealOpportunities st_opp ON ph.name = st_opp.name AND ph.hand_id = st_opp.hand_id
        LEFT JOIN StealAttempts st ON ph.name = st.name AND ph.hand_id = st.hand_id
        GROUP BY ph.name"""
        
        placeholders = ','.join('?' * len(players))
        query = query.format(placeholders, placeholders, placeholders, placeholders, placeholders, 
                           placeholders, placeholders, placeholders, placeholders, placeholders)
        
        cursor = self.conn.cursor()
        cursor.execute(query, players * 10)
        
        results = {}
        for row in cursor.fetchall():
            (name, total_hands, vpip_hands, threeb_opportunities, threeb_count, 
             river_hands, showdown_hands, won_hands, rfi_opportunities, rfi_count,
             steal_opportunities, steal_attempts, showdown_details_str) = row
            
            vpip_percentage = (vpip_hands / total_hands * 100) if total_hands > 0 else 0
            threeb_percentage = (threeb_count / threeb_opportunities * 100) if threeb_opportunities > 0 else 0
            showdown_percentage = (showdown_hands / vpip_hands * 100) if vpip_hands > 0 else 0
            wtsd_percentage = (showdown_hands / vpip_hands * 100) if vpip_hands > 0 else 0
            w_sd_percentage = (won_hands / showdown_hands * 100) if showdown_hands > 0 else 0
            rfi_percentage = (rfi_count / rfi_opportunities * 100) if rfi_opportunities > 0 else 0
            steal_percentage = (steal_attempts / steal_opportunities * 100) if steal_opportunities > 0 else 0

            # Parse showdown details
            showdown_details = []
            if showdown_details_str:
                for detail in showdown_details_str.split(','):
                    if detail:
                        hand_id, cards, board, result = detail.split('|')
                        showdown_details.append((hand_id, cards, board, float(result)))
            
            results[name] = PlayerStats(
                name=name,
                total_hands=total_hands,
                vpip_hands=vpip_hands,
                vpip_percentage=round(vpip_percentage, 1),
                threeb_opportunities=threeb_opportunities,
                threeb_count=threeb_count,
                threeb_percentage=round(threeb_percentage, 1),
                river_reached=river_hands,
                showdown_count=showdown_hands,
                showdown_percentage=round(showdown_percentage, 1),
                won_at_showdown=won_hands,
                wtsd_percentage=round(wtsd_percentage, 1),
                w_sd_percentage=round(w_sd_percentage, 1),
                rfi_opportunities=rfi_opportunities,
                rfi_count=rfi_count,
                rfi_percentage=round(rfi_percentage, 1),
                steal_opportunities=steal_opportunities,
                steal_attempts=steal_attempts,
                steal_percentage=round(steal_percentage, 1),
                showdown_details=showdown_details
            )
            
        self.close()
        return results

def print_stats(stats: Dict[str, PlayerStats]):
    """Pretty print all statistics including showdown hands."""
    print("\nPlayer Statistics:")
    print("-" * 140)
    print(f"{'Player':<15} {'Hands':<8} {'VPIP%':<8} {'3Bet%':<8} {'RFI%':<8} {'Steal%':<8} {'WTSD%':<8} {'Won@SD%':<8} {'ShowDn':<8} {'Rivers':<8}")
    print("-" * 140)
    
    for player_stats in stats.values():
        print(f"{player_stats.name:<15} "
              f"{player_stats.total_hands:<8} "
              f"{player_stats.vpip_percentage:>7.1f}% "
              f"{player_stats.threeb_percentage:>7.1f}% "
              f"{player_stats.rfi_percentage:>7.1f}% "
              f"{player_stats.steal_percentage:>7.1f}% "
              f"{player_stats.wtsd_percentage:>7.1f}% "
              f"{player_stats.w_sd_percentage:>7.1f}% "
              f"{player_stats.showdown_count:<8} "
              f"{player_stats.river_reached:<8}")
        
        if player_stats.showdown_details:
            print("\nShowdown Hands:")
            print("-" * 120)
            print(f"{'Hand ID':<20} {'Cards':<15} {'Board':<30} {'Result':<10}")
            print("-" * 120)
            for hand_id, cards, board, result in player_stats.showdown_details:
                print(f"{hand_id:<20} {cards:<15} {board:<30} {result:>10.2f}")
            print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calculate poker statistics for players')
    parser.add_argument('players', nargs='+', help='One or more player names to analyze')
    parser.add_argument('--db', default='poker_analysis.db', help='Path to the database file')
    
    args = parser.parse_args()
    
    calculator = PokerStatsCalculator(db_path=args.db)
    stats = calculator.calculate_stats(args.players)
    print_stats(stats)