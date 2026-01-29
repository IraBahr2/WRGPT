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
    river_reached: int
    showdown_count: int
    showdown_percentage: float
    won_at_showdown: int
    wtsd_percentage: float
    w_sd_percentage: float
    rfi_opportunities: int
    rfi_count: int
    rfi_percentage: float
    steal_opportunities: int
    steal_attempts: int
    iso_opportunities: int
    iso_attempts: int
    iso_percentage: float
    showdown_details: List[tuple] = None
    rfi_details: List[tuple] = None

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

    def _calculate_button_from_actions(self, hand_id: str) -> tuple:
        """Calculate button position and total players from blind actions for a specific hand."""
        cursor = self.conn.cursor()
        
        # Find the small blind action (smallest blind amount)
        blind_query = """
        SELECT hp.position, a.amount, p.name
        FROM actions a
        JOIN hand_players hp ON a.hand_id = hp.hand_id AND a.player_id = hp.player_id
        JOIN players p ON a.player_id = p.player_id
        WHERE a.hand_id = ? AND a.action_type = 'blind'
        ORDER BY a.amount ASC
        LIMIT 1
        """
        
        cursor.execute(blind_query, (hand_id,))
        result = cursor.fetchone()
        
        if not result:
            return None, None
        
        sb_seat, sb_amount, sb_player = result
        
        # Get total players to handle wraparound
        cursor.execute("SELECT total_players FROM hands WHERE hand_id = ?", (hand_id,))
        total_result = cursor.fetchone()
        total_players = total_result[0] if total_result else None
        
        if not total_players:
            return None, None
        
        # Button is one seat before small blind (with wraparound)
        button_seat = sb_seat - 1 if sb_seat > 1 else total_players
        
        return button_seat, total_players

    def get_position_name(self, seat: int, button_seat: int, total_players: int) -> str:
        """Convert absolute seat to relative position name."""
        # Calculate seats after button (with wraparound)
        seats_after_button = (seat - button_seat) % total_players
        
        # Map to position names based on seats after button
        if seats_after_button == 0:
            return "BTN"
        elif seats_after_button == 1:
            return "SB"
        elif seats_after_button == 2:
            return "BB"
        elif total_players >= 9:
            # 9-max positions
            position_map = {3: "UTG", 4: "UTG+1", 5: "MP1", 6: "MP2", 7: "HJ", 8: "CO"}
            return position_map.get(seats_after_button, f"+{seats_after_button}")
        elif total_players >= 6:
            # 6-max positions
            position_map = {3: "UTG", 4: "MP", 5: "CO"}
            return position_map.get(seats_after_button, f"+{seats_after_button}")
        else:
            # Short-handed
            return f"+{seats_after_button}"

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
        ),
        ISOOpportunities AS (
            -- Hands where there was a limp before the player acted
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
                        AND a2.action_type = 'call'
                        AND a2.street = 'preflop'
                        AND NOT EXISTS (
                            -- Make sure no raise before the limp
                            SELECT 1
                            FROM actions a3
                            WHERE a3.hand_id = a2.hand_id
                                AND a3.sequence_number < a2.sequence_number
                                AND a3.action_type = 'raise'
                                AND a3.street = 'preflop'
                        )
                )
            GROUP BY p.name, a1.hand_id
        ),
        ISOAttempts AS (
            -- Actual isolation raises over limpers
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
                        AND a2.action_type = 'call'
                        AND a2.street = 'preflop'
                        AND NOT EXISTS (
                            -- Make sure no raise before the limp
                            SELECT 1
                            FROM actions a3
                            WHERE a3.hand_id = a2.hand_id
                                AND a3.sequence_number < a2.sequence_number
                                AND a3.action_type = 'raise'
                                AND a3.street = 'preflop'
                        )
                )
            GROUP BY p.name, a1.hand_id
        ),
        ThreeBetOpportunities AS (
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
            -- Get hands where player had opportunity to raise first in (folded to them)
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
                        AND a2.action_type IN ('raise', 'call')
                )
            GROUP BY p.name, hp.hand_id
        ),
        RFITaken AS (
            -- Get hands where player actually raised first in (folded to them)
            SELECT DISTINCT
                p.name,
                a1.hand_id,
                hp.position,
                h.button_position,
                h.total_players,
                hp.cards_shown,
                a1.amount,
                hp.net_result
            FROM players p
            JOIN hand_players hp ON p.player_id = hp.player_id
            JOIN hands h ON hp.hand_id = h.hand_id
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
                        AND a2.action_type IN ('raise', 'call')
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
            COUNT(DISTINCT iso_opp.hand_id) as iso_opportunities,
            COUNT(DISTINCT iso.hand_id) as iso_attempts,
            GROUP_CONCAT(DISTINCT 
                CASE 
                    WHEN s.hand_id IS NOT NULL 
                    THEN s.hand_id || '|' || COALESCE(s.cards_shown, '') || '|' || 
                         COALESCE(s.board_cards, '') || '|' || COALESCE(s.net_result, '')
                END
            ) as showdown_details,
            GROUP_CONCAT(DISTINCT 
                CASE 
                    WHEN rfi.hand_id IS NOT NULL 
                    THEN rfi.hand_id || '|' || COALESCE(rfi.position, '') || '|' || 
                         COALESCE(rfi.button_position, '') || '|' || COALESCE(rfi.total_players, '') || '|' ||
                         COALESCE(rfi.cards_shown, '') || '|' || COALESCE(rfi.amount, '') || '|' ||
                         COALESCE(rfi.net_result, '')
                END
            ) as rfi_details
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
        LEFT JOIN ISOOpportunities iso_opp ON ph.name = iso_opp.name AND ph.hand_id = iso_opp.hand_id
        LEFT JOIN ISOAttempts iso ON ph.name = iso.name AND ph.hand_id = iso.hand_id
        GROUP BY ph.name"""
        
        placeholders = ','.join('?' * len(players))
        query = query.format(placeholders, placeholders, placeholders, placeholders, placeholders, 
                           placeholders, placeholders, placeholders, placeholders, placeholders,
                           placeholders, placeholders)
        
        cursor = self.conn.cursor()
        cursor.execute(query, players * 12)
        
        results = {}
        for row in cursor.fetchall():
            (name, total_hands, vpip_hands, threeb_opportunities, threeb_count, 
             river_hands, showdown_hands, won_hands, rfi_opportunities, rfi_count,
             steal_opportunities, steal_attempts, iso_opportunities, iso_attempts,
             showdown_details_str, rfi_details_str) = row
            
            vpip_percentage = (vpip_hands / total_hands * 100) if total_hands > 0 else 0
            showdown_percentage = (showdown_hands / vpip_hands * 100) if vpip_hands > 0 else 0
            wtsd_percentage = (showdown_hands / vpip_hands * 100) if vpip_hands > 0 else 0
            w_sd_percentage = (won_hands / showdown_hands * 100) if showdown_hands > 0 else 0
            rfi_percentage = (rfi_count / total_hands * 100) if total_hands > 0 else 0
            iso_percentage = (iso_attempts / iso_opportunities * 100) if iso_opportunities > 0 else 0

            # Parse showdown details
            showdown_details = []
            if showdown_details_str:
                for detail in showdown_details_str.split(','):
                    if detail:
                        hand_id, cards, board, result = detail.split('|')
                        showdown_details.append((hand_id, cards, board, float(result)))
            
            # Parse RFI details with position calculation
            rfi_details = []
            if rfi_details_str:
                for detail in rfi_details_str.split(','):
                    if detail:
                        parts = detail.split('|')
                        hand_id = parts[0]
                        seat = int(parts[1]) if parts[1] else 0
                        button_seat = int(parts[2]) if parts[2] else 0
                        total_players = int(parts[3]) if parts[3] else 0
                        cards = parts[4] if len(parts) > 4 else ''
                        amount = float(parts[5]) if len(parts) > 5 and parts[5] else 0.0
                        result = float(parts[6]) if len(parts) > 6 and parts[6] else 0.0
                        
                        # Calculate position name
                        if seat and button_seat and total_players:
                            position = self.get_position_name(seat, button_seat, total_players)
                        else:
                            # Try to calculate button position from actions if missing
                            if seat:
                                calc_button, calc_total = self._calculate_button_from_actions(hand_id)
                                if calc_button and calc_total:
                                    position = self.get_position_name(seat, calc_button, calc_total)
                                else:
                                    position = f"Seat {seat}"
                            else:
                                position = "Unknown"
                        
                        rfi_details.append((hand_id, position, cards, amount, result))
            
            results[name] = PlayerStats(
                name=name,
                total_hands=total_hands,
                vpip_hands=vpip_hands,
                vpip_percentage=round(vpip_percentage, 1),
                threeb_opportunities=threeb_opportunities,
                threeb_count=threeb_count,
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
                iso_opportunities=iso_opportunities,
                iso_attempts=iso_attempts,
                iso_percentage=round(iso_percentage, 1),
                showdown_details=showdown_details,
                rfi_details=rfi_details
            )
            
        self.close()
        return results

def print_stats(stats: Dict[str, PlayerStats]):
    """Pretty print all statistics including showdown hands and RFI hands."""
    print("\nPlayer Statistics:")
    print("-" * 130)
    print(f"{'Player':<16} {'Hands':<8} {'VPIP':<8} {'3Bet':<8} {'RFI':<8} {'ISO':<8} {'Steal':<10} {'WTSD':<8} {'Won@SD':<8} {'ShowDn':<8} {'Rivers':<8}")
    print("-" * 130)
    
    for player_stats in stats.values():
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
        
        if player_stats.rfi_details:
            print("\nRFI Hands:")
            print("-" * 120)
            print(f"{'Hand ID':<20} {'Position':<12} {'Cards':<15} {'Raise Amount':<15} {'Result':<10}")
            print("-" * 120)
            for hand_id, position, cards, amount, result in player_stats.rfi_details:
                cards_display = cards if cards else 'N/A'
                print(f"{hand_id:<20} {position:<12} {cards_display:<15} {amount:>15.2f} {result:>10.2f}")
            print()
        
        if player_stats.showdown_details:
            print("Showdown Hands:")
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
