import re
from datetime import datetime
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass
from enum import Enum

class Street(Enum):
    PREFLOP = 'preflop'
    FLOP = 'flop'
    TURN = 'turn'
    RIVER = 'river'

@dataclass
class HandAction:
    timestamp: datetime
    player: str
    action_type: str
    amount: Optional[int] = None
    is_all_in: bool = False
    street: Street = Street.PREFLOP

class HandParser:
    def __init__(self):
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _clean_player_name(self, name: str) -> str:
        """Clean player name by removing markers and extra whitespace."""
        self.logger.info(f"TRACE: _clean_player_name input: '{name}'")
        
        # Remove leading/trailing whitespace first
        name = name.strip()
        
        # Remove markers while preserving names that start with those letters
        if name.startswith('> '):
            self.logger.info("TRACE: Removing '> ' marker")
            name = name[2:]
        if name.startswith('D '):
            self.logger.info("TRACE: Removing 'D ' marker")
            name = name[2:]
        if name.startswith('V '):
            self.logger.info("TRACE: Removing 'V ' marker")
            name = name[2:]
            
        # Final trim of any remaining whitespace
        name = name.strip()
        
        self.logger.info(f"TRACE: _clean_player_name output: '{name}'")
        return name

    def _parse_header(self, text: str) -> Dict:
        """Parse the header of the hand history."""
        header_match = re.search(r'Subject: \[([^]]+)\]\[hand:(\d+)\]', text)
        if not header_match:
            raise ValueError("Cannot parse hand header")
        
        table_id, hand_number = header_match.groups()
        day_match = re.search(r'! Table [^,]+, Hand \d+, Day (\d+)', text)
        day = day_match.group(1) if day_match else None
        
        return {
            'table_id': table_id,
            'hand_number': hand_number,
            'day': day
        }

    def _parse_players(self, text: str) -> List[Dict]:
        """Parse player information from the hand layout."""
        players = {}
        
        table_start = text.find("+-+----------------------------+")
        table_end = text.find("! History of this hand:")
        if table_start == -1 or table_end == -1:
            return []
            
        player_table = text[table_start:table_end]
        
        # Updated pattern to handle D, >, V or space markers
        player_pattern = r'\s*(\d+)\|([DV>\s])\s*([^|]+?)\s*\|\s*(\d+,?\d*)\s*\|\s*(\d*,?\d*)\s*\|\s*([^|]*?)\s*\|'
        
        for match in re.finditer(player_pattern, player_table):
            seat, marker, name, bankroll, action, status = match.groups()
            name = self._clean_player_name(name)
            if name and not name.startswith('Name'):  
                is_on_vacation = (
                    marker.strip() == 'V' or 
                    '<AWOL>' in status or 
                    'on vacation' in status.lower() or
                    '<Gone>' in status
                )
                
                players[name] = {
                    'seat': int(seat),
                    'name': name,
                    'stack': int(bankroll.replace(',', '')) if bankroll.strip() else 0,
                    'action': int(action.replace(',', '')) if action.strip() else 0,
                    'status': status.strip(),
                    'is_on_vacation': is_on_vacation
                }
        
        return list(players.values())

    def _extract_explicit_dollar(self, action_text: str) -> Optional[int]:
        """Extract a literal $ amount from action text, if one is stated."""
        amount_match = re.search(r'\$(\d+,?\d*)', action_text)
        if amount_match:
            return int(amount_match.group(1).replace(',', ''))
        return None

    def _extract_raise_amounts(self, action_text: str, current_bet_level: int):
        """Parse a 'raises' action line.

        Returns (raise_size, new_total):
        - raise_size is the amount raised BY, as an action's own amount —
          taken from the literal "raises $X" figure when present, since
          that is the stated raise-by size, not the new total bet.
        - new_total is the new total bet level the street is raised to,
          used to update current_bet_level and the raiser's own
          street contribution. It is independent of what the raiser had
          already contributed this street (a blind, an earlier call) —
          it's an absolute total, not a delta.
        If the text only states the total ("raises to $X total", no
        explicit "$Y" raise-by figure), raise_size is derived as
        new_total - current_bet_level (the increase over the previous
        bet level being raised over).
        """
        raise_pattern = r'raises \$(\d+,?\d*) to \$(\d+,?\d*) total'
        raise_match = re.search(raise_pattern, action_text)
        if raise_match:
            raise_size = int(raise_match.group(1).replace(',', ''))
            new_total = int(raise_match.group(2).replace(',', ''))
            return raise_size, new_total

        total_match = re.search(r'to \$(\d+,?\d*) total', action_text)
        if total_match:
            new_total = int(total_match.group(1).replace(',', ''))
            raise_size = new_total - current_bet_level if current_bet_level is not None else None
            return raise_size, new_total

        return None, None

    def _parse_actions(self, text: str) -> List[HandAction]:
        """Parse all actions from the hand history."""
        actions = []
        history_start = text.find("! History of this hand:")
        history_end = text.find("! Hand over")
        if history_end == -1:
            history_end = text.find("+-+----", history_start)

        history_text = text[history_start:history_end]

        current_street = Street.PREFLOP
        # current_bet_level: the total a player must have contributed this
        # street to be "even" (what a caller with no stated $ owes up to).
        current_bet_level = 0
        # street_contributions: running total each player has put in during
        # the CURRENT street (reset on every street transition below). Seeded
        # from blinds so that a blind-poster who later calls only owes the
        # difference between the bet level and what they already posted,
        # rather than the full bet level again.
        street_contributions: Dict[str, int] = {}

        action_pattern = r'! (\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})! (.*?)(?=!|\n)'
        blind_pattern = r'! [^!]+! ([^!]+) blinds \$(\d+,?\d*)'

        # Parse blind actions first
        for match in re.finditer(blind_pattern, history_text):
            player, amount = match.groups()
            amount = int(amount.replace(',', ''))
            dealing_match = re.search(r'! (\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})! Dealing', history_text)
            if dealing_match:
                timestamp = datetime.strptime(dealing_match.group(1), '%m/%d/%y %H:%M:%S')
                player = self._clean_player_name(player)
                actions.append(HandAction(
                    timestamp=timestamp,
                    player=player,
                    action_type='blind',
                    amount=amount,
                    street=Street.PREFLOP
                ))
                street_contributions[player] = amount
                current_bet_level = max(current_bet_level, amount)

        # Parse other actions
        for match in re.finditer(action_pattern, history_text):
            timestamp_str, action_text = match.groups()
            
            # Skip table talk/chat messages and underscores
            if '"' in action_text or '--' in action_text or '_' in action_text:
                continue

            try:
                timestamp = datetime.strptime(timestamp_str, '%m/%d/%y %H:%M:%S')
                action = None

                # Update current street
                if 'Flopped cards:' in action_text:
                    current_street = Street.FLOP
                    current_bet_level = 0
                    street_contributions = {}
                    continue
                elif 'Flopped card:' in action_text and current_street == Street.FLOP:
                    current_street = Street.TURN
                    current_bet_level = 0
                    street_contributions = {}
                    continue
                elif 'Flopped card:' in action_text and current_street == Street.TURN:
                    current_street = Street.RIVER
                    current_bet_level = 0
                    street_contributions = {}
                    continue

                # Skip other non-action lines
                if any(skip in action_text for skip in ['Dealing', 'Pot right']):
                    continue
                
                # Parse player actions
                player_name = ''
                if 'is back from vacation' in action_text.lower():
                    continue
                elif 'is on vacation and folds' in action_text.lower():
                    player_name = action_text.split('is on vacation and folds')[0].strip()
                    action = HandAction(
                        timestamp=timestamp,
                        player=self._clean_player_name(player_name),
                        action_type='vacation_fold',
                        street=current_street
                    )
                elif 'folds' in action_text:
                    player_name = action_text.split(' folds')[0].strip()
                    action = HandAction(
                        timestamp=timestamp,
                        player=self._clean_player_name(player_name),
                        action_type='fold',
                        street=current_street
                    )
                elif 'calls' in action_text:
                    player_name = action_text.split(' calls')[0].strip()
                    clean_player = self._clean_player_name(player_name)
                    # Calls never carry an explicit $ amount in this data — the
                    # call is always to the current bet level (the last raise's
                    # total, or the big blind preflop if no one has raised).
                    # The amount owed is that level minus whatever this player
                    # already has in this street (a blind, or a prior call that
                    # got raised over) — not the full level again.
                    already_in = street_contributions.get(clean_player, 0)
                    amount = max(current_bet_level - already_in, 0)
                    street_contributions[clean_player] = current_bet_level
                    action = HandAction(
                        timestamp=timestamp,
                        player=clean_player,
                        action_type='call',
                        amount=amount,
                        is_all_in='all in' in action_text.lower(),
                        street=current_street
                    )
                elif 'raises' in action_text:
                    player_name = action_text.split(' raises')[0].strip()
                    clean_player = self._clean_player_name(player_name)
                    raise_size, new_total = self._extract_raise_amounts(action_text, current_bet_level)
                    action = HandAction(
                        timestamp=timestamp,
                        player=clean_player,
                        action_type='raise',
                        amount=raise_size,
                        is_all_in='all in' in action_text.lower(),
                        street=current_street
                    )
                    if new_total is not None:
                        current_bet_level = new_total
                        street_contributions[clean_player] = new_total
                elif 'checks' in action_text:
                    player_name = action_text.split(' checks')[0].strip()
                    action = HandAction(
                        timestamp=timestamp,
                        player=self._clean_player_name(player_name),
                        action_type='check',
                        street=current_street
                    )
                elif 'bets' in action_text:
                    player_name = action_text.split(' bets')[0].strip()
                    clean_player = self._clean_player_name(player_name)
                    amount = self._extract_explicit_dollar(action_text)
                    action = HandAction(
                        timestamp=timestamp,
                        player=clean_player,
                        action_type='bet',
                        amount=amount,
                        is_all_in='all in' in action_text.lower(),
                        street=current_street
                    )
                    if amount:
                        current_bet_level = amount
                        street_contributions[clean_player] = amount

                if action:
                    actions.append(action)

            except Exception as e:
                self.logger.warning(f"Skipping malformed action line: {action_text}. Error: {e}")
                continue

        return actions

    def _parse_ending_stacks(self, text: str) -> Dict[str, int]:
        """Parse the SECOND player table (printed after the hand resolves) to
        get each player's true post-hand bankroll. Each hand file contains two
        player tables: the first, before "! History of this hand:", is a
        mid/end-of-hand snapshot (Bankroll = uncommitted remainder, Action =
        amount already committed this hand -- their sum is the TRUE starting
        stack for this hand). The second, after the results lines, shows each
        player's real ending bankroll once the pot has been awarded/returned.
        Combined with the first table, this gives a true per-hand stack delta
        without needing any data from other hands (verified zero-sum across
        players on real hands, and matching a stated "(net $X)" exactly).
        Returns {player_name: ending_bankroll}. Returns {} if a hand doesn't
        have two full tables (e.g. malformed/truncated source text) rather
        than guessing.
        """
        border = "+-+----------------------------+"
        positions = [m.start() for m in re.finditer(re.escape(border), text)]
        if len(positions) < 6:
            return {}

        table2_start = positions[3]  # 4th border = opening border of table 2
        table2_text = text[table2_start:]

        ending_stacks = {}
        player_pattern = r'\s*(\d+)\|([DV>\s])\s*([^|]+?)\s*\|\s*(\d+,?\d*)\s*\|'
        for match in re.finditer(player_pattern, table2_text):
            seat, marker, name, bankroll = match.groups()
            name = self._clean_player_name(name)
            if name and not name.startswith('Name'):
                ending_stacks[name] = int(bankroll.replace(',', '')) if bankroll.strip() else 0

        # A player eliminated from the tournament during this hand has their
        # row dropped from the second table entirely (not shown with a zero),
        # so the loop above leaves them absent rather than at 0. Confirm via
        # the source text's own "! <name> is eliminated!" line rather than
        # inferring elimination just from a missing row -- a missing row
        # could in principle be some other parsing gap, and this is the only
        # positive signal the data gives us. setdefault so a name that did
        # parse from table 2 for some other reason is never overwritten.
        eliminated_pattern = r'! ([^!]+?) is eliminated!'
        for match in re.finditer(eliminated_pattern, text):
            name = self._clean_player_name(match.group(1))
            if name:
                ending_stacks.setdefault(name, 0)

        return ending_stacks

    def _parse_results(self, hand_text: str) -> Dict:
        """Parse all pot results including multiple winners and uncalled bets."""
        results = {
            'winners': [],  # List of (player, amount, net_amount)
            'uncalled_bets': []  # List of (player, amount)
        }

        # Parse all win lines with net amounts
        net_win_pattern = r'! ([^!]+?) wins \$(\d+,?\d*) \(net \$(-?\d+,?\d*)\)'
        for match in re.finditer(net_win_pattern, hand_text):
            player = self._clean_player_name(match.group(1))
            amount = int(match.group(2).replace(',', ''))
            net = int(match.group(3).replace(',', ''))
            results['winners'].append((player, amount, net))

        # Parse all pot-numbered win lines (no net amounts)
        pot_win_pattern = r'! Pot \d+: ([^!]+?) wins \$(\d+,?\d*)'
        for match in re.finditer(pot_win_pattern, hand_text):
            player = self._clean_player_name(match.group(1))
            amount = int(match.group(2).replace(',', ''))
            results['winners'].append((player, amount, None))

        # Parse uncalled bets
        uncalled_pattern = r'! Pot \d+: uncalled \$(\d+,?\d*) returned to ([^!]+)'
        for match in re.finditer(uncalled_pattern, hand_text):
            amount = int(match.group(1).replace(',', ''))
            player = self._clean_player_name(match.group(2))
            results['uncalled_bets'].append((player, amount))

        return results

    def parse_hand(self, hand_text: str) -> Dict:
        """Parse a complete hand history text and return structured data."""
        try:
            # Extract basic hand info from header
            hand_info = self._parse_header(hand_text)
            # Extract player positions and stacks
            players = self._parse_players(hand_text)
            # Extract actions
            actions = self._parse_actions(hand_text)

            # Extract the final board
            # Three observed formats:
            #   "! Hand over, no showdown\n! Current board is:  <cards>"
            #   "Hand over\n! Current board is:  <cards>"
            #   "! Hand over, current board is:  <cards>"  (single fused line, all-in runout)
            # Case of "current" and what precedes it both vary, so match on the
            # "current board is:" phrase itself rather than anchoring to a
            # specific prefix or line boundary.
            final_board_pattern = r'[Cc]urrent board is:\s+([^\n]+)'
            final_board_match = re.search(final_board_pattern, hand_text)
            final_board = final_board_match.group(1).strip() if final_board_match else None

            # Extract shown cards
            shown_cards = {}
            cards_pattern = r'! ([^!]+?)\s+has:\s+([^\n]+)'
            for match in re.finditer(cards_pattern, hand_text):
                player_name = self._clean_player_name(match.group(1))
                cards = match.group(2).strip()
                shown_cards[player_name] = cards

            # Update each player's shown cards in the results
            for player_name, cards in shown_cards.items():
                for i, player in enumerate(players):
                    if player['name'] == player_name:
                        players[i]['cards_shown'] = cards

            # Parse all winners and uncalled bets
            results = self._parse_results(hand_text)

            # True post-hand bankroll per player, from the second player
            # table -- used with players[i]['stack']+['action'] (true
            # starting stack) to compute each player's real net result.
            ending_stacks = self._parse_ending_stacks(hand_text)

            # Backward-compatible aggregate fields
            # True total pot is what winners actually won — it excludes any
            # uncalled amount returned to a bettor, since that money was
            # never really contested (confirmed).
            total_pot = sum(amount for _, amount, _ in results['winners'])
            winner = results['winners'][0][0] if results['winners'] else None
            uncalled_amount = sum(amount for _, amount in results['uncalled_bets'])

            # Add metadata about the hand
            result = {
                'hand_info': hand_info,
                'players': players,
                'actions': actions,
                'final_board': final_board,
                'winner': winner,                          # first winner, kept for backward compat
                'total_pot': total_pot,                     # sum across all winners
                'shown_hands': shown_cards,
                'uncalled_amount': uncalled_amount,         # sum across all uncalled bets
                'winners': results['winners'],              # full list of (player, amount, net)
                'uncalled_bets': results['uncalled_bets'],  # full list of (player, amount)
                'ending_stacks': ending_stacks               # {player_name: post-hand bankroll}
            }

            self.logger.info(f"Successfully parsed hand {hand_info['hand_number']}")
            return result

        except Exception as e:
            self.logger.error(f"Error parsing hand: {e}")
            raise
