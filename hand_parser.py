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

    def _extract_action_amount(self, action_text: str, prev_bet_amount: Optional[int] = None) -> Optional[int]:
        """Extract the amount from action text, handling various formats."""
        if 'raises' in action_text:
            raise_pattern = r'raises \$(\d+,?\d*) to \$(\d+,?\d*) total'
            raise_match = re.search(raise_pattern, action_text)
            if raise_match:
                raise_size = int(raise_match.group(1).replace(',', ''))
                return raise_size
            
            total_match = re.search(r'to \$(\d+,?\d*) total', action_text)
            if total_match and prev_bet_amount is not None:
                total_amount = int(total_match.group(1).replace(',', ''))
                return total_amount - prev_bet_amount

        if 'calls' in action_text:
            if prev_bet_amount:
                return prev_bet_amount
            amount_match = re.search(r'\$(\d+,?\d*)', action_text)
            if amount_match:
                return int(amount_match.group(1).replace(',', ''))
        
        amount_match = re.search(r'\$(\d+,?\d*)', action_text)
        if amount_match:
            return int(amount_match.group(1).replace(',', ''))
        
        return None

    def _parse_actions(self, text: str) -> List[HandAction]:
        """Parse all actions from the hand history."""
        actions = []
        history_start = text.find("! History of this hand:")
        history_end = text.find("! Hand over")
        if history_end == -1:
            history_end = text.find("+-+----", history_start)

        history_text = text[history_start:history_end]

        current_street = Street.PREFLOP
        prev_bet_amount = 0

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
                prev_bet_amount = amount

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
                    prev_bet_amount = 0
                    continue
                elif 'Flopped card:' in action_text and current_street == Street.FLOP:
                    current_street = Street.TURN
                    prev_bet_amount = 0
                    continue
                elif 'Flopped card:' in action_text and current_street == Street.TURN:
                    current_street = Street.RIVER
                    prev_bet_amount = 0
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
                    amount = self._extract_action_amount(action_text, prev_bet_amount)
                    action = HandAction(
                        timestamp=timestamp,
                        player=self._clean_player_name(player_name),
                        action_type='call',
                        amount=amount,
                        is_all_in='all in' in action_text.lower(),
                        street=current_street
                    )
                elif 'raises' in action_text:
                    player_name = action_text.split(' raises')[0].strip()
                    amount = self._extract_action_amount(action_text, prev_bet_amount)
                    action = HandAction(
                        timestamp=timestamp,
                        player=self._clean_player_name(player_name),
                        action_type='raise',
                        amount=amount,
                        is_all_in='all in' in action_text.lower(),
                        street=current_street
                    )
                    if amount:
                        prev_bet_amount = amount
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
                    amount = self._extract_action_amount(action_text)
                    action = HandAction(
                        timestamp=timestamp,
                        player=self._clean_player_name(player_name),
                        action_type='bet',
                        amount=amount,
                        is_all_in='all in' in action_text.lower(),
                        street=current_street
                    )
                    if amount:
                        prev_bet_amount = amount

                if action:
                    actions.append(action)

            except Exception as e:
                self.logger.warning(f"Skipping malformed action line: {action_text}. Error: {e}")
                continue

        return actions

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
            final_board_pattern = r'! Hand over, current board is:  ([^\n]+)'
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

            # Extract winner and total pot
            winner_pattern = r'! ([^!]+?) wins \$(\d+,?\d*)'
            winner_match = re.search(winner_pattern, hand_text)
            winner = None
            total_pot = 0
            if winner_match:
                winner = self._clean_player_name(winner_match.group(1))
                total_pot = int(winner_match.group(2).replace(',', ''))

             # Extract uncalled amount - ADD THIS SECTION HERE
            uncalled_pattern = r'! Uncalled bet \(\$(\d+,?\d*)\) returned to ([^!]+)'
            uncalled_match = re.search(uncalled_pattern, hand_text)
            uncalled_amount = 0
            if uncalled_match:
                uncalled_amount = int(uncalled_match.group(1).replace(',', ''))
                uncalled_player = self._clean_player_name(uncalled_match.group(2))
    

            # Add metadata about the hand
            result = {
                'hand_info': hand_info,
                'players': players,
                'actions': actions,
                'final_board': final_board,
                'winner': winner,
                'total_pot': total_pot,
                'shown_hands': shown_cards,
                'uncalled_amount': uncalled_amount
            }

            self.logger.info(f"Successfully parsed hand {hand_info['hand_number']}")
            return result

        except Exception as e:
            self.logger.error(f"Error parsing hand: {e}")
            raise