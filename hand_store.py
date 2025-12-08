from db_manager import PokerDBManager
from typing import Dict, Any, List
import logging
from datetime import datetime

class HandStore:
    def __init__(self):
        self.db = PokerDBManager()
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _clean_player_name(self, name: str) -> str:
        """Clean player name by removing markers and extra whitespace.
        Preserves the actual name even when prefixed with status markers."""
        self.logger.debug(f"TRACE: Cleaning name in HandStore: '{name}'")
        
        # Remove leading/trailing whitespace first
        name = name.strip()
        
        # Remove markers while preserving names that start with those letters
        if name.startswith('> '):
            name = name[2:]
        if name.startswith('D '):  # Only remove D if it's a separate marker
            name = name[2:]
        if name.startswith('V '):
            name = name[2:]
            
        # Final trim of any remaining whitespace
        name = name.strip()
        
        self.logger.debug(f"TRACE: HandStore cleaned name result: '{name}'")
        return name

    def _find_button_position(self, actions, players):
        """Find button position by looking for small blind action and going one seat back."""
        # First find the small blind action
        for action in actions:
            if action.action_type == 'blind' and action.amount == 100:  # Small blind
                sb_player_name = action.player
                # Find the SB player's seat number
                for player in players:
                    if self._clean_player_name(player['name']) == sb_player_name:
                        sb_seat = player['seat']
                        # Button is the seat before the SB, wrapping around to max seat if necessary
                        total_seats = max(p['seat'] for p in players)
                        return sb_seat - 1 if sb_seat > 1 else total_seats
        return None

    def _find_small_blind(self, actions):
        """Find small blind amount from actions."""
        blind_amounts = [action.amount for action in actions 
                        if action.action_type == 'blind' 
                        and action.amount is not None]
        return min(blind_amounts) if blind_amounts else None

    def _find_big_blind(self, actions):
        """Find big blind amount from actions."""
        blind_amounts = [action.amount for action in actions 
                        if action.action_type == 'blind' 
                        and action.amount is not None]
        return max(blind_amounts) if blind_amounts else None

    def _calculate_net_result(self, player_name: str, hand_data: Dict[str, Any]) -> int:
        """Calculate net result for a player in this hand."""
        # Sum up all the player's actions
        total_action = sum(action.amount for action in hand_data['actions'] 
                         if action.player == player_name and action.amount is not None)
        
        # If player won the pot, add it to their result
        if player_name == hand_data['winner']:
            return hand_data['total_pot'] - total_action
        
        return -total_action

    def store_hand(self, hand_data: Dict[str, Any]) -> None:
        """Store a complete hand in the database."""
        try:
            self.db.connect()
            
            # Start a transaction
            self.db.conn.execute("BEGIN TRANSACTION")
            
            try:
                # First clean all player names
                for player in hand_data['players']:
                    player['name'] = self._clean_player_name(player['name'])
                
                # Create a dictionary of players by name for easier lookup
                players_by_name = {player['name']: player for player in hand_data['players']}
                
                # Collect all unique player names from both table and actions
                unique_players = set()
                # Add players from the table
                for player in hand_data['players']:
                    unique_players.add(player['name'])
                # Add players from actions
                for action in hand_data['actions']:
                    unique_players.add(action.player)
                
                self.logger.info("Finding button position...")
                button_position = self._find_button_position(hand_data['actions'], hand_data['players'])
                
                self.logger.info("Storing players...")
                # Store or update all players and create ID mapping
                player_ids = {}
                for player_name in unique_players:
                    player_id = self.db.add_player(
                        name=player_name,
                        last_seen_date=datetime.now().isoformat()
                    )
                    player_ids[player_name] = player_id

                self.logger.info("Storing hand...")
                hand_info = hand_data['hand_info']
                hand_id = f"{hand_info['table_id']}_{hand_info['hand_number']}"
                self.db.add_hand({
                    'hand_id': hand_id,
                    'table_id': hand_info['table_id'],
                    'date_played': hand_data['actions'][0].timestamp.isoformat() if hand_data['actions'] else None,
                    'small_blind_amount': self._find_small_blind(hand_data['actions']),
                    'big_blind_amount': self._find_big_blind(hand_data['actions']),
                    'button_position': button_position,
                    'total_players': len(hand_data['players']),
                    'board_cards': hand_data['final_board'],
                    'total_pot': hand_data['total_pot']
                })

                self.logger.info("Storing player hands...")
                # Only store for players who were at the table and ensure uniqueness
                processed_players = set()
                for player in hand_data['players']:
                    if player['name'] not in processed_players:
                        net_result = self._calculate_net_result(player['name'], hand_data)
                        self.logger.info(f"Calculating net result for {player['name']}: {net_result}")
                        self.db.add_hand_player({
                            'hand_id': hand_id,
                            'player_id': player_ids[player['name']],
                            'position': player['seat'],
                            'starting_stack': player['stack'],
                            'net_result': net_result,
                            'cards_shown': hand_data['shown_hands'].get(player['name'])
                        })
                        processed_players.add(player['name'])

                self.logger.info("Storing actions...")
                self.logger.info(f"Available player_ids: {player_ids}")
                self.logger.info(f"First few actions: {[f'{a.player}: {a.action_type}' for a in hand_data['actions'][:3]]}")
                
                for sequence_number, action in enumerate(hand_data['actions'], 1):
                    self.db.add_action({
                        'hand_id': hand_id,
                        'player_id': player_ids[action.player],
                        'street': action.street.value,
                        'action_type': action.action_type,
                        'amount': action.amount,
                        'is_all_in': action.is_all_in,
                        'sequence_number': sequence_number
                    })

                self.db.conn.commit()
                self.logger.info(f"Successfully stored hand {hand_id}")

            except Exception as e:
                self.db.conn.rollback()
                self.logger.error(f"Transaction error: {str(e)}")
                raise
                
        except Exception as e:
            self.logger.error(f"Error storing hand: {str(e)}")
            raise
        finally:
            self.db.close()