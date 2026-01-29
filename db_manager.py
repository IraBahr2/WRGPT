import sqlite3
from pathlib import Path
import logging
from typing import Optional, List, Dict, Any

class PokerDBManager:
    def __init__(self, db_path: str = "poker_analysis.db"):
        """Initialize database manager with path to SQLite database."""
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._setup_logging()
        
    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.logger.info(f"Connected to database at {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")

    def initialize_database(self) -> None:
        """Create database tables if they don't exist."""
        schema_sql = '''
        -- Players table
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            last_seen_date TEXT
        );

        -- Hands table
        CREATE TABLE IF NOT EXISTS hands (
            hand_id TEXT PRIMARY KEY,
            table_id TEXT NOT NULL,
            date_played TEXT NOT NULL,
            small_blind_amount INTEGER,
            big_blind_amount INTEGER,
            button_position INTEGER,
            total_players INTEGER NOT NULL,
            board_cards TEXT,
            total_pot INTEGER NOT NULL
        );

        -- HandPlayers table
        CREATE TABLE IF NOT EXISTS hand_players (
            hand_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            starting_stack INTEGER NOT NULL,
            net_result INTEGER,
            cards_shown TEXT,
            PRIMARY KEY (hand_id, player_id),
            FOREIGN KEY (hand_id) REFERENCES hands(hand_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );

        -- Actions table
        CREATE TABLE IF NOT EXISTS actions (
            action_id INTEGER PRIMARY KEY AUTOINCREMENT,
            hand_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            street TEXT NOT NULL CHECK (street IN ('preflop', 'flop', 'turn', 'river')),
            action_type TEXT NOT NULL CHECK (action_type IN ('fold', 'check', 'bet', 'raise', 'call', 'blind', 'vacation_fold')),
            amount INTEGER,
            is_all_in BOOLEAN NOT NULL DEFAULT 0,
            sequence_number INTEGER NOT NULL,
            FOREIGN KEY (hand_id) REFERENCES hands(hand_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );

        -- Processed Hands table
        CREATE TABLE IF NOT EXISTS processed_hands (
            table_id TEXT,
            hand_number INTEGER,
            processed_time TEXT,
            PRIMARY KEY (table_id, hand_number)
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_hands_table ON hands(table_id);
        CREATE INDEX IF NOT EXISTS idx_actions_hand ON actions(hand_id);
        CREATE INDEX IF NOT EXISTS idx_actions_player ON actions(player_id);
        CREATE INDEX IF NOT EXISTS idx_hand_players_player ON hand_players(player_id);
        '''
        
        try:
            if not self.conn:
                self.connect()
            self.conn.executescript(schema_sql)
            self.conn.commit()
            self.logger.info("Database initialized successfully")
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing database: {e}")
            raise

    def add_player(self, name: str, last_seen_date: str) -> int:
        """Add a new player or update existing player's last seen date."""
        try:
            if not self.conn:
                self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO players (name, last_seen_date)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET last_seen_date = ?
                RETURNING player_id
            """, (name, last_seen_date, last_seen_date))
            
            player_id = cursor.fetchone()[0]
            self.conn.commit()
            return player_id
        except sqlite3.Error as e:
            self.logger.error(f"Error adding/updating player {name}: {e}")
            raise

    def add_hand(self, hand_data: Dict[str, Any]) -> None:
        """Add a new hand or update existing hand in the database."""
        try:
            if not self.conn:
                self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO hands (
                    hand_id, table_id, date_played, small_blind_amount,
                    big_blind_amount, button_position, total_players,
                    board_cards, total_pot
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                hand_data['hand_id'],
                hand_data['table_id'],
                hand_data['date_played'],
                hand_data['small_blind_amount'],
                hand_data['big_blind_amount'],
                hand_data['button_position'],
                hand_data['total_players'],
                hand_data['board_cards'],
                hand_data['total_pot']
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error adding hand {hand_data['hand_id']}: {e}")
            raise

    def add_hand_player(self, player_data: Dict[str, Any]) -> None:
        """Add a player's participation in a hand."""
        try:
            if not self.conn:
                self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO hand_players (
                    hand_id, player_id, position, starting_stack,
                    net_result, cards_shown
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                player_data['hand_id'],
                player_data['player_id'],
                player_data['position'],
                player_data['starting_stack'],
                player_data['net_result'],
                player_data['cards_shown']
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error adding hand player data: {e}")
            raise

    def add_action(self, action_data: Dict[str, Any]) -> None:
        """Add an action to the database."""
        try:
            if not self.conn:
                self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO actions (
                    hand_id, player_id, street, action_type,
                    amount, is_all_in, sequence_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                action_data['hand_id'],
                action_data['player_id'],
                action_data['street'],
                action_data['action_type'],
                action_data['amount'],
                1 if action_data['is_all_in'] else 0,
                action_data['sequence_number']
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error adding action: {e}")
            raise

    def is_hand_processed(self, table_id: str, hand_number: int) -> bool:
        """Check if a hand has already been processed."""
        try:
            if not self.conn:
                self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT 1 FROM processed_hands 
                WHERE table_id = ? AND hand_number = ?
            """, (table_id, hand_number))
            
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            self.logger.error(f"Error checking processed hand: {e}")
            return False

    def mark_hand_processed(self, table_id: str, hand_number: int) -> None:
        """Mark a hand as processed."""
        try:
            if not self.conn:
                self.connect()
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO processed_hands (table_id, hand_number, processed_time)
                VALUES (?, ?, datetime('now'))
            """, (table_id, hand_number))
            
            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error marking hand as processed: {e}")
            raise