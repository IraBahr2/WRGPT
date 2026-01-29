import requests
from hand_parser import HandParser
from hand_store import HandStore
from typing import List, Optional
import logging
import time
from dataclasses import dataclass
from bs4 import BeautifulSoup

@dataclass
class TableStatus:
    table_id: str
    current_hand: int
    status: str

class HandCollector:
    def __init__(self):
        self.parser = HandParser()
        self.store = HandStore()
        self._setup_logging()
        self.base_url = "http://hands.wrgpt.org/b"
        
        # Initialize database
        self.store.db.connect()
        self.store.db.initialize_database()
        self.store.db.close()


    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _get_table_start_info(self, table_id: str) -> tuple[str, int]:
        """Determine the base URL and starting hand number based on table ID."""
        if table_id.startswith('d'):
            return "http://hands.wrgpt.org/d", 199
        elif table_id.startswith('c'):
            return "http://hands.wrgpt.org/c", 120
        return "http://hands.wrgpt.org/b", 1    

    def parse_status_page(self) -> List[TableStatus]:
        """Parse the main status page to get all active tables and their hand numbers."""
        url = "http://hands.wrgpt.org/tablebytable.html"
        try:
            response = requests.get(url)
            response.raise_for_status()
            text = response.text

            # Parse the HTML using Beautiful Soup
            soup = BeautifulSoup(text, 'html.parser')

            # Find the table (assuming it's the first table in the HTML)
            table = soup.find('table')

            tables = []
            # Iterate through each row in the table
            for row in table.find_all('tr')[1:]:  # Skip header row
                columns = row.find_all('td')
                if len(columns) < 4:
                    continue  # Skip malformed rows

                table_id = columns[0].text.strip()  # First column: Table ID
                try:
                    hand_num = int(columns[1].text.strip())  # Second column: Hand number
                except ValueError:
                    continue  # Skip lines where hand number isn't an integer

                status = columns[3].text.strip()  # Fourth column: Status

                # Only add the table if it is not "Broken"
                if status != "Broken":
                    tables.append(TableStatus(table_id=table_id, current_hand=hand_num, status=status))

            self.logger.info(f"Found {len(tables)} active tables")
            return tables

        except requests.RequestException as e:
            self.logger.error(f"Error fetching status page: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing status page: {e}")
            return []

    def fetch_hand(self, table_id: str, hand_number: int) -> Optional[str]:
        """Fetch a single hand history."""
        url = f"{self.base_url}/hands/{table_id}_{hand_number}.txt"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.warning(f"Could not fetch hand {table_id}_{hand_number}: {e}")
            return None

    def collect_hands_for_table(self, table_id: str, up_to_hand: int, status: str) -> None:
        """Collect all unprocessed hands for a specific table up to the current hand number."""
        # Get the correct base URL and starting hand for this table
        base_url, start_hand = self._get_table_start_info(table_id)
        self.base_url = base_url
        
        # For finished tables and unknown status, include the last hand (it's complete).
        # For active tables (empty/whitespace status), exclude current hand (it's in progress).
        end_hand = up_to_hand + 1 if status.strip() in ["Finished", "Unk"] else up_to_hand
        
        self.logger.info(f"Collecting hands for table {table_id} (status: {status}) from hand {start_hand} up to hand {end_hand - 1}")
        
        for hand_num in range(start_hand, end_hand):
            try:
                # Check if we've already processed this hand
                if self.store.db.is_hand_processed(table_id, hand_num):
                    self.logger.info(f"Skipping {table_id} hand #{hand_num} - already processed")
                    continue

                self.logger.info(f"Fetching {table_id} hand #{hand_num}")
                hand_text = self.fetch_hand(table_id, hand_num)
                
                if hand_text:
                    # Parse and store the hand
                    hand_data = self.parser.parse_hand(hand_text)
                    self.store.store_hand(hand_data)
                    
                    # Mark this hand as processed
                    self.store.db.mark_hand_processed(table_id, hand_num)
                    
                    self.logger.info(f"Stored {table_id} hand #{hand_num}")
                else:
                    self.logger.warning(f"Could not fetch {table_id} hand #{hand_num}")
                
                # Be nice to the server
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error processing {table_id} hand #{hand_num}: {e}")
                continue

    def collect_all_hands(self) -> None:
        """Collect all unprocessed hands from all active tables."""
        # Get current table status
        tables = self.parse_status_page()
        total_tables = len(tables)
        
        self.logger.info(f"Beginning collection for {total_tables} tables")
        
        # Process each table
        for idx, table in enumerate(tables, 1):
            self.logger.info(f"Processing table {table.table_id} ({idx}/{total_tables}), current hand: {table.current_hand}, status: {table.status}")
            self.collect_hands_for_table(table.table_id, table.current_hand, table.status)
            
            # Brief pause between tables
            time.sleep(2)

# If you need to run the collector
if __name__ == "__main__":
    collector = HandCollector()
    collector.collect_all_hands()
