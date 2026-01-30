A Python-based system for collecting, storing, and analyzing poker hand histories from WRGPT (World Rec.Gambling Poker Tournament).

## System Components

1. **Hand Collector** (`hand_collector.py`)
   - Fetches hand histories from WRGPT
   - Manages the collection process
   - Handles rate limiting and server communication

2. **Hand Parser** (`hand_parser.py`)
   - Converts raw hand history text into structured data
   - Extracts player actions, cards, and results
   - Handles various hand history formats

3. **Hand Store** (`hand_store.py`)
   - Manages data storage interface
   - Handles player name cleaning
   - Coordinates database operations

4. **DB Manager** (`db_manager.py`)
   - Manages SQLite database operations
   - Handles schema creation and updates
   - Maintains data integrity

5. **Statistics Calculator** (`stats.py`)
   - Calculates player performance metrics
   - Generates statistical reports
   - Provides command-line interface for analysis
  


## Usage

### Collecting Hand Histories
```bash
python hand_collector.py
```
This will:
- Initialize the database if needed
- Fetch current table statuses
- Download and process new hands
- Store results in the database

### Calculating Player Statistics
```bash
# Analyze one or more players
python stats.py "Player1" "Player2" "Player3"

# Use custom database location
python stats.py "Player1" --db custom_database.db
```
### Running Tourney Averages and Leader
average_stats.py --active-only

## Available Statistics

The system calculates these key poker metrics:

| Metric    | Description |
|-----------|-------------|
| VPIP%     | Voluntarily Put money In Pot - % of hands played |
| 3Bet%     | Three-bet percentage when opportunity exists |
| WTSD%     | Went To ShowDown - % of hands reaching showdown |
| Won@SD%   | % of showdowns won |
| Hands     | Total hands played |
| ShowDn    | Number of showdowns reached |
| Rivers    | Number of rivers seen |


## Output Format

Statistics are displayed in a formatted table:
```
Player          Hands    VPIP%   3Bet%   WTSD%   Won@SD%  ShowDn  Rivers
----------------------------------------------------------------------------
PlayerName      1000     22.5%   8.3%    31.2%   52.1%    125     150
```

## Database Management

- Auto-created on first run
- Self-contained SQLite file
- Compatible with standard SQLite tools
- Easily backed up by copying file

## System Requirements

### Python Files
```
hand_collector.py
hand_parser.py
hand_store.py
db_manager.py
stats.py
```

Dependencies
- requests
- beautifulsoup4
- typing
- Standard library modules (sqlite3, logging, datetime, etc.)
