import sqlite3
from src.data.edhrec_scraper import EDHRECScraper, setup_database, empty_tables
import argparse

def main():
    parser = argparse.ArgumentParser(description='Train EDHREC PMI Model')
    parser.add_argument('--decks-per-commander', type=int, default=1000, 
                        help='Number of unique cards')
    parser.add_argument('--continue-existing-db', action='store_true')
    args = parser.parse_args()
    
    conn = sqlite3.connect('data/raw/edhrec_decks.db')
    scraper = EDHRECScraper(conn)

    if not args.continue_existing_db:
        setup_database(conn)
        empty_tables(conn)
    
    scraper.gather_decks(
        num_commanders=100,
        decks_per_commander=args.decks_per_commander,
        deck_delay=0.5,
        commander_delay=2,
        checkpoint=args.continue_existing_db
    )
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
    