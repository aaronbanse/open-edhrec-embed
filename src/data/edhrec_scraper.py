import requests
from bs4 import BeautifulSoup
import time
import re
import sqlite3
from collections import Counter

class EDHRECScraper:
    def __init__(self, db_connection):
        self.base_url = "https://edhrec.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.db_connection = db_connection
        self.db_cursor = db_connection.cursor()
        
    def _create_slug(self, name):
        """Convert commander name to URL slug"""
        # Remove special characters and convert to lowercase
        slug = name.lower()
        # Remove apostrophes and quotes
        slug = slug.replace("'", "").replace('"', "")
        # Replace spaces and commas with hyphens
        slug = re.sub(r'[,\s]+', '-', slug)
        # Remove other special characters
        slug = re.sub(r'[^\w\-]', '', slug)
        # Clean up multiple hyphens
        slug = re.sub(r'-+', '-', slug.strip('-'))
        # Handle special cases
        if "//" in name:  # For partner commanders
            slug = slug.replace("//-", "-")
        return slug
    
    def _get_commanders_from_page(self):
        """Scrape commander names from the EDHREC commanders page"""
        url = f"{self.base_url}/commanders"
        print(f"Fetching commanders from: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            commanders = []
            
            # Look for commander names using the class pattern from the HTML
            name_elements = soup.find_all('span', class_='Card_name__Mpa7S')
            
            if not name_elements:
                # Try alternative patterns
                name_elements = soup.find_all('span', class_=re.compile(r'Card.*name'))
            
            for elem in name_elements:
                name = elem.text.strip()
                if name:
                    commanders.append(name)
            
            print(f"Found {len(commanders)} commanders")
            return commanders
            
        except Exception as e:
            print(f"Error fetching commanders: {e}")
            return []
    
    def _get_deck_hashes_from_commander_page(self, commander_name):
        """Extract only deck URL hashes from a commander's page"""
        slug = self._create_slug(commander_name)
        url = f"{self.base_url}/decks/{slug}"
        print(f"\n[{commander_name}]")
        print(f"  Commander page: {url}")

        deck_hashes = []

        try:
            response = self.session.get(url, timeout=10)

            if response.status_code != 200:
                print(f"  ✗ Error {response.status_code} accessing commander page")
                return deck_hashes

            # Search for all occurrences of "urlhash":"HASH_VALUE"
            page_text = response.text
            pattern = r'"urlhash"\s*:\s*"([^"]+)"'
            deck_hashes = re.findall(pattern, page_text)

            if deck_hashes:
                print(f"  ✓ Found {len(deck_hashes)} deck hashes")
            else:
                print(f"  No deck hashes found")

        except Exception as e:
            print(f"  ✗ Failed to access commander page: {e}")

        return deck_hashes

    def _extract_decklist(self, deck_url_hash):
        """Visit an individual deck page"""
        deck_url = f"{self.base_url}/deckpreview/{deck_url_hash}"
        
        try:
            response = self.session.get(deck_url, timeout=10)
            if response.status_code == 200:
                print(f"    ✓ Deck: {deck_url_hash}")
                decklist = re.search(r'"deck_preview":\{.*?"cards":\[(.*?)\].*?\}', response.text, re.DOTALL)
                print(f"      Cards: {decklist.group(1)[:60]}..." if decklist else "      No cards found")
                if decklist:
                    cards = re.findall(r'"([^"]+)"', decklist.group(1))
                    print(f"      Total cards: {len(cards)}")
                return cards if decklist else False
            else:
                print(f"    ✗ Error {response.status_code}: {deck_url_hash}")
                return False
        except Exception as e:
            print(f"    ✗ Failed: {deck_url_hash} - {str(e)[:30]}")
            return False

    def _save_decklist(self, commander_name, deck_url_hash, decklist):
        """Save the commander, deck, and cards to the database"""
        c = self.db_cursor
        
        # Insert commander if not exists
        if c.execute("SELECT id FROM commanders WHERE name = ?", (commander_name,)).fetchone() is None:
            c.execute("INSERT OR IGNORE INTO commanders (name) VALUES (?)", (commander_name,))
        commander_id = c.execute("SELECT id FROM commanders WHERE name = ?", (commander_name,)).fetchone()[0]

        # Insert deck
        c.execute("INSERT INTO decks (commander_id, url_hash) VALUES (?, ?)", (commander_id, deck_url_hash))
        deck_id = c.lastrowid

        # Insert cards and link to deck
        if len(decklist) > 99:
            print(f"      ✗ Skipping deck with {len(decklist)} cards (over 99 limit)")
            return
        
        counter = Counter(decklist)
        duplicates = [card for card, count in counter.items() if count > 1]
        if duplicates:
            print(f"      ✗ Skipping deck with duplicate cards: {', '.join(duplicates)}")
            return
        
        for card_name in decklist:
            if c.execute("SELECT id FROM cards WHERE name = ?", (card_name,)).fetchone() is None:
                c.execute("INSERT OR IGNORE INTO cards (name) VALUES (?)", (card_name,))
            card_id = c.execute("SELECT id FROM cards WHERE name = ?", (card_name,)).fetchone()[0]
            
            try:
                c.execute("INSERT INTO deck_cards (deck_id, card_id) VALUES (?, ?)", (deck_id, card_id))
            except sqlite3.IntegrityError:
                print(f"      ✗ Duplicate card entry for deck {deck_id} and card {card_id}")
                print(f" deck_hash: {deck_url_hash}, card_name: {card_name}")

        self.db_connection.commit()

    def _get_last_commander_id(self):
            c = self.db_cursor
            result = c.execute("SELECT id FROM commanders ORDER BY id DESC LIMIT 1").fetchone()[0]
            return result
        
    def _remove_decks_by_commander_id(self, commander_id):
        c = self.db_cursor
        c.execute("""
            DELETE FROM deck_cards 
            WHERE deck_id IN (SELECT id FROM decks WHERE commander_id = ?)
        """, (commander_id,))
        c.execute("DELETE FROM decks WHERE commander_id = ?", (commander_id,))
        self.db_connection.commit()
        print(f"  Removed existing decks for commander ID {commander_id}")

    def gather_decks(self, num_commanders, decks_per_commander, deck_delay=0.5, commander_delay=2, checkpoint=False):
        """Main function to visit all commander pages and their decks"""
        commanders = self._get_commanders_from_page()
        
        if not commanders:
            print("No commanders found!")
            return
        
        if checkpoint:
            # restart from last commander with saved decks after resetting all decks from that commander
            last_commander_id = self._get_last_commander_id()
            self._remove_decks_by_commander_id(last_commander_id)
            checkpoint_idx = last_commander_id - 1  # Convert to 0-based index

            print(f"Resuming from commander id: {last_commander_id}, commander name: '{commanders[checkpoint_idx]}'")
        else:
            checkpoint_idx = 0
        
        # Limit to specified number of commanders
        commanders = commanders[checkpoint_idx:num_commanders]
        
        total_decks_visited = 0
        successful_visits = 0
        failed_visits = 0
        
        for i, commander in enumerate(commanders, 1):
            print(f"\n[{i}/{len(commanders)}] Processing: {commander}")
            print("=" * 60)
            
            # Get deck hashes from commander page
            deck_hashes = self._get_deck_hashes_from_commander_page(commander)
            
            if deck_hashes:
                print(f"  Visiting {len(deck_hashes)} deck pages...")
                # Visit each deck page (limit to specified number for each commander to be respectful)
                decks_to_visit = deck_hashes[:decks_per_commander]
                for j, deck_url_hash in enumerate(decks_to_visit, 1):
                    print(f"  [{j}/{len(decks_to_visit)}]", end=" ")
                    if decklist := self._extract_decklist(deck_url_hash):
                        successful_visits += 1
                        # SAVE TO DATABASE
                        self._save_decklist(commander, deck_url_hash, decklist)
                    else:
                        failed_visits += 1
                    
                    total_decks_visited += 1
                    
                    # Delay between deck requests
                    if j < len(decks_to_visit):
                        time.sleep(deck_delay)
            else:
                print(f"  No decks found for {commander}")
            
            # Delay between commanders
            if i < len(commanders):
                time.sleep(commander_delay)
        
        # Print summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Commanders processed: {len(commanders)}")
        print(f"Total decks visited: {total_decks_visited}")
        print(f"Successful visits: {successful_visits}")
        print(f"Failed visits: {failed_visits}")
        if total_decks_visited > 0:
            success_rate = (successful_visits / total_decks_visited) * 100
            print(f"Success rate: {success_rate:.1f}%")

def setup_database(conn):
    c = conn.cursor()
    # Create tables if they don't exist
    c.execute('''CREATE TABLE IF NOT EXISTS commanders(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS cards(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS decks(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commander_id INTEGER REFERENCES commanders(id),
                url_hash TEXT NOT NULL UNIQUE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS deck_cards(
                deck_id INTEGER REFERENCES decks(id),
                card_id INTEGER REFERENCES cards(id),
                PRIMARY KEY (deck_id, card_id))''')
    conn.commit()

def empty_tables(conn):
    c = conn.cursor()
    c.execute("DELETE FROM deck_cards")
    c.execute("DELETE FROM decks")
    c.execute("DELETE FROM cards")
    c.execute("DELETE FROM commanders")
    c.execute("DELETE FROM sqlite_sequence")
    conn.commit()
