import sqlite3
import torch
import time
from math import log2

class TrainingSetCreator:
    def __init__(self, db_path = 'edhrec_decks.db', inclusion_threshold=100):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.num_commanders = self.cursor.execute("SELECT COUNT(*) FROM commanders").fetchone()[0]
        self.num_decks = self.cursor.execute("SELECT COUNT(*) FROM decks").fetchone()[0]
        self.num_cards = self.cursor.execute("SELECT COUNT(*) FROM cards").fetchone()[0]
        
        # use PMI as the scoring function
        self.score_fn = self.pmi
        
        # cache to speed up repeated queries
        self.cards_above_threshold_cache = {}
        self.inclusion_rates_cache = {}
        self.conditional_rates_cache = {}
        
        # Pre-compute inclusion rates for cards above the threshold with one query
        self._precompute_inclusion_rates(inclusion_threshold)

    def _get_cards_above_threshold(self, threshold):
        if threshold not in self.cards_above_threshold_cache:
            result = self.cursor.execute("""
                SELECT card_id
                FROM deck_cards
                GROUP BY card_id
                HAVING COUNT(DISTINCT deck_id) > ?
            """, (threshold,)).fetchall()
            self.cards_above_threshold_cache[threshold] = set(row[0] for row in result)

        return self.cards_above_threshold_cache[threshold]

    def _get_commander_card_pairs_above_threshold(self, threshold):
        cards_list = list(self._get_cards_above_threshold(threshold))

        # Use IN clause with cached card IDs
        placeholders = ','.join('?' * len(cards_list))
        result = self.cursor.execute(f"""
            SELECT DISTINCT d.commander_id, dc.card_id
            FROM decks d
            INNER JOIN deck_cards dc ON d.id = dc.deck_id
            WHERE dc.card_id IN ({placeholders})
            ORDER BY d.commander_id, dc.card_id
        """, cards_list).fetchall()
        
        return result

    def _get_all_commander_cards_above_threshold(self, threshold):
        """Get all commander->cards mappings in one query"""
        cards_list = list(self._get_cards_above_threshold(threshold))

        placeholders = ','.join('?' * len(cards_list))
        result = self.cursor.execute(f"""
            SELECT DISTINCT d.commander_id, dc.card_id
            FROM decks d
            INNER JOIN deck_cards dc ON d.id = dc.deck_id
            WHERE dc.card_id IN ({placeholders})
            ORDER BY d.commander_id, dc.card_id
        """, cards_list).fetchall()
        
        # Group by commander
        commander_cards = {}
        for commander_id, card_id in result:
            if commander_id not in commander_cards:
                commander_cards[commander_id] = []
            commander_cards[commander_id].append(card_id)
        
        return commander_cards

    def _precompute_inclusion_rates(self, threshold):
        """Pre-compute all base inclusion rates"""
        print("Pre-computing base inclusion rates...")

        cards_above_threshold = self._get_cards_above_threshold(threshold)
        cards_list = list(cards_above_threshold)
        placeholders = ','.join('?' * len(cards_list))
        
        result = self.cursor.execute(f"""
            SELECT card_id, COUNT(DISTINCT deck_id) as count
            FROM deck_cards 
            WHERE card_id IN ({placeholders})
            GROUP BY card_id
        """, cards_list).fetchall()
        
        for card_id, count in result:
            self.inclusion_rates_cache[card_id] = count / self.num_decks if self.num_decks > 0 else 0.0
        
        print(f"Cached {len(self.inclusion_rates_cache)} inclusion rates")

    def _precompute_all_conditional_rates(self, threshold):
        """Pre-compute all conditional inclusion rates in bulk"""
        print("Pre-computing all conditional inclusion rates...")
        start_time = time.time()
        
        # Get cards above threshold
        cards_above_threshold = self._get_cards_above_threshold(threshold)
        cards_list = list(cards_above_threshold)
        
        # Single massive query to get all conditional rates at once
        placeholders = ','.join('?' * len(cards_list))
        
        print("Executing bulk conditional rates query...")
        result = self.cursor.execute(f"""
            SELECT 
                d.commander_id,
                dc_condition.card_id as condition_card_id,
                dc_target.card_id as target_card_id,
                COUNT(DISTINCT d.id) as denominator,
                COUNT(DISTINCT CASE WHEN dc_target.card_id IS NOT NULL THEN d.id END) as numerator
            FROM decks d
            JOIN deck_cards dc_condition ON dc_condition.deck_id = d.id
            LEFT JOIN deck_cards dc_target ON dc_target.deck_id = d.id
            WHERE dc_condition.card_id IN ({placeholders})
            AND (dc_target.card_id IN ({placeholders}) OR dc_target.card_id IS NULL)
            GROUP BY d.commander_id, dc_condition.card_id, dc_target.card_id
            HAVING COUNT(DISTINCT d.id) > 0
        """, cards_list + cards_list).fetchall()
        
        # Store in nested dict: {commander_id: {condition_card: {target_card: rate}}}
        
        for commander_id, condition_card, target_card, denominator, numerator in result:
            if target_card is None:  # Skip NULL targets
                continue

            if commander_id not in self.conditional_rates_cache:
                self.conditional_rates_cache[commander_id] = {}
            if condition_card not in self.conditional_rates_cache[commander_id]:
                self.conditional_rates_cache[commander_id][condition_card] = {}

            rate = numerator / denominator if denominator > 0 else 0.0
            self.conditional_rates_cache[commander_id][condition_card][target_card] = rate

        elapsed = time.time() - start_time
        print(f"Pre-computed {len(result):,} conditional rates in {elapsed:.2f}s")

    def _get_conditional_inclusion_rate_cached(self, card_id, condition_card_id, condition_commander_id):
        """Get cached conditional inclusion rate - O(1) lookup!"""
        try:
            return self.conditional_rates_cache[condition_commander_id][condition_card_id][card_id]
        except KeyError:
            return 0.0  # Default for missing combinations

    def _get_score(self, card_id, condition_card_id, condition_commander_id):
        """Optimized score calculation using cached rates"""
        inclusion_rate = self.inclusion_rates_cache.get(card_id, 0.0)
        conditional_rate = self._get_conditional_inclusion_rate_cached(
            card_id, condition_card_id, condition_commander_id
        )
        return self.score_fn(conditional_rate, inclusion_rate)

    def pmi(self, conditional_rate, inclusion_rate, min_conditional_rate = .0001):
        conditional_rate = max(conditional_rate, min_conditional_rate)
        return log2(conditional_rate / inclusion_rate)
    
    # generate a training example (commander_id, condition_card_id, target_card_id) -> score
    def _generate_training_example(self, commander_id, condition_card_id, target_card_id):
        score = self._get_score(target_card_id, condition_card_id, commander_id)
        return torch.tensor([commander_id, condition_card_id, target_card_id], dtype=torch.long), torch.tensor([score], dtype=torch.float)

    # create the full training set
    def create_training_set(self, threshold=500):
        start_time = time.time()
        
        print("Caching cards above threshold...")
        cards_above_threshold = self._get_cards_above_threshold(threshold)
        print(f"Found {len(cards_above_threshold)} cards above threshold {threshold}")
        
        print("Getting all commander-card mappings...")
        commander_cards = self._get_all_commander_cards_above_threshold(threshold)
        print(f"Found {len(commander_cards)} commanders")
        
        print("Getting commander-card pairs...")
        pairs = self._get_commander_card_pairs_above_threshold(threshold)
        print(f"Found {len(pairs)} commander-card pairs")
        
        # limit examples per pair to the minimum number of cards available for any commander
        # so dataset is balanced
        examples_per_pair = torch.min(torch.LongTensor([len(cards) for cards in commander_cards.values()])).item()
        
        # Pre-allocate tensors
        estimated_examples = len(pairs) * examples_per_pair
        data = torch.zeros((estimated_examples, 3), dtype=torch.long)
        scores = torch.zeros(estimated_examples, dtype=torch.float)
        
        example_idx = 0
        torch.random.manual_seed(42)
        
        for i, (commander_id, condition_card_id) in enumerate(pairs):
            # Get cards from cache instead of SQL query!
            cards_in_commander = commander_cards.get(commander_id, [])
            cards_in_commander = torch.tensor(cards_in_commander)
            
            # Shuffle and sample
            shuffled_indices = torch.randperm(len(cards_in_commander))
            cards_in_commander = cards_in_commander[shuffled_indices[:examples_per_pair]]
            
            for target_card_id in cards_in_commander:
                if target_card_id.item() == condition_card_id:
                    continue
                    
                example, score = self._generate_training_example(
                    commander_id, condition_card_id, target_card_id.item()
                )
                data[example_idx] = example
                scores[example_idx] = score
                example_idx += 1
            
            if i % 1000 == 0:
                elapsed = time.time() - start_time
                rate = example_idx / elapsed if elapsed > 0 else 0
                print(f"Progress: {i:,}/{len(pairs):,} pairs, {example_idx:,} examples, {rate:.0f} examples/sec")
        
        # Trim and save
        data = data[:example_idx]
        scores = scores[:example_idx]
        
        torch.save({'data': data, 'scores': scores}, "data/processed/training_set.pt")
        print(f"Created {len(data):,} examples in {time.time() - start_time:.2f}s")
        
        return data, scores
