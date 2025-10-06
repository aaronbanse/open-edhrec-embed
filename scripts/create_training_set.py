# scripts/create_training_set.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.training_set_creator import TrainingSetCreator

# Main execution
if __name__ == "__main__":
    include_threshold = 500
    training_set_creator = TrainingSetCreator(db_path='data/raw/edhrec_decks.db', inclusion_threshold=include_threshold)
    # fix this to choose examples per pair based on minimum cards above thresh per commander
    training_set_creator.create_training_set(threshold=include_threshold)
