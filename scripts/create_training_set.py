from src.data.training_set_creator import TrainingSetCreator

# Main execution
if __name__ == "__main__":
    training_set_creator = TrainingSetCreator()
    # fix this to choose examples per pair based on minimum cards above thresh per commander
    training_set_creator.create_training_set(threshold=100, examples_per_pair=205)
