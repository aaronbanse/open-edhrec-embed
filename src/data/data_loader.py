import torch

class EDHRECDataLoader:
    def __init__(self):
        pass

def train_valid_split(data, valid_ratio=0.1):
    """Splits data into training and validation sets"""
    n = len(data)
    data_shuffled = data[torch.randperm(n)]
    split_idx = int(n * (1 - valid_ratio))
    train_data = data_shuffled[:split_idx]
    valid_data = data_shuffled[split_idx:]
    return train_data, valid_data
