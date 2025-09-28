import torch.nn as nn
import torch

class CardPointwiseMutualPredictor(nn.Module):
    def __init__(self, num_cards, num_commanders, embed_dim, hidden_size=256):
        super(CardPointwiseMutualPredictor, self).__init__()
        self.card_embedding = nn.Embedding(num_embeddings=num_cards, embedding_dim=embed_dim)
        self.commander_embedding = nn.Embedding(num_embeddings=num_commanders, embedding_dim=embed_dim)
        self.encoder = nn.Sequential(
            nn.Linear(3 * embed_dim, hidden_size),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1)
        )
    
    def forward(self, x):
        commander_embed = self.commander_embedding(x[:, 0])
        condition_card_embed = self.card_embedding(x[:, 1])
        target_card_embed = self.card_embedding(x[:, 2])
        
        combined = torch.cat([commander_embed, condition_card_embed, target_card_embed], dim=-1)
        score = self.encoder(combined)
        return score