This repo contains a tool for scraping decks off of EDHREC into a sql database, as well as experimental code for creating card embeddings. 

The strategy I'm using for card embeddings involves computing co-occurrence rates for cards in the context of a commander, and using the prediction of these occurrence rates to learn embeddings for each card.
