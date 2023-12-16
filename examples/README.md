# Example

This example contains an indexer and a searcher. To run it, you shall first download the dataset [OpenAI embeddings for Wikipedia Simple English](https://www.kaggle.com/datasets/stephanst/wikipedia-simple-openai-embeddings/). Put the downloaded file under `fixtures/wikipedia.zip`. Then run the indexer:

```bash
cargo run --example indexer --release
```

After the indexer finishes, you can run the searcher:

```bash
cargo run --example searcher --release
```
