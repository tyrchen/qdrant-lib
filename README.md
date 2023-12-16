# Qdrant lib

## Why?

Qdrant is a vector search engine known for its speed, scalability, and user-friendliness. While it excels in its domain, it currently lacks a library interface for direct embedding into applications. This is fine for constructing a web service geared towards semantic search, as Qdrant can be used directly. However, for desktop or mobile applications, integrating a separate service is not an ideal approach. This library has been developed to address this specific challenge.

## How?

Luckily Qdrant code is pretty organized. It's easy to extract the core logic and build a library. Initially I spent two nights to build a POC which can create collection, index points and search.
