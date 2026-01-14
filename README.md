# Embabel RAG pgvector store

```
   ╔═══════════════════════════════════════════════════════════╗
   ║                                                           ║
   ║    🚀  ██████╗ ██████╗ ███╗   ███╗██╗███╗   ██╗ ██████╗   ║
   ║       ██╔════╝██╔═══██╗████╗ ████║██║████╗  ██║██╔════╝   ║
   ║       ██║     ██║   ██║██╔████╔██║██║██╔██╗ ██║██║  ███╗  ║
   ║       ██║     ██║   ██║██║╚██╔╝██║██║██║╚██╗██║██║   ██║  ║
   ║       ╚██████╗╚██████╔╝██║ ╚═╝ ██║██║██║ ╚████║╚██████╔╝  ║
   ║        ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝ ╚═════╝   ║
   ║                                                           ║
   ║         ███████╗ ██████╗  ██████╗ ███╗   ██╗              ║
   ║         ██╔════╝██╔═══██╗██╔═══██╗████╗  ██║              ║
   ║         ███████╗██║   ██║██║   ██║██╔██╗ ██║              ║
   ║         ╚════██║██║   ██║██║   ██║██║╚██╗██║              ║
   ║         ███████║╚██████╔╝╚██████╔╝██║ ╚████║              ║
   ║         ╚══════╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═══╝   🔥        ║
   ║                                                           ║
   ║              ⚡ Stay tuned for updates! ⚡                ║
   ║                                                           ║
   ╚═══════════════════════════════════════════════════════════╝
```

RAG (Retrieval-Augmented Generation) vector store implementation using pgvector.

## Features

- **Vector similarity search** via pgvector for semantic matching
- **Full-text search** using PostgreSQL's tsvector/tsquery
- **Trigram fuzzy matching** via pg_trgm for typo-tolerant search
- **Weighted hybrid scoring** combining vector and lexical results
- **Automatic tsvector maintenance** via PostgreSQL triggers
- **Fluent builder API** for easy configuration in Java and Kotlin

## Usage

### Java

```java
PgVectorStore store = PgVectorStore.builder()
    .withDataSource(dataSource)
    .withVectorStore(vectorStore)
    .withEmbeddingService(embeddingService)
    .withName("my-rag-store")
    .withHybridWeights(0.7, 0.3)  // 70% vector, 30% FTS
    .withFuzzyThreshold(0.2)
    .build();

// Hybrid search with automatic fuzzy fallback
List<SimilarityResult<Chunk>> results = store.hybridSearch(
    new TextSimilaritySearchRequest("machine learning", 10, 0.5),
    Chunk.class
);
```

### Kotlin

```kotlin
val store = PgVectorStore.builder()
    .withDataSource(dataSource)
    .withVectorStore(vectorStore)
    .withEmbeddingService(embeddingService)
    .withName("my-rag-store")
    .withHybridWeights(0.7, 0.3)
    .build()

val results = store.hybridSearch(
    TextSimilaritySearchRequest("machine learning", topK = 10),
    Chunk::class.java
)
```

### Spring Boot Auto-Configuration

```yaml
embabel.rag.pgvector:
  name: my-rag-store
  content-element-table: content_elements
  vector-weight: 0.7
  fts-weight: 0.3
  fuzzy-threshold: 0.2
```

## Acknowledgments

Hybrid search architecture inspired by [Josh Long's](https://joshlong.com) excellent article
[Building a Hybrid Search Engine with PostgreSQL and JDBC](https://joshlong.com/jl/blogPost/building-a-search-engine-with-postgresql-and-jdbc.html),
which demonstrates the elegant technique of using full-text search as a prefilter before
computing expensive vector similarities, and falling back to trigram fuzzy matching when needed.