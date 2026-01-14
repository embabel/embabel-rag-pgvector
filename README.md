# Embabel RAG pgvector store

RAG (Retrieval-Augmented Generation) vector store implementation using PostgreSQL with pgvector.

## Features

- **Vector similarity search** via pgvector for semantic matching
- **Full-text search** using PostgreSQL's tsvector/tsquery
- **Trigram fuzzy matching** via pg_trgm for typo-tolerant search
- **Weighted hybrid scoring** combining vector and lexical results
- **Automatic tsvector maintenance** via PostgreSQL triggers
- **Fluent builder API** for easy configuration in Java and Kotlin

## Quick Start

### Start PostgreSQL with Docker Compose

```bash
docker compose up -d
```

This starts PostgreSQL 17 with pgvector and pg_trgm extensions pre-installed.

**Connection details:**
- Host: `localhost`
- Port: `5432`
- Database: `embabel_rag`
- Username: `embabel`
- Password: `embabel`

### Spring Boot Configuration

Add to your `application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/embabel_rag
    username: embabel
    password: embabel

embabel.rag.pgvector:
  name: my-rag-store
  content-element-table: content_elements
  vector-weight: 0.7
  fts-weight: 0.3
  fuzzy-threshold: 0.2
```

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

## Docker Compose

The included `docker-compose.yml` provides a ready-to-use PostgreSQL instance:

```yaml
services:
  postgres:
    image: pgvector/pgvector:pg17
    environment:
      POSTGRES_DB: embabel_rag
      POSTGRES_USER: embabel
      POSTGRES_PASSWORD: embabel
    ports:
      - "5432:5432"
    volumes:
      - pgvector_data:/var/lib/postgresql/data
      - ./init-extensions.sql:/docker-entrypoint-initdb.d/init-extensions.sql:ro
```

The `init-extensions.sql` script automatically installs the required extensions:
- `vector` - pgvector for vector similarity search
- `pg_trgm` - Trigram matching for fuzzy text search

### Commands

```bash
# Start the database
docker compose up -d

# View logs
docker compose logs -f postgres

# Stop the database
docker compose down

# Stop and remove data
docker compose down -v
```

## Hybrid Search Architecture

The hybrid search combines three search strategies:

1. **Vector Similarity** (default 70% weight)
   - Semantic matching using embeddings
   - Finds conceptually similar content

2. **Full-Text Search** (default 30% weight)
   - PostgreSQL tsvector/tsquery
   - Exact term and phrase matching

3. **Fuzzy Fallback**
   - Trigram similarity via pg_trgm
   - Handles typos and misspellings
   - Activated when hybrid search returns no results

The hybrid query uses full-text search as a cheap prefilter before computing expensive vector similarities:

```sql
WITH fts AS (
    SELECT id, ts_rank(tsv, plainto_tsquery('english', ?)) AS fts_score
    FROM content_elements
    WHERE tsv @@ plainto_tsquery('english', ?)
)
SELECT ...,
       0.7 * (1 - (embedding <=> ?::vector)) + 0.3 * fts_score AS score
FROM fts JOIN content_elements ON ...
ORDER BY score DESC
```

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `embabel.rag.pgvector.name` | `pgvector-store` | Store instance name |
| `embabel.rag.pgvector.content-element-table` | `content_elements` | Table name for content |
| `embabel.rag.pgvector.schema-name` | `public` | PostgreSQL schema |
| `embabel.rag.pgvector.vector-weight` | `0.7` | Weight for vector similarity (0.0-1.0) |
| `embabel.rag.pgvector.fts-weight` | `0.3` | Weight for full-text search (0.0-1.0) |
| `embabel.rag.pgvector.fuzzy-threshold` | `0.2` | Minimum trigram similarity (0.0-1.0) |

## Acknowledgments

Hybrid search architecture inspired by [Josh Long's](https://joshlong.com) excellent article
[Building a Hybrid Search Engine with PostgreSQL and JDBC](https://joshlong.com/jl/blogPost/building-a-search-engine-with-postgresql-and-jdbc.html),
which demonstrates the elegant technique of using full-text search as a prefilter before
computing expensive vector similarities, and falling back to trigram fuzzy matching when needed.
