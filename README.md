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

## Planned Features

This implementation will include hybrid search techniques combining:

- **Vector similarity search** via pgvector for semantic matching
- **Full-text search** using PostgreSQL's tsvector/tsquery
- **Trigram fuzzy matching** via pg_trgm for typo-tolerant search
- **Weighted hybrid scoring** combining vector and lexical results

## Acknowledgments

Hybrid search architecture inspired by [Josh Long's](https://joshlong.com) excellent article
[Building a Hybrid Search Engine with PostgreSQL and JDBC](https://joshlong.com/jl/blogPost/building-a-search-engine-with-postgresql-and-jdbc.html),
which demonstrates the elegant technique of using full-text search as a prefilter before
computing expensive vector similarities, and falling back to trigram fuzzy matching when needed.