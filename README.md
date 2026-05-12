# Mt Postings

A Fjall-based posting index with support for BM25 scoring.

Inputs documents are assigned sequential docid keys. Posting lists are stored in a way that exposes
blocks of docids as a key that ends with the first docid. A posting list may be divided into more
blocks when it becomes too large while still allowing efficient seeks to data as needed. This also
allows us to control write amplification somewhat, particular if the user batches their updates.

For each posting list we store 3 different bits:
* A stats key that contains document frequency and term frequency across the corpus.
* A doc posting list that contains matching docids and the term frequency for each.
* An optional posting posting list that contains all matching positions within each document.

TBD
* How to handle deletes.