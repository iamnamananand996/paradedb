DROP TABLE IF EXISTS cohere_wikipedia CASCADE;

CREATE TABLE cohere_wikipedia (
    _id TEXT PRIMARY KEY,
    url TEXT,
    title TEXT,
    text TEXT,
    emb JSONB
);
