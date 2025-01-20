# CoreKV Memory store

A lightweight in-memory CoreKV Store implementation.

Prioritizes startup speed and overall memory consumption over post-initialization read/write speed, it's main use-case is for versioned queries within Defra. Other, external, use-cases are currently supported, but are not targeted (this may change).
