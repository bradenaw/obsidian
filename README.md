# Obsidian

Obsidian is a distributed, transactional key-value store. It outsources durability by depending on
some kind of blob storage (e.g. S3) and something to use as sharded journals. The nodes also need
some way to discover each other, like Consul or K8s.

This is a side-project I'm mostly using to learn Rust. It's very much incomplete and there are
enough data-loss level TODOs around that you should definitely not use it for anything.
