# Local Warehouse Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a fully local shard-to-warehouse content pipeline that can scale beyond the SQLite corpus path.

**Architecture:** Keep SQLite for control-plane metadata, write append-only JSONL shard files during harvest, materialize those shards into a local DuckDB warehouse, and build local dataset/export commands on top. Provide a local Docker path that mounts repo code and a persistent data volume.

**Tech Stack:** Python 3.11, DuckDB, JSONL shards, argparse CLI, Docker, docker-compose.

---
