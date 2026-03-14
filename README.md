# RedPill Radar

A platform for detecting, analysing, and responding to harmful/hateful content targeting women on social media (Twitter/X).

## Project Structure

```
redpill-radar/
├── analyse/       Content analysis & moderation API (Python/FastAPI + Groq LLM)
├── collect/       Twitter/X crawler agent that gathers content for analysis
├── rebutt/        Rebuttal generation module
├── data/          Shared SQLite database (used by analyse and rebutt)
└── venv/          Shared Python virtual environment
```

## Modules

### analyse

FastAPI backend that ingests tweets, classifies them using Groq LLM (safe/harmful, age category, abuse subcategories), and tracks processing state in SQLite. See [analyse/README.md](analyse/README.md) for full API docs, setup, and database schema.

### collect

Frontend agent responsible for searching/crawling Twitter/X to gather content that may contain harmful or hateful speech targeting women. Feeds content into the `analyse` API.

### rebutt

Module for generating rebuttals or counter-narratives to flagged harmful content.

## Quick Start

```bash
# 1. Set up the virtual environment (from repo root)
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS/Linux

# 2. Install dependencies
pip install -r analyse/requirements.txt
pip install -r collect/requirements.txt

# 3. Configure environment (single .env at repo root)
copy .env.example .env
# Edit .env with your Groq API key, Twitter credentials, etc.

# 4. Run the analysis API
cd analyse
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API docs available at `http://localhost:8000/docs` once running.

All modules (analyse, collect, dashboard, rebutt) read from the shared `.env` at the repo root.
