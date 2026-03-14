# Rebuttal Service

Automatic rebuttal generation service for harmful manosphere content. This service polls the RedPill Radar database for unprocessed harmful tweets, generates respectful, evidence-based rebuttals using Groq LLM, and displays them in a browser for review.

## Architecture

### Components

- **`config.py`** — Configuration management, loads settings from `.env`
- **`worker.py`** — Main polling loop, orchestrates the processing workflow, handles graceful shutdown
- **`services/rebuttal.py`** — `RebuttalService` class: database polling, LLM rebuttal generation, record updates
- **`services/browser_handler.py`** — `BrowserHandler` class: browser automation using Playwright

### Workflow

```
┌─────────────────────────────────────────────────────────┐
│  Worker.run() - Main Polling Loop                       │
├─────────────────────────────────────────────────────────┤
│  1. Poll database for unprocessed harmful content       │
│  2. For each item:                                      │
│     - Generate rebuttal via Groq LLM                    │
│     - Update database (review_comment + is_processed)   │
│     - Open tweet in browser for review                  │
│  3. Sleep for REBUTTAL_POLLING_INTERVAL seconds         │
│  4. Repeat until shutdown signal                        │
└─────────────────────────────────────────────────────────┘
```

## Setup

### 1. Create and activate virtual environment

```bash
cd rebutt
python3 -m venv venv

# macOS/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt

# Install browser binaries for Playwright
playwright install chromium
```

### 3. Configure environment

Copy and edit `.env`:

```bash
cp .env .env.local  # (or just edit .env)
```

**Required variables:**
- `GROQ_API_KEY` — Your Groq API key (get from https://console.groq.com)
- `DATABASE_URL` — SQLite database path (must match the analyse folder's database)

**Optional variables:**
- `REBUTTAL_POLLING_INTERVAL=10` — Seconds between polling cycles (default: 10)
- `BROWSER_HEADLESS=false` — Run browser in headless mode (default: false for visible window)
- `LLM_REBUTTAL_TIMEOUT=30` — LLM API timeout in seconds (default: 30)
- `REBUTTAL_MAX_RETRIES=3` — Retries on LLM failures (default: 3)

### 4. Ensure database exists

The database should be at `../data/redpill_radar.db` (created by the analyse service).

## Running the Service

### Start the worker

```bash
python3 worker.py
```

**Output:**
```
2026-03-14 10:15:30 - worker - INFO - Starting rebuttal worker (polling interval: 10s)
2026-03-14 10:15:30 - services.rebuttal - INFO - Found 2 unprocessed harmful content item(s)
2026-03-14 10:15:31 - services.rebuttal - INFO - Generated rebuttal (length: 187 chars)
2026-03-14 10:15:31 - services.rebuttal - INFO - Updated content record with rebuttal
2026-03-14 10:15:31 - services.browser_handler - INFO - Opening tweet in browser: https://twitter.com/i/web/status/1234567890
...
```

### Graceful shutdown

Press `Ctrl+C` or send `SIGTERM` signal. The worker will:
1. Stop accepting new items
2. Close the browser
3. Flush any pending database operations
4. Exit cleanly

## Database Integration

### Query Pattern

The service queries the `content` table:

```sql
SELECT id, twitter_id, content_text, content_type FROM content 
WHERE is_processed = false AND content_type = 'harmful' 
ORDER BY created_at ASC 
LIMIT 10;
```

### Update Pattern

For each processed item, the service:

1. Sets `review_comment = {generated_rebuttal}` — Store the rebuttal text
2. Sets `is_processed = true` — Mark as processed
3. Appends to `processing_history` — Audit trail entry:
   ```json
   {
     "timestamp": "2026-03-14T10:15:31.123456+00:00",
     "action": "rebuttal_generated",
     "rebuttal": "The rebuttal text..."
   }
   ```

## LLM Integration

The service uses Groq's LLM API to generate rebuttals with the following prompt:

```
You are a respectful, evidence-based counter-argument specialist. A harmful tweet targeting women has been flagged for analysis. 

Tweet: {content_text}

Generate a concise, factual rebuttal (2-3 sentences) that:
- Directly addresses the claim
- Uses data or logic where applicable
- Maintains a respectful, non-confrontational tone
- Does NOT platform or repeat the harmful framing

Rebuttal:
```

**Parameters:**
- Model: `llama-3.3-70b-versatile`
- Temperature: 0.7
- Max tokens: 300
- Timeout: 30 seconds (configurable)
- Retries: 3 on failure (configurable)

## Browser Automation

The service uses the `playwright` library to open tweets in a browser window for review. Current implementation:

- Opens each processed tweet at: `https://twitter.com/i/web/status/{twitter_id}`
- Uses Chromium browser for compatibility
- Gracefully handles browser initialization/shutdown

**Note:** This is for testing phase. The tweets are displayed for human review but not automatically posted.

## Error Handling

- **LLM failures** — Retry with exponential backoff (up to `REBUTTAL_MAX_RETRIES`)
- **Database errors** — Log and skip item, continue polling
- **Browser errors** — Log warning but don't block rebuttal generation
- **Graceful shutdown** — On shutdown signal, finish current item and close cleanly

## Logging

All operations are logged to console with:
- Timestamp
- Module name
- Log level (DEBUG, INFO, WARNING, ERROR)
- Message

## Testing & Development

### Manual testing with sample data

1. Insert a harmful content record into the `content` table (via API or directly)
2. Start the worker
3. Verify:
   - Rebuttal is generated
   - Browser opens the tweet
   - Database record is updated with `is_processed = true` and rebuttal in `review_comment`

### Checking database updates

```bash
# Connect to the database
sqlite3 ../data/redpill_radar.db

# Check processed content
SELECT id, twitter_id, is_processed, review_comment FROM content LIMIT 5;
```

## Troubleshooting

### Worker won't start

- Check that `.env` file exists and contains `GROQ_API_KEY`
- Verify `DATABASE_URL` points to correct database
- Ensure all dependencies are installed: `pip install -r requirements.txt`

### LLM errors

- Verify `GROQ_API_KEY` is valid
- Check Groq API status
- Review rate limits and retry configuration

### Database connection errors

- Ensure database file exists at `../data/redpill_radar.db`
- Verify file permissions
- Check `DATABASE_URL` path is correct

### Browser won't open

- Install playwright: `pip install playwright`
- Install browser binaries: `playwright install chromium`
- Ensure X11 display is available (for headless systems, modify `browser_handler.py` to use headless mode)

## Architecture Notes

- **Polling design** — SQLite doesn't have triggers, so we poll at regular intervals
- **Async processing** — Uses `asyncio` for non-blocking database and LLM calls
- **Database session management** — Each polling cycle creates a fresh database session
- **Path handling** — Reuses database and models from `/analyse` folder
