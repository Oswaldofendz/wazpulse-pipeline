# wazpulse-pipeline

PulseEngine for **WazPulse** — Python worker that runs every 5 min to:

1. Fetch news from whitelisted RSS sources → `pulse_candidates` (Bloque 5)
2. Generate editorial posts via WaStake backend → `pulse_posts` (Bloque 6)
3. Send drafts to Telegram bot for human approval (Bloque 7)
4. Publish approved posts to Twitter / IG / FB / TikTok / YouTube (Bloque 8+)

Sibling of [wastake](https://github.com/Oswaldofendz/wastake). Deployed on Railway alongside the wastake-backend service.

## Setup (local)

```bash
cp .env.example .env   # fill in keys
pip install -r requirements.txt
python -u src/main.py
```

Expected log on startup:

```
INFO [pulse-engine] WazPulse PulseEngine starting — interval=300s, wastake_api=...
INFO [pulse-engine] cycle tick — pulse_candidates count=0
```

## Environment

See `.env.example`. The only vars required for Bloque 4 smoke test are `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`; the rest become required as later bloques come online.

## Deployment

Auto-deploys on push to `master` via Railway service in the WaStake Railway project (same project as `wastake-backend`, separate service).

## Roadmap

- [x] **Bloque 4** — scaffold + Supabase sanity check loop (this commit)
- [ ] **Bloque 5** — RSS fetcher → `pulse_candidates`
- [ ] **Bloque 6** — editorial generator (candidates → `pulse_posts` via WaStake backend)
- [ ] **Bloque 7** — Telegram approval bot (`pending_approval` → `approved`/`rejected`)
- [ ] **Bloque 8** — social publishers (Twitter first, then Meta, TikTok, YouTube)
