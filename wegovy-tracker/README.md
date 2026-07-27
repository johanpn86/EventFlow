# Wegovy Tracker

Privat, mobil-først web-app til at styre et 4-måneders Wegovy-forløb: dosis-/optrapningsplan, vægt- og vanetracking, bivirknings-log og en lomme-guide. Alt gemmes udelukkende i browserens LocalStorage — ingen backend, ingen ekstern datadeling.

## Kør lokalt

```bash
npm install
npm run dev
```

Åbn http://localhost:3000.

## Teknologi

- Next.js (App Router) + TypeScript
- Tailwind CSS (dark mode default)
- LocalStorage til al datapersistering (ingen server, ingen konto)
