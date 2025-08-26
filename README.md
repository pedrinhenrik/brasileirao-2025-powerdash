# Brasileirão 2025 PowerDash

Dashboard de portfólio com ingestão via API e visualização no Power BI.

## Quick start
1. Copie `.env.example` para `.env` e preencha as variáveis.
2. Crie e ative um ambiente virtual.
3. `pip install -r requirements.txt`
4. `python src/ingest/fetch_standings.py`

## Estrutura
- `src/` scripts de ingestão e utilidades
- `data/` dados brutos e normalizados
- `dwh/ddl/` DDL do modelo analítico
- `.github/workflows/` automação opcional
