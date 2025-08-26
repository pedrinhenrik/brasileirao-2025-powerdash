CREATE TABLE IF NOT EXISTS dim_team (
  team_key INTEGER PRIMARY KEY,
  team_api_id INTEGER,
  nome VARCHAR,
  sigla VARCHAR,
  cidade VARCHAR,
  uf VARCHAR,
  estadio VARCHAR,
  url_escudo VARCHAR
);
