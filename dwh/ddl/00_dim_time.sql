CREATE TABLE IF NOT EXISTS dim_time (
  date_key INTEGER PRIMARY KEY,
  data DATE,
  ano INTEGER,
  mes INTEGER,
  dia INTEGER,
  semana INTEGER,
  nome_mes VARCHAR
);
