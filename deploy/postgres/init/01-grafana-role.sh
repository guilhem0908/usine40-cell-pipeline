#!/bin/sh
# Read-only role for Grafana. The collector creates the tables at start-up;
# pg_read_all_data covers them whenever they appear.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE grafana LOGIN PASSWORD '${GRAFANA_DB_PASSWORD}';
GRANT pg_read_all_data TO grafana;
SQL
