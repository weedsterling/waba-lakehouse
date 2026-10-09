#!/usr/bin/env bash
# =============================================================================
# Complète le fichier .env : toute variable absente ou encore à "change-me…" reçoit
# une valeur aléatoire forte. Les secrets déjà définis ne sont JAMAIS modifiés.
# Usage : ./scripts/gen-secrets.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || cp .env.example .env

rand_hex() { openssl rand -hex "$1"; }
fernet()   { python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"; }

declare -A GEN=(
  [MINIO_ROOT_PASSWORD]="$(rand_hex 16)"
  [LAKEHOUSE_SECRET_KEY]="$(rand_hex 16)"
  [PII_HASH_SALT]="$(rand_hex 32)"
  [AIRFLOW_ADMIN_PASSWORD]="$(rand_hex 8)"
  [AIRFLOW_DB_PASSWORD]="$(rand_hex 16)"
  [AIRFLOW_FERNET_KEY]="$(fernet)"
  [AIRFLOW_JWT_SECRET]="$(rand_hex 32)"
  [AIRFLOW_API_SECRET_KEY]="$(rand_hex 32)"
  [NIFI_ADMIN_PASSWORD]="$(rand_hex 12)"
  [NIFI_SENSITIVE_PROPS_KEY]="$(rand_hex 16)"
  [ICEBERG_DB_PASSWORD]="$(rand_hex 16)"
  [SUPERSET_SECRET_KEY]="$(rand_hex 32)"
  [SUPERSET_DB_PASSWORD]="$(rand_hex 16)"
  [SUPERSET_ADMIN_PASSWORD]="$(rand_hex 8)"
)

# Ajoute les clés présentes dans .env.example mais absentes de .env (nouveaux niveaux)
while IFS= read -r line; do
  key="${line%%=*}"
  grep -q "^${key}=" .env || echo "$line" >> .env
done < <(grep -E '^[A-Z_]+=' .env.example)

for key in "${!GEN[@]}"; do
  current="$(grep -E "^${key}=" .env | cut -d= -f2- || true)"
  if [[ -z "$current" || "$current" == change-me* ]]; then
    sed -i "s|^${key}=.*|${key}=${GEN[$key]}|" .env
    echo "✔ $key généré"
  fi
done
chmod 600 .env
echo "Mot de passe de l'interface NiFi (utilisateur admin) : $(grep '^NIFI_ADMIN_PASSWORD=' .env | cut -d= -f2)"
echo "Mot de passe de l'interface Airflow (utilisateur admin) : $(grep '^AIRFLOW_ADMIN_PASSWORD=' .env | cut -d= -f2)"
echo "Mot de passe de l'interface Superset (utilisateur admin) : $(grep '^SUPERSET_ADMIN_PASSWORD=' .env | cut -d= -f2)"
