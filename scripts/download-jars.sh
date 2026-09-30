#!/usr/bin/env bash
# =============================================================================
# Télécharge les JARs nécessaires à l'image Spark (Iceberg + S3A) dans spark/jars/
# AVANT le build Docker.
#
# Pourquoi hors du Dockerfile ?
#   * reprise des téléchargements interrompus (connexions lentes / instables) ;
#   * vérification d'intégrité SHA-1 contre Maven Central ;
#   * build Docker ensuite 100 % hors ligne et reproductible.
# Idempotent : relancer le script ne retélécharge que les fichiers manquants ou corrompus.
# =============================================================================
set -euo pipefail

DEST="$(cd "$(dirname "$0")/.." && pwd)/spark/jars"
MVN="https://repo1.maven.org/maven2"
JARS=(
  "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
  "org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar"
  "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
  "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
)

mkdir -p "$DEST"
for path in "${JARS[@]}"; do
  name="$(basename "$path")"
  file="$DEST/$name"
  expected="$(curl -fsSL --retry 10 --retry-all-errors --retry-delay 3 "$MVN/$path.sha1" | cut -d' ' -f1)"

  if [[ -f "$file" && "$(sha1sum "$file" | cut -d' ' -f1)" == "$expected" ]]; then
    echo "✔ déjà présent et vérifié : $name"
    continue
  fi

  echo "↓ téléchargement : $name"
  # -C - : reprend là où le téléchargement s'est arrêté ; --retry : relance automatique
  for attempt in 1 2 3 4 5; do
    if curl -fL --retry 20 --retry-all-errors --retry-delay 5 -C - -o "$file" "$MVN/$path"; then
      break
    fi
    echo "  nouvelle tentative ($attempt/5)…"; sleep 5
  done

  actual="$(sha1sum "$file" | cut -d' ' -f1)"
  if [[ "$actual" != "$expected" ]]; then
    echo "✘ somme de contrôle invalide pour $name (fichier supprimé, relancez le script)" >&2
    rm -f "$file"; exit 1
  fi
  echo "✔ vérifié : $name"
done

echo -e "\nTous les JARs sont prêts dans spark/jars/ :"
ls -lh "$DEST"
