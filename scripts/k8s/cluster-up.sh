#!/usr/bin/env bash
# =============================================================================
# Démarre (ou réutilise) le cluster Minikube et y charge les images WABA construites localement.
# Mémoire : RAM de la VM moins 4 Go (système + Docker) ; tous les vCPU.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
# Version de Kubernetes ÉPINGLÉE sur la matrice de compatibilité des opérateurs : Kubernetes 1.33 ajoute
# des champs à /version (emulationMajor…) que le client fabric8 de Strimzi 0.45 refuse
# (« UnrecognizedPropertyException », l'opérateur redémarre en boucle). 1.32 est supportée par tous.
K8S_VERSION=v1.32.5

# Prérequis réseau : DNS fiable pour les conteneurs. Si la configuration Docker change, le nœud
# Minikube (créé avec l'ancienne) est recréé — sans perte, le cluster ne contient encore rien d'utile.
rc=0; ./scripts/k8s/docker-dns.sh || rc=$?
if [[ $rc -eq 10 ]]; then
  minikube delete >/dev/null 2>&1 || true
elif [[ $rc -ne 0 ]]; then
  exit $rc
fi

# Cluster existant dans une autre version : Minikube refuse de rétrograder -> recréation
if minikube status >/dev/null 2>&1; then
  current=$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])' || true)
  if [[ -n "$current" && "$current" != "$K8S_VERSION" ]]; then
    echo "Cluster en $current, version attendue $K8S_VERSION : recréation du cluster"
    minikube delete
  fi
fi

if ! minikube status >/dev/null 2>&1; then
  total_mb=$(free -m | awk '/^Mem:/ {print $2}')
  mem=$(( total_mb - 4096 ))
  echo "Démarrage de Minikube : $(nproc) vCPU, ${mem} Mo"
  minikube start --driver=docker --kubernetes-version="$K8S_VERSION" \
    --cpus="$(nproc)" --memory="${mem}m" --addons=ingress,metrics-server
else
  echo "Minikube déjà démarré"
fi

# Contrôle bloquant : le cluster doit pouvoir télécharger des images
if ! minikube ssh -- "nslookup ghcr.io >/dev/null 2>&1 && nslookup registry.k8s.io >/dev/null 2>&1"; then
  echo "✘ le nœud Minikube ne résout pas les registres d'images (DNS) : voir scripts/k8s/docker-dns.sh" >&2
  exit 1
fi
echo "✔ DNS du cluster opérationnel"

# Pods bloqués par un téléchargement échoué AVANT la correction réseau : relancés immédiatement
# (sinon Kubernetes attend jusqu'à 5 min entre deux tentatives).
kubectl get pods -A --no-headers 2>/dev/null | awk '$4 ~ /ImagePull|ErrImage/ {print $1, $2}' \
  | while read -r ns pod; do kubectl -n "$ns" delete pod "$pod" --wait=false; done

# Le contrôleur Ingress valide chaque objet Ingress (webhook) : il doit être prêt avant tout déploiement
minikube addons enable ingress >/dev/null 2>&1 || true
echo "attente du contrôleur Ingress…"
kubectl -n ingress-nginx wait --for=condition=ready pod \
  -l app.kubernetes.io/component=controller --timeout=600s
echo "✔ contrôleur Ingress prêt"

# Images construites par Docker Compose aux Levels 1-3, copiées dans le cluster (pas de registre).
# Liste complétée à chaque étape du Level 4 (9.3 : Spark/Airflow, 9.2 : générateur).
IMAGES=(${WABA_IMAGES:-})
for img in "${IMAGES[@]}"; do
  if docker image inspect "$img" >/dev/null 2>&1; then
    minikube image ls | grep -q "${img%%:*}:${img##*:}" || { echo "chargement de $img"; minikube image load "$img"; }
  else
    echo "ℹ image $img absente de la VM : elle sera téléchargée par le cluster"
  fi
done
kubectl get nodes -o wide
