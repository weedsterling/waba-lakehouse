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

# RÈGLE : ce script ne détruit JAMAIS le cluster de lui-même (il contient les données du lakehouse :
# MinIO, catalogue, bases Airflow/Superset). Une recréation exige WABA_RECREATE_CLUSTER=1, explicitement.

# Prérequis réseau : DNS fiable pour les conteneurs. Un changement de configuration redémarre Docker,
# ce qui arrête le nœud Minikube : il est simplement redémarré plus bas (données conservées).
rc=0; ./scripts/k8s/docker-dns.sh || rc=$?
if [[ $rc -ne 0 && $rc -ne 10 ]]; then
  exit $rc
fi

if [[ "${WABA_RECREATE_CLUSTER:-0}" == "1" ]]; then
  echo "WABA_RECREATE_CLUSTER=1 : suppression du cluster existant et de TOUTES ses données"
  minikube delete
fi

# Cluster existant dans une autre version : Minikube refuse de rétrograder -> arrêt, décision humaine
if minikube status >/dev/null 2>&1; then
  current=$(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])' || true)
  if [[ -n "$current" && "$current" != "$K8S_VERSION" ]]; then
    echo "✘ cluster en $current, version attendue $K8S_VERSION. Pour le recréer (perte des données) :" >&2
    echo "  WABA_RECREATE_CLUSTER=1 ./scripts/k8s/cluster-up.sh" >&2
    exit 1
  fi
fi

# Mémoire du nœud = RAM de la VM - 4 Go. Minikube la fige à la création du conteneur : si la VM a reçu
# plus de RAM depuis, la limite du conteneur existant est relevée à chaud (docker update), sans recréation.
total_mb=$(free -m | awk '/^Mem:/ {print $2}')
mem=$(( total_mb - 4096 ))
if docker inspect minikube >/dev/null 2>&1; then
  have_mb=$(( $(docker inspect -f '{{.HostConfig.Memory}}' minikube) / 1048576 ))
  if (( have_mb > 0 && have_mb + 512 < mem )); then
    echo "Mémoire du nœud Minikube : ${have_mb} -> ${mem} Mo"
    docker update --memory "${mem}m" --memory-swap "${mem}m" minikube >/dev/null
  fi
fi

if ! minikube status >/dev/null 2>&1; then
  # Profil existant (VM redémarrée, Docker relancé) : minikube le redémarre tel quel, données comprises
  echo "Démarrage de Minikube : $(nproc) vCPU, ${mem} Mo"
  minikube start --driver=docker --kubernetes-version="$K8S_VERSION" \
    --cpus="$(nproc)" --memory="${mem}m" --addons=ingress,metrics-server
else
  echo "Minikube déjà démarré"
fi

# Contrôle bloquant : le cluster doit pouvoir télécharger des images
# (6 essais : une coupure réseau passagère de la VM ne doit pas interrompre un déploiement)
dns_ok() { minikube ssh -- "nslookup ghcr.io >/dev/null 2>&1 && nslookup registry.k8s.io >/dev/null 2>&1" 2>/dev/null; }
for try in 1 2 3 4 5 6; do dns_ok && break; echo "DNS du nœud indisponible (essai $try/6)…"; sleep 10; done
if ! dns_ok; then
  echo "✘ le nœud Minikube ne résout pas les registres d'images (DNS). Diagnostic :" >&2
  if getent hosts ghcr.io >/dev/null; then echo "  VM : résolution OK" >&2; else echo "  VM : résolution KO -> réseau de la VM coupé (NAT VMware, VPN ?)" >&2; fi
  docker run --rm --dns 1.1.1.1 busybox:1.36 nslookup ghcr.io >/dev/null 2>&1 \
    && echo "  conteneur -> 1.1.1.1 : OK (DNS Docker à revoir : scripts/k8s/docker-dns.sh)" >&2 \
    || echo "  conteneur -> 1.1.1.1 : KO -> DNS public bloqué : WABA_DOCKER_DNS=\"<DNS du réseau>\" scripts/k8s/docker-dns.sh" >&2
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

# Noms *.waba.local résolus DANS le cluster vers l'Ingress (réécriture CoreDNS) : le navigateur et les pods
# utilisent la même URL Keycloak (http://keycloak.waba.local), donc le même « issuer » dans les jetons OIDC.
corefile=$(kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}')
if ! grep -q 'waba\\.local' <<<"$corefile"; then
  python3 - "$corefile" > /tmp/waba-coredns.json <<'PY2'
import json, sys
rule = "    rewrite name regex (.+)\\.waba\\.local\\.$ ingress-nginx-controller.ingress-nginx.svc.cluster.local. answer auto"
lines = sys.argv[1].splitlines()
i = next(n for n, l in enumerate(lines) if l.strip().startswith(".:53"))
lines.insert(i + 1, rule)
print(json.dumps({"data": {"Corefile": "\n".join(lines) + "\n"}}))
PY2
  kubectl -n kube-system patch configmap coredns --type merge -p "$(cat /tmp/waba-coredns.json)" >/dev/null
  kubectl -n kube-system rollout restart deployment coredns >/dev/null
  kubectl -n kube-system rollout status deployment coredns --timeout=120s >/dev/null
  echo "✔ CoreDNS : *.waba.local -> Ingress"
fi

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
