#!/usr/bin/env bash
# =============================================================================
# Préparation d'une VM Ubuntu Server 24.04 pour le challenge WABA Lakehouse.
# Idempotent : peut être relancé sans risque.
#
#   ./scripts/vm-setup.sh              # Levels 1-3 : Docker Engine, Git, Java 17, Python
#   ./scripts/vm-setup.sh --with-k8s   # + Level 4 : kubectl, Minikube, Helm, k9s
#
# Après la 1re exécution : se déconnecter/reconnecter (groupe docker).
# =============================================================================
set -euo pipefail

WITH_K8S=false
[[ "${1:-}" == "--with-k8s" ]] && WITH_K8S=true
K8S_MINOR="v1.33"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

if [[ "$(. /etc/os-release && echo "$ID")" != "ubuntu" ]]; then
  echo "Ce script cible Ubuntu (24.04 recommandé)." >&2; exit 1
fi

log "Paquets de base"
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg git make jq unzip htop \
  openjdk-17-jdk-headless python3 python3-venv python3-pip openssh-server

log "Docker Engine + Compose (dépôt officiel Docker)"
if ! command -v docker >/dev/null 2>&1; then
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
sudo usermod -aG docker "$USER"
sudo systemctl enable --now docker

log "Rotation des logs Docker (évite de saturer le disque de la VM)"
if [[ ! -f /etc/docker/daemon.json ]]; then
  echo '{ "log-driver": "json-file", "log-opts": { "max-size": "50m", "max-file": "3" } }' \
    | sudo tee /etc/docker/daemon.json >/dev/null
  sudo systemctl restart docker
fi

log "Réglages noyau (Kafka, OpenSearch/OpenMetadata, watchers de fichiers)"
sudo tee /etc/sysctl.d/99-waba.conf >/dev/null <<'EOF'
vm.max_map_count=262144
fs.inotify.max_user_watches=524288
fs.inotify.max_user_instances=512
EOF
sudo sysctl --system >/dev/null

git config --global core.autocrlf input

if $WITH_K8S; then
  log "kubectl ${K8S_MINOR}"
  if ! command -v kubectl >/dev/null 2>&1; then
    curl -fsSL "https://pkgs.k8s.io/core:/stable:/${K8S_MINOR}/deb/Release.key" \
      | sudo gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
    echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/${K8S_MINOR}/deb/ /" \
      | sudo tee /etc/apt/sources.list.d/kubernetes.list >/dev/null
    sudo apt-get update -y && sudo apt-get install -y kubectl
  fi

  log "Minikube"
  if ! command -v minikube >/dev/null 2>&1; then
    curl -fsSLo /tmp/minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install /tmp/minikube /usr/local/bin/minikube && rm /tmp/minikube
  fi

  log "Helm 3"
  command -v helm >/dev/null 2>&1 || curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

  log "k9s (supervision du cluster)"
  if ! command -v k9s >/dev/null 2>&1; then
    curl -fsSL https://github.com/derailed/k9s/releases/latest/download/k9s_Linux_amd64.tar.gz \
      | sudo tar -xz -C /usr/local/bin k9s
  fi
fi

log "Versions installées"
docker --version; docker compose version; git --version; java -version 2>&1 | head -1; python3 --version
if $WITH_K8S; then kubectl version --client | head -1; minikube version --short; helm version --short; fi

cat <<EOF

✅ VM prête. Ressources visibles : $(nproc) vCPU, $(free -g | awk '/Mem:/{print $2}') Go RAM, $(df -h / | awk 'NR==2{print $4}') libres.
   IP de la VM : $(hostname -I | awk '{print $1}')
   ➜ Déconnectez-vous puis reconnectez-vous (groupe docker), puis : docker run --rm hello-world
EOF
