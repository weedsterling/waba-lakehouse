#!/usr/bin/env bash
# =============================================================================
# Installe les outils Kubernetes du Level 4 (versions épinglées, sommes de contrôle vérifiées).
#   kubectl · minikube · helm · helmfile          -> /usr/local/bin
# Idempotent : un outil déjà présent dans la bonne version n'est pas retéléchargé.
# =============================================================================
set -euo pipefail
KUBECTL=v1.33.1
MINIKUBE=v1.36.0
HELM=v3.18.4
HELMFILE=1.1.3
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
cd "$TMP"

fetch() { curl -fsSL --retry 10 --retry-all-errors --retry-delay 3 -o "$2" "$1"; }
verify() {  # fichier somme_attendue
  [[ "$(sha256sum "$1" | cut -d' ' -f1)" == "$2" ]] || { echo "✘ somme SHA-256 invalide : $1" >&2; exit 1; }
}

if ! kubectl version --client 2>/dev/null | grep -q "$KUBECTL"; then
  fetch "https://dl.k8s.io/release/$KUBECTL/bin/linux/amd64/kubectl" kubectl
  verify kubectl "$(curl -fsSL "https://dl.k8s.io/release/$KUBECTL/bin/linux/amd64/kubectl.sha256")"
  sudo install -m 0755 kubectl /usr/local/bin/kubectl
fi
if ! minikube version 2>/dev/null | grep -q "$MINIKUBE"; then
  fetch "https://storage.googleapis.com/minikube/releases/$MINIKUBE/minikube-linux-amd64" minikube
  verify minikube "$(curl -fsSL "https://storage.googleapis.com/minikube/releases/$MINIKUBE/minikube-linux-amd64.sha256")"
  sudo install -m 0755 minikube /usr/local/bin/minikube
fi
if ! helm version 2>/dev/null | grep -q "$HELM"; then
  fetch "https://get.helm.sh/helm-$HELM-linux-amd64.tar.gz" helm.tgz
  verify helm.tgz "$(curl -fsSL "https://get.helm.sh/helm-$HELM-linux-amd64.tar.gz.sha256sum" | cut -d' ' -f1)"
  tar xzf helm.tgz && sudo install -m 0755 linux-amd64/helm /usr/local/bin/helm
fi
if ! helmfile --version 2>/dev/null | grep -q "$HELMFILE"; then
  base="https://github.com/helmfile/helmfile/releases/download/v$HELMFILE"
  fetch "$base/helmfile_${HELMFILE}_linux_amd64.tar.gz" helmfile.tgz
  verify helmfile.tgz "$(curl -fsSL "$base/helmfile_${HELMFILE}_checksums.txt" | grep "linux_amd64.tar.gz" | cut -d' ' -f1)"
  tar xzf helmfile.tgz helmfile && sudo install -m 0755 helmfile /usr/local/bin/helmfile
fi
echo "Outils prêts :"
kubectl version --client | head -1; minikube version | head -1; helm version --short; helmfile --version
