#!/usr/bin/env bash
# =============================================================================
# Corrige la résolution DNS des conteneurs Docker (dont le nœud Minikube).
#
# Symptôme : « lookup ghcr.io on 192.168.49.1:53: server misbehaving » (SERVFAIL) dans le cluster,
# alors que la VM résout correctement. Cause : le DNS interne de Docker relaie vers le résolveur
# local de la VM (systemd-resolved, 127.0.0.53), qui n'est pas joignable de façon fiable depuis le
# réseau du conteneur. Correctif : donner à Docker des résolveurs publics FIXES.
#
# Volontairement SANS le DNS de la passerelle NAT VMware : son adresse (192.168.<sous-réseau>.2) change
# à chaque redémarrage du service NAT de l'hôte ; l'inclure rendait la configuration instable, et chaque
# changement entraînait une reconfiguration de Docker (cause des pertes de cluster du 08-09/10).
# Surcharge possible (réseau d'entreprise filtrant le DNS public) : WABA_DOCKER_DNS="10.0.0.53 10.0.0.54"
# Idempotent : ne modifie /etc/docker/daemon.json (en conservant ses autres clés) que si nécessaire.
# Code de retour 10 : configuration modifiée (Docker redémarré ; le cluster Minikube est conservé).
# =============================================================================
set -euo pipefail
servers=${WABA_DOCKER_DNS:-"1.1.1.1 8.8.8.8 9.9.9.9"}
json=$(printf '%s\n' $servers | python3 -c 'import json,sys; print(json.dumps([l.strip() for l in sys.stdin if l.strip()]))')

current=$(sudo python3 -c 'import json,os; p="/etc/docker/daemon.json"; print(json.dumps(json.load(open(p)).get("dns", [])) if os.path.exists(p) else "[]")')
if [[ "$current" == "$json" ]]; then
  echo "DNS Docker déjà configuré : $json"; exit 0
fi
sudo python3 - "$json" <<'PY'
import json, os, sys
p = "/etc/docker/daemon.json"
cfg = json.load(open(p)) if os.path.exists(p) else {}
cfg["dns"] = json.loads(sys.argv[1])
json.dump(cfg, open(p, "w"), indent=2)
PY
echo "DNS Docker configuré : $json -> redémarrage de Docker"
sudo systemctl restart docker
exit 10
