"""Synchronise le realm Keycloak avec keycloak/waba-realm.json (réalm « as code », à chaque déploiement).

`--import-realm` n'importe le fichier qu'au PREMIER démarrage : une correction ultérieure du realm (client,
rôle, utilisateur) ne serait jamais appliquée. Ce script, lancé par un Job Helm après chaque installation /
mise à jour, applique le fichier sur le realm existant via l'API d'administration (partialImport, mode
OVERWRITE) : clients, rôles et utilisateurs reviennent à l'état du dépôt. Les ${VARIABLES} du fichier sont
remplacées par l'environnement (Secret waba-keycloak) ; aucune valeur secrète n'est écrite dans un log.

Stdlib uniquement :  python sync_realm.py /realm/waba-realm.json
Environnement : KEYCLOAK_URL, KC_BOOTSTRAP_ADMIN_USERNAME, KC_BOOTSTRAP_ADMIN_PASSWORD + variables du realm.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def substitute(text: str) -> str:
    def repl(m: re.Match) -> str:
        name = m.group(1)
        if name not in os.environ:
            raise SystemExit(f"variable {name} absente de l'environnement")
        return json.dumps(os.environ[name])[1:-1]          # échappement JSON de la valeur
    return re.sub(r"\$\{([A-Z0-9_]+)\}", repl, text)


def call(method: str, url: str, token: str | None = None, body=None, form=None):
    headers, data = {}, None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
    if form is not None:
        data, headers["Content-Type"] = urllib.parse.urlencode(form).encode(), "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read().decode()
    return json.loads(raw) if raw and raw[0] in "{[" else raw


def admin_token(base: str) -> str:
    form = {"client_id": "admin-cli", "grant_type": "password",
            "username": os.environ.get("KC_BOOTSTRAP_ADMIN_USERNAME", "admin"),
            "password": os.environ["KC_BOOTSTRAP_ADMIN_PASSWORD"]}
    for attempt in range(60):                              # Keycloak peut encore démarrer
        try:
            return call("POST", f"{base}/realms/master/protocol/openid-connect/token", form=form)["access_token"]
        except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
            print(f"attente de Keycloak ({attempt + 1}/60) : {exc}", flush=True)
            time.sleep(10)
    raise SystemExit("Keycloak injoignable")


def main(path: str) -> int:
    base = os.environ.get("KEYCLOAK_URL", "http://keycloak:8080").rstrip("/")
    realm = json.loads(substitute(open(path, encoding="utf-8").read()))
    name = realm["realm"]
    token = admin_token(base)
    try:
        call("GET", f"{base}/admin/realms/{name}", token)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        call("POST", f"{base}/admin/realms", token, realm)                 # realm absent : création complète
        print(f"realm {name} créé", flush=True)
        return 0
    # Réglages du realm (durées de session, politique de mot de passe…) puis objets en écrasement
    settings = {k: v for k, v in realm.items() if k not in ("roles", "clients", "users", "groups")}
    call("PUT", f"{base}/admin/realms/{name}", token, settings)
    result = call("POST", f"{base}/admin/realms/{name}/partialImport", token, {
        "ifResourceExists": "OVERWRITE", "roles": realm.get("roles", {}),
        "clients": realm.get("clients", []), "users": realm.get("users", [])})
    print(f"realm {name} synchronisé : {result.get('overwritten', 0)} objets mis à jour, "
          f"{result.get('added', 0)} ajoutés", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/realm/waba-realm.json"))
