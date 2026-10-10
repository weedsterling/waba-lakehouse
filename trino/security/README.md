# Trino — contrôle d'accès (file-based)

* `rules.json` : catalogues, schémas, tables, colonnes et filtres de lignes par utilisateur / groupe
  (première règle qui correspond l'emporte ; dernière règle = refus).
* `groups.txt` : groupes = rôles du realm Keycloak `waba` (même vocabulaire que Superset).
  En production : fournisseur de groupes LDAP / annuaire au lieu d'un fichier.

Montés par ConfigMap (`trino-security`, créée par `scripts/k8s/bootstrap.sh`) et relus toutes les 60 s :
une modification est appliquée sans redémarrer Trino.

Authentification : OAuth2 Keycloak pour l'interface web (https://trino.waba.local).
Le protocole client n'est exposé que dans le cluster (Superset, OpenMetadata, CLI) : Trino n'y authentifie
pas l'appelant (activer un authentificateur client fait refuser tout appel HTTP, et le client Python refuse
d'envoyer un mot de passe sans TLS) ; l'identité est portée par des comptes techniques en lecture seule. En production : TLS de bout en bout, mot de passe ou
JWT pour ces comptes et NetworkPolicy limitant l'accès au port 8080.
