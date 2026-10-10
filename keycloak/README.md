# Keycloak — realm `waba` (as code)

`waba-realm.json` est importé au premier démarrage (`--import-realm`), puis RÉAPPLIQUÉ à chaque déploiement
par le Job Helm `keycloak-realm-sync` (`sync_realm.py`, API d'administration, partialImport en écrasement) :
le realm reste aligné sur le dépôt (clients, rôles, utilisateurs).
Aucun secret dans ce fichier : `${SUPERSET_OIDC_SECRET}`, `${TRINO_OIDC_SECRET}` et `${KEYCLOAK_DEMO_PASSWORD}`
sont remplacés par Keycloak depuis l'environnement du pod (Secret `waba-keycloak`, issu de `.env`).

| Rôle du realm | Accès |
|---|---|
| `group_admin` | Superset Admin, Trino : tout |
| `country_analyst` + `country_XX` | 3 tableaux de bord limités au pays XX (RLS Superset) ; Trino : gold/silver filtrés sur XX, IBAN masqués |
| `compliance_officer` | tableau de bord Risque & conformité, SQL Lab sur gold/reporting ; Trino : gold/reporting |
| `viewer` | 3 tableaux de bord agrégés, sans SQL Lab ni export ; Trino : tables gold agrégées uniquement |

Utilisateurs de démonstration (mot de passe = `KEYCLOAK_DEMO_PASSWORD` de `.env`) :
`admin.groupe`, `analyste.ci`, `analyste.sn`, `conformite`, `lecteur`.

Modifier le realm : éditer ce fichier, puis `./scripts/k8s/bootstrap.sh && helmfile -f k8s/helmfile.yaml -l name=keycloak sync`.
Une modification faite dans la console (http://keycloak.waba.local) est écrasée au déploiement suivant.

Le client `trino` n'exige pas PKCE (le client OAuth2 de Trino 467 n'envoie pas de code_challenge) ;
le client `superset` l'exige (Authlib le gère).
