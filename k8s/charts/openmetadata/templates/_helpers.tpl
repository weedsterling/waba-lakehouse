{{- define "om.selector" -}}
app.kubernetes.io/name: {{ . }}
{{- end }}

{{/* Variables de openmetadata.yaml (image officielle) ; secrets depuis waba-openmetadata (.env). */}}
{{- define "om.env" -}}
- { name: OPENMETADATA_CLUSTER_NAME, value: waba }
- { name: OPENMETADATA_HEAP_OPTS, value: {{ .Values.server.heap | quote }} }
- { name: DB_DRIVER_CLASS, value: org.postgresql.Driver }
- { name: DB_SCHEME, value: postgresql }
- { name: DB_PARAMS, value: "allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC" }
- { name: DB_USE_SSL, value: "false" }
- { name: DB_HOST, value: openmetadata-postgres }
- { name: DB_PORT, value: "5432" }
- { name: DB_USER, value: openmetadata }
- name: DB_USER_PASSWORD
  valueFrom: { secretKeyRef: { name: waba-openmetadata, key: DB_USER_PASSWORD } }
- { name: OM_DATABASE, value: openmetadata_db }
- { name: SEARCH_TYPE, value: elasticsearch }
- { name: ELASTICSEARCH_HOST, value: openmetadata-search }
- { name: ELASTICSEARCH_PORT, value: "9200" }
- { name: ELASTICSEARCH_SCHEME, value: http }
- { name: PIPELINE_SERVICE_CLIENT_ENABLED, value: "false" }   # ingestion par CronJob, pas d'Airflow dédié
- { name: AUTHENTICATION_PROVIDER, value: basic }
- { name: AUTHORIZER_ADMIN_PRINCIPALS, value: "[admin]" }
- { name: AUTHORIZER_PRINCIPAL_DOMAIN, value: open-metadata.org }
- name: FERNET_KEY                                          # chiffrement des secrets de connexion stockés
  valueFrom: { secretKeyRef: { name: waba-openmetadata, key: FERNET_KEY } }
- { name: RSA_PUBLIC_KEY_FILE_PATH, value: /etc/om-jwt/public_key.der }
- { name: RSA_PRIVATE_KEY_FILE_PATH, value: /etc/om-jwt/private_key.der }
- { name: JWT_KEY_ID, value: waba-openmetadata }
{{- end }}
