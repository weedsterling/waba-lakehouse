{{- define "superset.env" -}}
- { name: SUPERSET_CONFIG_PATH, value: /app/waba/config/superset_config.py }
- name: SUPERSET_SECRET_KEY
  valueFrom: { secretKeyRef: { name: waba-superset, key: SUPERSET_SECRET_KEY } }
- name: SUPERSET_DB_PASSWORD
  valueFrom: { secretKeyRef: { name: waba-superset, key: SUPERSET_DB_PASSWORD } }
- { name: KEYCLOAK_URL, value: {{ .Values.keycloakUrl | quote }} }
- name: SUPERSET_OIDC_SECRET
  valueFrom: { secretKeyRef: { name: waba-superset, key: SUPERSET_OIDC_SECRET } }
{{- end }}

{{- define "superset.volumes" -}}
- name: config
  configMap: { name: superset-config }
- name: dashboards
  configMap: { name: superset-dashboards }
{{- end }}

{{- define "superset.mounts" -}}
- { name: config, mountPath: /app/waba/config, readOnly: true }
- { name: dashboards, mountPath: /app/waba/dashboards, readOnly: true }
{{- end }}

{{/* Empreinte de la configuration (ConfigMaps créées par bootstrap.sh) : redémarrage si elle change */}}
{{- define "superset.checksum" -}}
{{- $c := lookup "v1" "ConfigMap" .Release.Namespace "superset-config" | default dict -}}
{{- $d := lookup "v1" "ConfigMap" .Release.Namespace "superset-dashboards" | default dict -}}
{{- printf "%s|%s" (get $c "data" | default dict | toJson) (get $d "data" | default dict | toJson) | sha256sum -}}
{{- end }}
