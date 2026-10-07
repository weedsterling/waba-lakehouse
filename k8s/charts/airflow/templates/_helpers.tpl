{{/* Variables d'environnement communes à tous les composants Airflow (aucun secret en clair). */}}
{{- define "airflow.env" -}}
- name: AIRFLOW_DB_PASSWORD
  valueFrom: { secretKeyRef: { name: waba-airflow, key: AIRFLOW_DB_PASSWORD } }
- name: LAKEHOUSE_ACCESS_KEY
  valueFrom: { secretKeyRef: { name: waba-s3, key: LAKEHOUSE_ACCESS_KEY } }
- name: LAKEHOUSE_SECRET_KEY
  valueFrom: { secretKeyRef: { name: waba-s3, key: LAKEHOUSE_SECRET_KEY } }
- name: AIRFLOW__CORE__FERNET_KEY
  valueFrom: { secretKeyRef: { name: waba-airflow, key: AIRFLOW_FERNET_KEY } }
- name: AIRFLOW__API_AUTH__JWT_SECRET
  valueFrom: { secretKeyRef: { name: waba-airflow, key: AIRFLOW_JWT_SECRET } }
- name: AIRFLOW__API__SECRET_KEY
  valueFrom: { secretKeyRef: { name: waba-airflow, key: AIRFLOW_API_SECRET_KEY } }
# $(VAR) : expansion par Kubernetes des variables déclarées plus haut (secrets jamais écrits dans le chart)
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  value: postgresql+psycopg2://airflow:$(AIRFLOW_DB_PASSWORD)@airflow-postgres:5432/airflow
- name: AIRFLOW__CORE__EXECUTION_API_SERVER_URL
  value: http://airflow-api-server.{{ .Release.Namespace }}.svc.cluster.local:8080/execution/
- name: AIRFLOW_CONN_MINIO_S3
  value: '{"conn_type": "aws", "login": "$(LAKEHOUSE_ACCESS_KEY)", "password": "$(LAKEHOUSE_SECRET_KEY)", "extra": {"endpoint_url": "{{ .Values.s3Endpoint }}", "region_name": "{{ .Values.region }}"}}'
{{- range $k, $v := .Values.config }}
- { name: {{ $k }}, value: {{ $v | quote }} }
{{- end }}
{{- end }}

{{/* Attente du schéma de la base (migré par le Job airflow-init) avant de démarrer un composant. */}}
{{- define "airflow.waitForMigrations" -}}
- name: wait-for-migrations
  image: {{ .Values.image }}
  imagePullPolicy: Never
  args: ["db", "check-migrations", "--migration-wait-timeout=900"]
  env:
    {{- include "airflow.env" . | nindent 4 }}
{{- end }}

{{/*
DAGs : ConfigMaps waba-airflow-dags (airflow/dags/*.py) et waba-airflow-lib (airflow/dags/waba/*.py),
créées par bootstrap.sh, recopiées dans un emptyDir -> même arborescence que Docker Compose
(paquet waba/ sous le dossier des DAGs), sans les dossiers techniques ..data des volumes ConfigMap.
*/}}
{{- define "airflow.dagsInit" -}}
- name: copy-dags
  image: {{ .Values.initImage }}
  command: ["sh", "-c", "cp /src/dags/*.py /dags/ && mkdir -p /dags/waba && cp /src/lib/*.py /dags/waba/ && ls -R /dags"]
  volumeMounts:
    - { name: src-dags, mountPath: /src/dags }
    - { name: src-lib, mountPath: /src/lib }
    - { name: dags, mountPath: /dags }
{{- end }}

{{- define "airflow.dagsVolumes" -}}
- name: src-dags
  configMap: { name: waba-airflow-dags }
- name: src-lib
  configMap: { name: waba-airflow-lib }
- name: dags
  emptyDir: {}
- name: spark-template                  # rendu par le chart spark-jobs, relu à chaque soumission
  configMap: { name: spark-batch-template }
{{- end }}

{{- define "airflow.dagsMounts" -}}
- { name: dags, mountPath: /opt/airflow/dags }
- { name: spark-template, mountPath: /opt/waba/k8s, readOnly: true }
{{- end }}

{{/*
Empreinte du code des DAGs (ConfigMaps lues dans le cluster au déploiement) : bootstrap.sh met à jour
les ConfigMaps, `helmfile sync` redémarre alors le scheduler et le dag-processor, et eux seuls.
*/}}
{{- define "airflow.codeChecksum" -}}
{{- $d := lookup "v1" "ConfigMap" .Release.Namespace "waba-airflow-dags" | default dict -}}
{{- $l := lookup "v1" "ConfigMap" .Release.Namespace "waba-airflow-lib" | default dict -}}
{{- printf "%s|%s" (get $d "data" | default dict | toJson) (get $l "data" | default dict | toJson) | sha256sum -}}
{{- end }}
