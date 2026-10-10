{{/*
SparkApplication WABA : une seule définition pour les flux permanents (apps.yaml), le modèle des jobs
batch (batch-template.yaml, utilisé par Airflow) et scripts/k8s/spark-run.sh.
Appel : include "waba.sparkapp" (dict "app" <application> "root" $)
Les jobs batch (kind: batch) héritent des valeurs par défaut .Values.batch.
*/}}
{{- define "waba.sparkapp" -}}
{{- $root := .root -}}
{{- $app := .app -}}
{{- if eq $app.kind "batch" }}{{ $app = merge (deepCopy $app) (deepCopy $root.Values.batch) }}{{ end -}}
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: {{ $app.name }}
  labels:
    app.kubernetes.io/part-of: waba
    waba.io/kind: {{ $app.kind }}
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: {{ $root.Values.image }}
  imagePullPolicy: Never
  mainApplicationFile: local:///opt/waba/jobs/{{ $app.file }}
  sparkVersion: "3.5.3"
  {{- if $app.args }}
  arguments:
  {{- range $app.args }}
    - {{ . | quote }}
  {{- end }}
  {{- end }}
  {{- if $app.ttl }}
  timeToLiveSeconds: {{ $app.ttl }}
  {{- end }}
  restartPolicy:
    type: {{ $app.restart }}
    {{- if eq $app.restart "Always" }}
    onFailureRetryInterval: 30
    onSubmissionFailureRetryInterval: 30
    {{- end }}
  sparkConf:
  {{- range $k, $v := $root.Values.sparkConf }}
    {{ $k }}: {{ $v | quote }}
  {{- end }}
  {{- range $k, $v := $app.conf }}
    {{ $k }}: {{ $v | quote }}
  {{- end }}
  volumes:
    - name: jobs
      configMap: { name: waba-spark-jobs }
    - name: lib
      configMap: { name: waba-spark-lib }
  driver:
    cores: 1
    coreRequest: {{ $root.Values.coreRequest.driver | quote }}
    memory: {{ $app.driverMemory | quote }}
    serviceAccount: {{ $root.Values.serviceAccount }}
    labels: { app.kubernetes.io/part-of: waba, waba.io/app: {{ $app.name }} }
    {{- if eq $app.kind "streaming" }}
    # Empreinte du code monté (ConfigMaps) : un changement de code modifie la spec -> l'opérateur relance le
    # flux, qui reprend exactement depuis son checkpoint. Sans elle, un flux permanent garderait l'ancien code.
    annotations: { waba.io/code-checksum: {{ include "waba.codeChecksum" $root | quote }} }
    {{- end }}
    env:
    {{- range $k, $v := $root.Values.env.plain }}
      - { name: {{ $k }}, value: {{ $v | quote }} }
    {{- end }}
    {{- range $k, $v := $root.Values.env.secrets }}
      - name: {{ $k }}
        valueFrom: { secretKeyRef: { name: {{ $v.secret }}, key: {{ $v.key }} } }
    {{- end }}
    volumeMounts:
      - { name: jobs, mountPath: /opt/waba/jobs }
      - { name: lib, mountPath: /opt/waba/waba_spark }
  executor:
    instances: {{ $app.executorInstances }}
    cores: {{ $app.executorCores }}
    coreRequest: {{ $root.Values.coreRequest.executor | quote }}
    memory: {{ $app.executorMemory | quote }}
    labels: { app.kubernetes.io/part-of: waba, waba.io/app: {{ $app.name }} }
    env:
    {{- range $k, $v := $root.Values.env.plain }}
      - { name: {{ $k }}, value: {{ $v | quote }} }
    {{- end }}
    {{- range $k, $v := $root.Values.env.secrets }}
      - name: {{ $k }}
        valueFrom: { secretKeyRef: { name: {{ $v.secret }}, key: {{ $v.key }} } }
    {{- end }}
    volumeMounts:
      - { name: jobs, mountPath: /opt/waba/jobs }
      - { name: lib, mountPath: /opt/waba/waba_spark }
{{- end }}

{{/* Empreinte du code Spark monté dans les pods (ConfigMaps créées par scripts/k8s/bootstrap.sh) */}}
{{- define "waba.codeChecksum" -}}
{{- $j := lookup "v1" "ConfigMap" .Release.Namespace "waba-spark-jobs" | default dict -}}
{{- $l := lookup "v1" "ConfigMap" .Release.Namespace "waba-spark-lib" | default dict -}}
{{- printf "%s|%s" (get $j "data" | default dict | toJson) (get $l "data" | default dict | toJson) | sha256sum | trunc 16 -}}
{{- end }}
