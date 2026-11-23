{{/* Labels every object carries. Call with (dict "root" $ "name" "order-service" "component" "service") */}}
{{- define "orderflow.labels" -}}
{{ include "orderflow.selectorLabels" . }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/part-of: orderflow
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}

{{/* The labels a Deployment selects its pods by. Never change them on a running release. */}}
{{- define "orderflow.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{/* Secret with the database login */}}
{{- define "orderflow.databaseSecret" -}}
{{- .Values.database.existingSecret | default "orderflow-database" -}}
{{- end }}

{{/* Pod-level security settings shared by the apps */}}
{{- define "orderflow.podSecurity" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/* Container-level security settings shared by the apps */}}
{{- define "orderflow.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}

{{/* A Service. Call with (dict "root" $ "name" "mailpit" "component" "..." "ports" (list (dict "name" "http" "port" 8025 "nodePort" 30825))) */}}
{{- define "orderflow.service" -}}
{{- $nodePorts := false -}}
{{- range .ports }}{{ if .nodePort }}{{ $nodePorts = true }}{{ end }}{{ end -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .name }}
  labels:
    {{- include "orderflow.labels" . | nindent 4 }}
spec:
  type: {{ if $nodePorts }}NodePort{{ else }}ClusterIP{{ end }}
  {{- if .headless }}
  clusterIP: None
  {{- end }}
  selector:
    {{- include "orderflow.selectorLabels" . | nindent 4 }}
  ports:
    {{- range .ports }}
    - name: {{ .name }}
      port: {{ .port }}
      targetPort: {{ .targetPort | default .name }}
      {{- if .nodePort }}
      nodePort: {{ .nodePort }}
      {{- end }}
    {{- end }}
{{- end }}
