{{- define "flink-application.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flink-application.fullname" -}}
{{- include "flink-application.name" . | trunc 45 | trimSuffix "-" -}}
{{- end -}}

{{- define "flink-application.labels" -}}
app.kubernetes.io/name: {{ include "flink-application.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: openshift-app-mode-lab
lab.xle2911.com/tenant: {{ .Values.tenant | quote }}
lab.xle2911.com/environment: {{ .Values.environment | quote }}
{{- end -}}
