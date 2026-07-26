{{- define "metrics-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "metrics-agent.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "metrics-agent.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "metrics-agent.labels" -}}
helm.sh/chart: {{ include "metrics-agent.chart" . }}
{{ include "metrics-agent.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "metrics-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "metrics-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "metrics-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "metrics-agent.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "metrics-agent.vmagentName" -}}
{{- default (include "metrics-agent.fullname" .) .Values.vmagent.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "metrics-agent.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{- define "metrics-agent.httpPort" -}}
{{- if .Values.ui.enabled -}}
{{- default .Values.service.port .Values.ui.port -}}
{{- else -}}
{{- .Values.service.port -}}
{{- end -}}
{{- end }}
