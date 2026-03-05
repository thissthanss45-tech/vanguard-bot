{{/*
_helpers.tpl — переиспользуемые именованные шаблоны Vanguard Bot Helm chart.
*/}}

{{/*─── Chart name ─────────────────────────────────────────────────────────*/}}
{{- define "vanguard-bot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*─── Fullname ────────────────────────────────────────────────────────────*/}}
{{- define "vanguard-bot.fullname" -}}
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

{{/*─── Chart label  ───────────────────────────────────────────────────────*/}}
{{- define "vanguard-bot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*─── Common labels ──────────────────────────────────────────────────────*/}}
{{- define "vanguard-bot.labels" -}}
helm.sh/chart: {{ include "vanguard-bot.chart" . }}
{{ include "vanguard-bot.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: vanguard-bot
{{- end }}

{{/*─── Selector labels ────────────────────────────────────────────────────*/}}
{{- define "vanguard-bot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vanguard-bot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*─── Full image reference ───────────────────────────────────────────────*/}}
{{- define "vanguard-bot.image" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*─── Namespace ──────────────────────────────────────────────────────────*/}}
{{- define "vanguard-bot.namespace" -}}
{{- .Values.namespace.name | default .Release.Namespace }}
{{- end }}

{{/*─── ServiceAccount name ───────────────────────────────────────────────*/}}
{{- define "vanguard-bot.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- .Values.serviceAccount.name | default (include "vanguard-bot.fullname" .) }}
{{- else }}
{{- .Values.serviceAccount.name | default "default" }}
{{- end }}
{{- end }}

{{/*─── Redis URL (внутренний service) ────────────────────────────────────*/}}
{{- define "vanguard-bot.redisUrl" -}}
{{- printf "redis://%s-redis:6379/0" (include "vanguard-bot.fullname" .) }}
{{- end }}
