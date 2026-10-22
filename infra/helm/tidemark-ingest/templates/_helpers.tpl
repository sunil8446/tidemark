{{- define "ingest.labels" -}}
app.kubernetes.io/name: tidemark-ingest
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: tidemark
{{- end }}
{{- define "ingest.selector" -}}
app.kubernetes.io/name: tidemark-ingest
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
