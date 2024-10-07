{{- define "imageUrl" -}}
{{- $img := index . 0 -}}
{{- $root := index . 1 -}}

{{/* some images are defined as a string instead of an object (busybox, curl, connector sidecar, etc) */}}
{{- if (eq (typeOf $img) "string") -}}
{{- printf "%s%s" $root.Values.global.image.registry (tpl $img $root) | quote -}}
{{- else -}}
{{- $tag := coalesce $img.tag $root.Values.global.image.tag $root.Chart.AppVersion -}}
{{- $reg := default "" (coalesce $img.registry $root.Values.global.image.registry) -}}
{{- printf "%s%s:%s" $reg $img.repository $tag | quote -}}
{{- end -}}

{{- end -}}