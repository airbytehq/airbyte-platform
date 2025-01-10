{{- define "imageUrl" -}}
{{- $img := index . 0 -}}
{{- $root := index . 1 -}}

{{- $reg := $root.Values.global.image.registry -}}
{{/* ensure the registry has a trailing slash, if set */}}
{{- $reg = (ternary $reg (printf "%s/" (trimSuffix "/" $reg)) (empty $reg)) -}}

{{/* some images are defined as a string instead of an object (busybox, curl, connector sidecar, etc) */}}
{{- if (eq (typeOf $img) "string") -}}
{{- printf "%s%s" $reg (tpl $img $root) -}}
{{- else -}}
{{- $tag := coalesce $img.tag $root.Values.global.image.tag $root.Chart.AppVersion -}}
{{- printf "%s%s:%s" $reg $img.repository $tag -}}
{{- end -}}

{{- end -}}
