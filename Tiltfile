if (k8s_context() != 'colima-ab-control-plane' and k8s_context() != 'docker-desktop'):
  fail('!!! You are currently configured to use a non-local context. Please run `make k8s.up`')

load_dynamic('./airbyte-workers/Tiltfile')
load_dynamic('./airbyte-server/Tiltfile')
