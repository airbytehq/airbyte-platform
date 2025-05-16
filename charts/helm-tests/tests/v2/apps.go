package tests

type App struct {
	Name    string
	Kind    string
	Release string
}

// Returns the fully qualified name of the application
func (a *App) FQN() string {
	return a.Release + "-" + a.Name
}

var apps = []struct {
	name string
	kind string
}{
	{name: "bootloader", kind: "Pod"},
	{name: "connector-builder-server", kind: "Deployment"},
	{name: "connector-rollout-worker", kind: "Deployment"},
	{name: "cron", kind: "Deployment"},
	{name: "featureflag-server", kind: "Deployment"},
	{name: "featureflag-server", kind: "Deployment"},
	{name: "keycloak", kind: "StatefulSet"},
	{name: "keycloak-setup", kind: "Job"},
	{name: "metrics", kind: "Deployment"},
	{name: "server", kind: "Deployment"},
	{name: "temporal", kind: "Deployment"},
	{name: "temporal-ui", kind: "Deployment"},
	{name: "temporal-ui", kind: "Deployment"},
	{name: "webapp", kind: "Deployment"},
	{name: "worker", kind: "Deployment"},
	{name: "workload-api-server", kind: "Deployment"},
	{name: "workload-launcher", kind: "Deployment"},
}

func appsForRelease(releaseName string) map[string]App {
	fqApps := make(map[string]App)
	for _, app := range apps {
		fqApps[app.name] = App{
			Name:    app.name,
			Kind:    app.kind,
			Release: releaseName,
		}
	}

	return fqApps
}
