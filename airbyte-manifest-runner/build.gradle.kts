plugins {
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.kube-reload")
}

airbyte {
  docker {
    imageName = "manifest-runner"
  }
  kubeReload {
    deployment = "ab-manifest-runner"
    container = "airbyte-manifest-runner"
  }
}

val copyPythonFiles =
  tasks.register<Copy>("copyPythonFiles") {
    from("$projectDir") {
      include("app/**")
      include("pyproject.toml")
      include("uv.lock")
    }
    into("${project.layout.buildDirectory.get()}/airbyte/docker/")
  }

tasks.named("dockerCopyDistribution") {
  dependsOn(copyPythonFiles)
}
