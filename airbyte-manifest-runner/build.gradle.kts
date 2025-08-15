import ru.vyarus.gradle.plugin.python.task.PythonTask

plugins {
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.kube-reload")
  alias(libs.plugins.use.python)
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

val pythonVenvPath = ".gradle/python"

python {
  minPythonVersion = "3.10"
  pip("uv:0.8.2")
  // Use same venv directory as uv
  envPath = pythonVenvPath
}

tasks.register<PythonTask>("uvSync") {
  description = "Install uv and sync dependencies"
  dependsOn("pipInstall")

  command = "-m uv sync --group test --group dev"

  // Set UV_PROJECT_ENVIRONMENT to align with python plugin's envPath
  environment = mapOf("UV_PROJECT_ENVIRONMENT" to pythonVenvPath)

  inputs.file("pyproject.toml")
  inputs.file("uv.lock")
  outputs.dir(".venv")
}

tasks.register<PythonTask>("test") {
  description = "Run Python tests with pytest via uv"
  dependsOn("uvSync")

  command = "-m uv run pytest"

  // Set UV_PROJECT_ENVIRONMENT to align with python plugin's envPath
  environment = mapOf("UV_PROJECT_ENVIRONMENT" to pythonVenvPath)

  inputs.files(fileTree("app"), fileTree("tests"))
  inputs.file("pyproject.toml")
  inputs.file("uv.lock")
}

tasks.register<PythonTask>("format") {
  description = "Format Python code using ruff via uv"
  dependsOn("uvSync")

  command = "-m uv run ruff format"

  // Set UV_PROJECT_ENVIRONMENT to align with python plugin's envPath
  environment = mapOf("UV_PROJECT_ENVIRONMENT" to pythonVenvPath)

  inputs.files(fileTree("app"), fileTree("tests"))
  inputs.file("pyproject.toml")
  outputs.upToDateWhen { false }
}

tasks.named("check") {
  dependsOn(tasks.named("format"))
}

tasks.named("assemble").configure {
  dependsOn(tasks.named("uvSync"))
}
