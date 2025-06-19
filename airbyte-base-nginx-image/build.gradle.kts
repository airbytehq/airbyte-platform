import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerNginxBaseImage") {
  inputDir = layout.projectDirectory.dir("src")
  dockerfile = layout.projectDirectory.file("src/Dockerfile")
  tag = "1.27.3-3"
  imageName = "airbyte-base-nginx-image"
  buildArgs.put("UID", "1000")
  buildArgs.put("GID", "1000")
}
