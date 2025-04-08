import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaBaseImage") {
  inputDir = project.projectDir
  tag = "3.3.4"
  imageName = "airbyte/airbyte-base-java-image"
}