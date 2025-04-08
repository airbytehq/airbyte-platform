import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaPythonBaseImage") {
  inputDir = project.projectDir
  tag = "2.2.4"
  buildArgs.put("AIRBYTE_BASE_JAVA_IMAGE_TAG", "3.3.4")
  imageName = "airbyte/airbyte-base-java-python-image"
}