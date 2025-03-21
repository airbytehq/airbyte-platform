import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaPythonBaseImage") {
  inputDir = project.projectDir
  tag = "2.2.3"
  buildArgs.put("AIRBYTE_BASE_JAVA_IMAGE_TAG", "3.3.3")
  imageName = "airbyte/airbyte-base-java-python-image"
}