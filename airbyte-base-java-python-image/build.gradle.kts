import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaPythonBaseImage") {
  inputDir = project.projectDir
  tag = "2.2.5"
  buildArgs.put("AIRBYTE_BASE_JAVA_IMAGE_TAG", "3.3.5")
  imageName = "airbyte-base-java-python-image"
}
