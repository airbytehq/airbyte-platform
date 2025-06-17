import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaPythonBaseImage") {
  inputDir = project.projectDir
  dockerfile = layout.projectDirectory.file("./Dockerfile")
  tag = "2.2.6"
  buildArgs.put("AIRBYTE_BASE_JAVA_IMAGE_TAG", "3.3.6")
  imageName = "airbyte-base-java-python-image"
}
