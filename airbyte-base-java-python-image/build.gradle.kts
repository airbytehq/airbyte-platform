import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaPythonBaseImage") {
  inputDir = project.projectDir
  dockerfile = layout.projectDirectory.file("./Dockerfile")
  tag = layout.projectDirectory.file(".version").asFile.readText().trim()
  additionalTags = listOf(
    tag.substringBeforeLast("."),  // Minor version mutable tag
    tag.substringBeforeLast(".").substringBeforeLast(".") // Major version mutable tag
  )
  buildArgs.put("AIRBYTE_BASE_JAVA_IMAGE_TAG",
    layout.projectDirectory.file("../airbyte-base-java-image/.version").asFile.readText().trim()
  )
  imageName = "airbyte-base-java-python-image"
}
