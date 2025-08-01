import io.airbyte.gradle.tasks.DockerBuildxTask

plugins {
  id("io.airbyte.gradle.docker") apply false
}

tasks.register<DockerBuildxTask>("dockerJavaBaseImage") {
  inputDir = project.projectDir
  dockerfile = layout.projectDirectory.file("./Dockerfile")
  tag = layout.projectDirectory.file(".version").asFile.readText().trim()
  additionalTags = listOf(
    tag.substringBeforeLast("."),  // Minor version mutable tag
    tag.substringBeforeLast(".").substringBeforeLast(".") // Major version mutable tag
  )
  imageName = "airbyte-base-java-image"
}
