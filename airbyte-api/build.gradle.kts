/*
 Placeholder project build script to ensure that running `./gradlew :oss:airbyte-api:build` builds all
 of the child API projects.
 */
plugins {
  id("io.airbyte.gradle.jvm.lib")
}

airbyte {
  spotless {
    excludes = listOf(project(":oss:airbyte-api:workload-api").file("src/main/openapi/workload-openapi.yaml").path)
  }
}

dependencies {
  project.subprojects.forEach { subProject ->
    implementation(project(subProject.path))
  }
}