/*
 Placeholder project build script to ensure that running `./gradlew :oss:airbyte-api:build` builds all
 of the child API projects.
 */
plugins {
  id("io.airbyte.gradle.jvm.lib")
}

airbyte {
  spotless {
    excludes = listOf(
      project(":oss:airbyte-api:workload-api").file("src/main/openapi/workload-openapi.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api.yaml").path,

      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_sdk.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_terraform.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_documentation_connections.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_documentation_sources.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_documentation_destinations.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_documentation_streams.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_documentation_jobs.yaml").path,
      project(":oss:airbyte-api:server-api").file("src/main/openapi/api_documentation_workspaces.yaml").path,
    )
  }
}

dependencies {
  project.subprojects.forEach { subProject ->
    implementation(project(subProject.path))
  }
}
