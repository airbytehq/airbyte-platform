/*
 Placeholder project build script to ensure that running `./gradlew :oss:airbyte-api:build` builds all
 of the child API projects.
 */
plugins {
  id("io.airbyte.gradle.jvm.lib")
}

dependencies {
  project.subprojects.forEach { implementation(project(it.path)) }
}

airbyte {
  spotless {
    excludes =
      listOf(
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

// Delete stale Eclipse/LSP-generated bin/ directories before Spotless runs.
// These directories contain generated Kotlin files with wildcard imports that ktlint rejects.
// The Spotless plugin's targetExclude mechanism (via airbyte.spotless.excludes) only supports
// absolute file paths, not glob patterns, so we clean up the directories instead.
val cleanBinDirs = tasks.register<Delete>("cleanBinDirs") {
  delete(
    subprojects.map { file("${it.projectDir}/bin") } + file("$projectDir/bin"),
  )
}

tasks.matching { it.name.startsWith("spotless") }.configureEach {
  dependsOn(cleanBinDirs)
}

subprojects {
  val cleanSubprojectBinDir = tasks.register<Delete>("cleanBinDir") {
    delete(file("$projectDir/bin"))
  }
  tasks.matching { it.name.startsWith("spotless") }.configureEach {
    dependsOn(cleanSubprojectBinDir)
  }
}
