plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

// Add a configuration(for our migrations(tasks defined below to encapsulate their dependencies)
val migrations: Configuration by configurations.creating {
  extendsFrom(configurations.getByName("implementation"))
}

configurations.all {
  exclude(group = "io.micronaut.flyway")
}

airbyte {
  docker {
    imageName = "db"
  }
}

dependencies {
  api(libs.hikaricp)
  api(libs.jooq.meta)
  api(libs.jooq)
  api(libs.postgresql)

  implementation(project(":oss:airbyte-commons"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(libs.bundles.flyway)
  implementation(libs.guava)
  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)
  implementation(libs.kotlin.logging)

  migrations(libs.platform.testcontainers.postgresql)
  migrations(sourceSets["main"].runtimeClasspath)

  // Mark as compile Only to avoid leaking transitively to connectors
  compileOnly(libs.platform.testcontainers.postgresql)

  // These are required because gradle might be using lower version of Jna from other
  // library transitive dependency. Can be removed if we can figure out which library is the cause.
  // Refer: https://github.com/testcontainers/testcontainers-java/issues/3834#issuecomment-825409079
  implementation(libs.jna)
  implementation(libs.jna.platform)

  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.platform.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
  testImplementation(libs.json.assert)
  testImplementation(libs.mockk)
  testImplementation(kotlin("test"))
}

tasks.named<Test>("test") {
  jvmArgs(
    listOf(
      // Required to use junit-pioneer @SetEnvironmentVariable
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
    ),
  )
}

val envVars = mapOf("VERSION" to "dev")

tasks.register<JavaExec>("newConfigsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  args = listOf("configs", "create", System.getenv("DESCRIPTION") ?: "TodoDescription")
  classpath = files(migrations)
  environment = envVars
  dependsOn(tasks.classes)
}

tasks.register<JavaExec>("runConfigsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations)
  args = listOf("configs", "migrate")
  environment = envVars
  dependsOn(tasks.classes)
}

tasks.register<JavaExec>("dumpConfigsSchema") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations)
  args = listOf("configs", "dump_schema")
  environment = envVars
  dependsOn(tasks.classes)
}

tasks.register<JavaExec>("newJobsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  args = listOf("jobs", "create", System.getenv("DESCRIPTION") ?: "TodoDescription")
  classpath = files(migrations)
  environment = envVars
  dependsOn(tasks.classes)
}

tasks.register<JavaExec>("runJobsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations)
  args = listOf("jobs", "migrate")
  environment = envVars
  dependsOn(tasks.classes)
}

tasks.register<JavaExec>("dumpJobsSchema") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations)
  args = listOf("jobs", "dump_schema")
  environment = envVars
  dependsOn(tasks.classes)
}

val copyInitSql =
  tasks.register<Copy>("copyInitSql") {
    from("src/main/resources") {
      include("init.sql")
      include("airbyte-entrypoint.sh")
    }
    into("build/airbyte/docker/bin")
  }

tasks.named("dockerCopyDistribution") {
  dependsOn(copyInitSql)
}
