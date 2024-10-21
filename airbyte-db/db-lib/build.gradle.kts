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
  implementation(libs.commons.io)

  migrations(libs.platform.testcontainers.postgresql)
  migrations(sourceSets["main"].output)

  // Mark as compile Only to avoid leaking transitively to connectors
  compileOnly(libs.platform.testcontainers.postgresql)

  // These are required because gradle might be using lower version of Jna from other
  // library transitive dependency. Can be removed if we can figure out which library is the cause.
  // Refer: https://github.com/testcontainers/testcontainers-java/issues/3834#issuecomment-825409079
  implementation(libs.jna)
  implementation(libs.jna.platform)

  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation(libs.apache.commons.lang)
  testImplementation(libs.platform.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)

  testImplementation(libs.junit.pioneer)
  testImplementation(libs.json.assert)
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

tasks.register<JavaExec>("newConfigsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations.files)
  args = listOf("configs", "create")
  dependsOn(":oss:airbyte-db:db-lib:build")
}

tasks.register<JavaExec>("runConfigsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations.files)
  args = listOf("configs", "migrate")
  dependsOn(":oss:airbyte-db:db-lib:build")
}

tasks.register<JavaExec>("dumpConfigsSchema") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations.files)
  args = listOf("configs", "dump_schema")
  dependsOn(":oss:airbyte-db:db-lib:build")
}

tasks.register<JavaExec>("newJobsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations.files)
  args = listOf("jobs", "create")
  dependsOn(":oss:airbyte-db:db-lib:build")
}

tasks.register<JavaExec>("runJobsMigration") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations.files)
  args = listOf("jobs", "migrate")
  dependsOn(":oss:airbyte-db:db-lib:build")
}

tasks.register<JavaExec>("dumpJobsSchema") {
  mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
  classpath = files(migrations.files)
  args = listOf("jobs", "dump_schema")
  dependsOn(":oss:airbyte-db:db-lib:build")
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
