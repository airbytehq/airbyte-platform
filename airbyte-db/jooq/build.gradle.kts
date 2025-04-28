import nu.studer.gradle.jooq.JooqGenerate

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
  alias(libs.plugins.nu.studer.jooq)
}

dependencies {
  implementation(libs.jooq.meta)
  implementation(libs.jooq)
  implementation(libs.postgresql)
  implementation(libs.bundles.flyway)
  implementation(project(":oss:airbyte-db:db-lib"))

  // jOOQ code generation)
  jooqGenerator(libs.platform.testcontainers.postgresql)

  // These are required because gradle might be using lower version of Jna from other
  // library transitive dependency. Can be removed if we can figure out which library is the cause.
  // Refer: https://github.com/testcontainers/testcontainers-java/issues/3834#issuecomment-825409079
  jooqGenerator(libs.jna)
  jooqGenerator(libs.jna.platform)

  // The jOOQ code generator(only has access to classes added to the jooqGenerator configuration
  jooqGenerator(project(":oss:airbyte-db:db-lib")) {
    isTransitive = false
  }
  jooqGenerator(project(":oss:airbyte-commons")) {
    isTransitive = false
  }
  jooqGenerator(project(":oss:airbyte-config:config-models")) {
    isTransitive = false
  }
  jooqGenerator(libs.bundles.flyway)
  jooqGenerator(libs.guava)
  jooqGenerator(libs.hikaricp)
  jooqGenerator(libs.jackson.datatype)
  jooqGenerator(libs.jackson.jdk.datatype)
  jooqGenerator(libs.postgresql)
  jooqGenerator(libs.slf4j.simple)
  jooqGenerator(libs.platform.testcontainers.postgresql)
  jooqGenerator(libs.jackson.kotlin)
  jooqGenerator(libs.kotlin.logging)
  // the jooqGenerator picks up the wrong version of kotlin by default, not sure why
  // TODO: figure out why _and_ fix!
  jooqGenerator("org.jetbrains.kotlin:kotlin-stdlib-jdk8:2.1.20")
}

jooq {
  version = libs.versions.jooq
  edition = nu.studer.gradle.jooq.JooqEdition.OSS

  configurations {
    create("configsDatabase") {
      generateSchemaSourceOnCompilation = true
      jooqConfiguration.apply {
        generator.apply {
          name = "org.jooq.codegen.DefaultGenerator"
          database.apply {
            name = "io.airbyte.db.instance.configs.ConfigsFlywayMigrationDatabase"
            inputSchema = "public"
            excludes = "airbyte_configs_migrations"
          }
          target.apply {
            packageName = "io.airbyte.db.instance.configs.jooq.generated"
            directory = "build/generated/configsDatabase/src/main/java"
          }
        }
      }
    }

    create("jobsDatabase") {
      generateSchemaSourceOnCompilation = true
      jooqConfiguration.apply {
        generator.apply {
          name = "org.jooq.codegen.DefaultGenerator"
          database.apply {
            name = "io.airbyte.db.instance.jobs.JobsFlywayMigrationDatabase"
            inputSchema = "public"
            excludes = "airbyte_jobs_migrations"
          }
          target.apply {
            packageName = "io.airbyte.db.instance.jobs.jooq.generated"
            directory = "build/generated/jobsDatabase/src/main/java"
          }
        }
      }
    }
  }
}

sourceSets["main"].java {
  srcDirs(
    tasks.named<JooqGenerate>("generateConfigsDatabaseJooq").flatMap { it.outputDir },
    tasks.named<JooqGenerate>("generateJobsDatabaseJooq").flatMap { it.outputDir },
  )
}

sourceSets["main"].java {
  srcDirs(
    "${project.layout.buildDirectory.get()}/generated/configsDatabase/src/main/java",
    "${project.layout.buildDirectory.get()}/generated/jobsDatabase/src/main/java",
  )
}

tasks.named<JooqGenerate>("generateConfigsDatabaseJooq") {
  allInputsDeclared = true
  outputs.cacheIf { true }
}

tasks.named<JooqGenerate>("generateJobsDatabaseJooq") {
  allInputsDeclared = true
  outputs.cacheIf { true }
}
