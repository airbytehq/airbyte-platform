import nu.studer.gradle.jooq.JooqGenerate

plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    alias(libs.plugins.nu.studer.jooq)
}

configurations.all {
    resolutionStrategy {
        force(libs.platform.testcontainers.postgresql)
    }
}
dependencies {
    implementation(libs.jooq.meta)
    implementation(libs.jooq)
    implementation(libs.postgresql)
    implementation(libs.flyway.core)
    implementation(project(":airbyte-db:db-lib"))

    // jOOQ code generation)
    implementation(libs.jooq.codegen)
    implementation(libs.platform.testcontainers.postgresql)

    // These are required because gradle might be using lower version of Jna from other
    // library transitive dependency. Can be removed if we can figure out which library is the cause.
    // Refer: https://github.com/testcontainers/testcontainers-java/issues/3834#issuecomment-825409079
    implementation("net.java.dev.jna:jna:5.8.0")
    implementation("net.java.dev.jna:jna-platform:5.8.0")

    // The jOOQ code generator(only has access to classes added to the jooqGenerator configuration
    jooqGenerator(project(":airbyte-db:db-lib"))
    jooqGenerator(libs.platform.testcontainers.postgresql)
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
    srcDirs("$buildDir/generated/configsDatabase/src/main/java", "$buildDir/generated/jobsDatabase/src/main/java")
}

tasks.named<JooqGenerate>("generateConfigsDatabaseJooq") {
    allInputsDeclared = true
    outputs.cacheIf { true }
}

tasks.named<JooqGenerate>("generateJobsDatabaseJooq") {
    allInputsDeclared = true
    outputs.cacheIf { true }
}
