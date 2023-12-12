plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

// Add a configuration(for our migrations(tasks defined below to encapsulate their dependencies)
val migrations by configurations.creating {
    extendsFrom(configurations.getByName("implementation"))
}

configurations.all {
    exclude(group = "io.micronaut.flyway")
    resolutionStrategy {
        force (libs.platform.testcontainers.postgresql)
    }
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

    implementation(project(":airbyte-commons"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-config:config-models"))
    implementation(libs.flyway.core)
    implementation(libs.guava)
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    implementation(libs.commons.io)

    migrations(libs.platform.testcontainers.postgresql)
    migrations(sourceSets["main"].output)

    // Mark as compile Only to avoid leaking transitively to connectors)
    compileOnly(libs.platform.testcontainers.postgresql)

    // These are required because gradle might be using lower version of Jna from other)
    // library transitive dependency. Can be removed if we can figure out which library is the cause.)
    // Refer: https://github.com/testcontainers/testcontainers-java/issues/3834#issuecomment-825409079)
    implementation("net.java.dev.jna:jna:5.8.0")
    implementation("net.java.dev.jna:jna-platform:5.8.0")

    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.apache.commons.lang)
    testImplementation(libs.platform.testcontainers.postgresql)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
    testImplementation(libs.json.assert)
}

tasks.register<JavaExec>("newConfigsMigration") {
    mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
    classpath = files(migrations.files)
    args =  listOf("configs", "create")
    dependsOn(tasks.named("classes"))
}

tasks.register<JavaExec>("runConfigsMigration") {
    mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
    classpath = files(migrations.files)
    args = listOf("configs", "migrate")
    dependsOn(tasks.named("classes"))
}

tasks.register<JavaExec>("dumpConfigsSchema") {
    mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
    classpath = files(migrations.files)
    args = listOf("configs", "dump_schema")
    dependsOn(tasks.named("classes"))
}

tasks.register<JavaExec>("newJobsMigration") {
    mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
    classpath = files(migrations.files)
    args = listOf("jobs", "create")
    dependsOn(tasks.named("classes"))
}

tasks.register<JavaExec>("runJobsMigration") {
    mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
    classpath = files(migrations.files)
    args = listOf( "jobs", "migrate")
    dependsOn(tasks.named("classes"))
}

tasks.register<JavaExec>("dumpJobsSchema") {
    mainClass = "io.airbyte.db.instance.development.MigrationDevCenter"
    classpath = files(migrations.files)
    args = listOf("jobs", "dump_schema")
    dependsOn(tasks.named("classes"))
}

val copyInitSql = tasks.register<Copy>("copyInitSql") {
    from("src/main/resources") {
        include("init.sql")
    }
    into("build/airbyte/docker/bin")
}

tasks.named("dockerBuildImage") {
    dependsOn(copyInitSql)
}
