import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    id("io.airbyte.gradle.jvm.lib")
}

@Suppress("UnstableApiUsage")
testing {
    registerTestSuite(name="acceptanceTest", type="acceptance-test", dirName="test-acceptance") {
        implementation.add(project())

        implementation(project(":airbyte-api"))
        implementation(project(":airbyte-commons"))
        implementation(project(":airbyte-commons-temporal"))
        implementation(project(":airbyte-config:config-models"))
        implementation(project(":airbyte-config:config-persistence"))
        implementation(project(":airbyte-db:db-lib"))
        implementation(project(":airbyte-tests"))
        implementation(project(":airbyte-test-utils"))
        implementation(project(":airbyte-commons-worker"))

        implementation("com.fasterxml.jackson.core:jackson-databind")
        implementation("io.github.cdimascio:java-dotenv:3.0.0")
        implementation(libs.temporal.sdk)
        implementation("org.apache.commons:commons-csv:1.4")
        implementation(libs.platform.testcontainers.postgresql)
        implementation(libs.postgresql)
        implementation("org.bouncycastle:bcprov-jdk15on:1.66")
        implementation("org.bouncycastle:bcpkix-jdk15on:1.66")
    }
}

/**
 * Registers a test-suite with Gradle's JvmTestSuite
 * @param name name the name of the test suite, must be unique, will match the name of the created task
 * @param type name the name of this test suite, passed directly to the testType property
 * @param dirName directory name which corresponds to this test-suite, assumes that this directory is located in `src`
 * @param deps lambda for registering dependencies specific to this test-suite with this test-suite
 */
@Suppress("UnstableApiUsage")
fun registerTestSuite(name: String, type: String, dirName: String, deps: JvmComponentDependencies.() -> Unit) {
    testing {
        suites.register<JvmTestSuite>(name) {
            testType.set(type)

            deps(dependencies)

            sources {
                java {
                    setSrcDirs(listOf("src/$dirName/java"))
                }
                resources {
                    setSrcDirs(listOf("src/$dirName/resources"))
                }
            }

            targets.all {
                testTask.configure {
                    testLogging {
                        events = setOf(TestLogEvent.PASSED, TestLogEvent.FAILED)
                    }
                    shouldRunAfter(suites.named("test"))
                }
            }
        }

        configurations.named("${name}Implementation") {
            extendsFrom(configurations.getByName("testImplementation"))
        }
    }
}

configurations.configureEach {
    // Temporary hack to avoid dependency conflicts
    exclude(group="io.micronaut.email")
}

dependencies {
    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-container-orchestrator"))

    implementation(libs.bundles.kubernetes.client)
    implementation(libs.platform.testcontainers)

    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
}
// Cole - Leaving these commented out, if nothing breaks I'll delete them
// test should run using the current version of the docker compose configuration.
//val taskAcceptance = tasks.register<Copy>("copyComposeFileForAcceptanceTests") {
//    from("${rootDir}/docker-compose.yaml")
//    into("${sourceSets.acceptanceTests.output.resourcesDir}")
//}
//tasks.named("checkstyleAcceptanceTests") {
//    dependsOn(taskAcceptance)
//}
//tasks.named("pmdAcceptanceTests") {
//    dependsOn(taskAcceptance)
//}
//

tasks.withType<Copy>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
