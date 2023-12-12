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



        implementation(libs.failsafe)
        implementation(libs.jackson.databind)
        implementation(libs.okhttp)
        implementation(libs.temporal.sdk)
        implementation(libs.platform.testcontainers.postgresql)
        implementation(libs.postgresql)

        // needed for fabric to connect to k8s.
        runtimeOnly(libs.bouncycastle.bcpkix)
        runtimeOnly(libs.bouncycastle.bcprov)
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
                    // Ensure they re-run since these are integration tests.
                    outputs.upToDateWhen { false }
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

    testImplementation("com.airbyte:api:0.39.2")

    implementation(libs.bundles.kubernetes.client)
    implementation(libs.platform.testcontainers)

    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
}

tasks.withType<Copy>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
