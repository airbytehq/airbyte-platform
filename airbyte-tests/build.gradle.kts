import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
  id("io.airbyte.gradle.jvm.lib")
}
/**
 * Registers a test-suite with Gradle's JvmTestSuite
 * @param name name the name of the test suite, must be unique, will match the name of the created task
 * @param includeTags tags of the tests to be included in this test-suite
 */
@Suppress("UnstableApiUsage")
fun registerTestSuite(name: String, includeTags: Array<String> = emptyArray()) {
  testing {
    suites.register<JvmTestSuite>(name) {
      dependencies {
        implementation(project())
      }

      sources {
        java {
          setSrcDirs(listOf("src/test-acceptance/java", "src/test-acceptance/kotlin"))
        }
        resources {
          setSrcDirs(listOf("src/test-acceptance/resources"))
        }
      }

      targets.all {
        testTask.configure {

          val parallelExecutionEnabled = System.getenv()["TESTS_PARALLEL_EXECUTION_ENABLED"] ?: "true"
          val ciMode = System.getProperty("ciMode") ?: "false"

          systemProperties = mapOf(
            "junit.jupiter.execution.parallel.enabled" to parallelExecutionEnabled,
            // we use this property for our logging configuration. Gradle creates a new JVM to run tests, so we need to explicitly pass this property
            "ciMode" to ciMode)

          useJUnitPlatform {
            includeTags(*includeTags)
          }
          testLogging {
            events = setOf(TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.STARTED, TestLogEvent.SKIPPED)
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

registerTestSuite(name = "syncAcceptanceTest", includeTags = arrayOf("sync"))
registerTestSuite(name = "apiAcceptanceTest", includeTags = arrayOf("api"))
registerTestSuite(name = "builderAcceptanceTest", includeTags = arrayOf("builder"))
registerTestSuite(name = "enterpriseAcceptanceTest", includeTags = arrayOf("enterprise"))
registerTestSuite(name = "acceptanceTest")

configurations.configureEach {
  // Temporary hack to avoid dependency conflicts
  exclude(group = "io.micronaut.email")
}

dependencies {
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-test-utils"))
  implementation(project(":oss:airbyte-commons-worker"))
  implementation(project(":oss:airbyte-container-orchestrator"))
  implementation(project(":oss:airbyte-featureflag"))

  implementation(libs.bundles.kubernetes.client)
  implementation(libs.platform.testcontainers)
  implementation(libs.failsafe)
  implementation(libs.jackson.databind)
  implementation(libs.okhttp)
  implementation(libs.temporal.sdk)
  implementation(libs.platform.testcontainers.postgresql)
  implementation(libs.postgresql)

  runtimeOnly(libs.bouncycastle.bcpkix)
  runtimeOnly(libs.bouncycastle.bcprov)

  testImplementation("com.airbyte:api:0.39.2")

  testImplementation(libs.bundles.junit)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
}

tasks.withType<Copy>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
