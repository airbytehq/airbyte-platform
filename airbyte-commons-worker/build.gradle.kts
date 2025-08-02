plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.google.cloud.pubsub)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.micronaut.metrics)
  implementation(libs.micronaut.http)
  implementation(libs.kotlin.logging)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.micronaut.jackson.databind)
  implementation(libs.bundles.kubernetes.client)
  implementation(libs.java.jwt)
  implementation(libs.gson)
  implementation(libs.guava)
  implementation(libs.temporal.sdk) {
    exclude(module = "guava")
  }
  implementation(libs.bundles.datadog)
  implementation(libs.failsafe.okhttp)
  implementation(libs.google.cloud.storage)
  implementation(libs.okhttp)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation(libs.s3)
  implementation(libs.sts)
  implementation(libs.kotlin.coroutines)
  implementation(libs.kotlin.coroutines.sl4j)

  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-api:workload-api"))
  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-commons-with-dependencies"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-domain:models"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-mappers"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-worker-models"))
  implementation(libs.jakarta.validation.api)

  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.json.path)
  testImplementation(libs.json.smart)
  testImplementation(libs.bundles.mockito.inline)
  testImplementation(libs.mockito.kotlin)
  testImplementation(libs.mockk)
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.docker.java)
  testImplementation(libs.docker.java.transport.httpclient5)
  testImplementation(libs.reactor.test)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.logback)
  testImplementation(libs.kotlin.coroutines.test)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.javax.databind)
}

tasks.named<Test>("test") {
  useJUnitPlatform {
    excludeTags("cloud-storage")
  }
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
