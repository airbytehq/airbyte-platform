plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
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
  implementation(libs.apache.ant)
  implementation(libs.apache.commons.text)
  implementation(libs.bundles.datadog)
  implementation(libs.commons.io)
  implementation(libs.bundles.apache)
  implementation(libs.bundles.log4j)
  implementation(libs.failsafe.okhttp)
  implementation(libs.google.cloud.storage)
  implementation(libs.okhttp)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.aws.java.sdk.sts)
  implementation(libs.s3)
  implementation(libs.sts)

  implementation(project(":airbyte-api"))
  implementation(project(":airbyte-analytics"))
  implementation(project(":airbyte-commons"))
  implementation(project(":airbyte-commons-auth"))
  implementation(project(":airbyte-commons-converters"))
  implementation(project(":airbyte-commons-protocol"))
  implementation(project(":airbyte-commons-temporal"))
  implementation(project(":airbyte-commons-temporal-core"))
  implementation(project(":airbyte-commons-with-dependencies"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-config:config-persistence"))
  implementation(project(":airbyte-config:config-secrets"))
  implementation(project(":airbyte-featureflag"))
  implementation(project(":airbyte-json-validation"))
  implementation(project(":airbyte-metrics:metrics-lib"))
  implementation(project(":airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":airbyte-worker-models"))
  implementation(libs.jakarta.validation.api)

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)    // Lombok must be added BEFORE Micronaut
  testAnnotationProcessor(platform(libs.micronaut.platform))
  testAnnotationProcessor(libs.bundles.micronaut.annotation.processor)
  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
  testAnnotationProcessor(libs.jmh.annotations)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.annotation.processor)
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.json.path)
  testImplementation(libs.bundles.mockito.inline)
  testImplementation(libs.mockk)
  testImplementation(variantOf(libs.opentracing.util) { classifier("tests") })
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.jmh.core)
  testImplementation(libs.jmh.annotations)
  testImplementation(libs.docker.java)
  testImplementation(libs.docker.java.transport.httpclient5)
  testImplementation(libs.reactor.test)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)

  testRuntimeOnly(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.javax.databind)
}

tasks.named<Test>("test") {
  useJUnitPlatform {
    excludeTags("cloud-storage")
  }
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.)
// Once lombok has been removed, this can also be removed.)
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
