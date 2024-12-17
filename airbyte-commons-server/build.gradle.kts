plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(platform(libs.micronaut.platform))
  annotationProcessor(libs.bundles.micronaut.annotation.processor)
  annotationProcessor(libs.micronaut.jaxrs.processor)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)
  ksp(libs.micronaut.jaxrs.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.datadog)
  implementation(libs.micronaut.cache.caffeine)
  implementation(libs.micronaut.inject)
  implementation(libs.micronaut.security)
  implementation(libs.micronaut.security.jwt)
  implementation(libs.bundles.micronaut.data.jdbc)
  implementation(libs.bundles.micronaut.kotlin)
  implementation(libs.bundles.flyway)
  implementation(libs.s3)
  implementation(libs.aws.java.sdk.s3)
  implementation(libs.sts)
  implementation(libs.bundles.apache)
  implementation(libs.quartz.scheduler)
  implementation(libs.temporal.sdk)
  implementation(libs.swagger.annotations)
  implementation(libs.commons.io)
  implementation(libs.apache.commons.lang)
  implementation(libs.kotlin.logging)
  implementation(libs.reactor.core)
  implementation(libs.jakarta.ws.rs.api)
  implementation(libs.kubernetes.client)
  implementation(libs.guava)
  implementation(libs.cron.utils)

  implementation(project(":oss:airbyte-analytics"))
  implementation(project(":oss:airbyte-api:connector-builder-api"))
  implementation(project(":oss:airbyte-api:problems-api"))
  implementation(project(":oss:airbyte-api:server-api"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-converters"))
  implementation(project(":oss:airbyte-commons-license"))
  implementation(project(":oss:airbyte-commons-protocol"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-commons-temporal"))
  implementation(project(":oss:airbyte-commons-temporal-core"))
  implementation(project(":oss:airbyte-commons-with-dependencies"))
  implementation(project(":oss:airbyte-config:init"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-config:config-secrets"))
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-connector-rollout-client"))
  implementation(project(":oss:airbyte-connector-rollout-shared"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-mappers"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-oauth"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(project(":oss:airbyte-worker-models"))
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-csp-check"))

  testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

  kspTest(platform(libs.micronaut.platform))
  kspTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(project(":oss:airbyte-test-utils"))
  testImplementation("org.jetbrains.kotlin:kotlin-reflect")
  testImplementation(libs.postgresql)
  testImplementation(libs.platform.testcontainers.postgresql)
  testImplementation(libs.mockwebserver)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.micronaut.http)
  testImplementation(libs.mockk)
  testImplementation(libs.reactor.test)
  testImplementation(libs.bundles.kotest)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into spotbug issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
  enabled = false
}

tasks.withType<Jar> {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
