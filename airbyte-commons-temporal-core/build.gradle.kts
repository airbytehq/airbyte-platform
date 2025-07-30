plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  api(libs.bundles.micronaut.annotation)
  
  implementation(libs.bundles.temporal)
  implementation(libs.failsafe)
  implementation(libs.kotlin.logging)

  // We do not want dependency on(databases from this library.)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))

  testImplementation(libs.assertj.core)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.kotlin)
  testImplementation(libs.temporal.testing)
  // this is temporarily needed because temporal.testing uses InProcessBuilder which is included in on grpc-core 1.52.2
  // featureflag depends on  micronaut-platform 4.4.1 depends on grpc-core 1.62.2, which doesn't contain the InProcessBuilder class.
  testImplementation(libs.grpc.inprocess)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
