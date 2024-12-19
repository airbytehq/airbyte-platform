// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// NOTE: this settings is only discovered when running from oss/build.gradle
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
pluginManagement {
  repositories {
    // uncomment for local dev
    // maven {
    // name = "localPluginRepo"
    // url = uri("../.gradle-plugins-local")
    // }
    maven(url = "https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars")
    gradlePluginPortal()
    maven(url = "https://oss.sonatype.org/content/repositories/snapshots")
  }
}

// Configure the gradle enterprise plugin to enable build scans. Enabling the plugin at the top of the settings file allows the build scan to record
// as much information as possible.
plugins {
  id("com.gradle.enterprise") version "3.15.1"
  id("com.github.burrunan.s3-build-cache") version "1.8.1"
}

gradleEnterprise {
  buildScan {
    termsOfServiceUrl = "https://gradle.com/terms-of-service"
    termsOfServiceAgree = "yes"
  }
}

val isCiServer = System.getenv().containsKey("CI")

gradleEnterprise {
  buildScan {
    isUploadInBackground = !isCiServer // Disable async upload so that the containers doesn't terminate the upload
    buildScanPublished {
      file("scan-journal.log").writeText("${java.util.Date()} - $buildScanId - ${buildScanUri}\n")
    }
  }
}

buildCache {
  remote<com.github.burrunan.s3cache.AwsS3BuildCache> {
    region = "us-west-2"
    bucket = "ab-ci-cache"
    prefix = "platform-ci-cache/"
    isPush = isCiServer
    isEnabled = System.getenv().containsKey("S3_BUILD_CACHE_ACCESS_KEY_ID")
  }
}

rootProject.name = "airbyte-oss"

// definition for dependency resolution
dependencyResolutionManagement {
  repositories {
    maven(url = "https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars/")
  }

  versionCatalogs {
    create("libs") {
      from(files("deps.toml"))
    }
  }
}

// todo (cgardens) - alphabetize
// Note: Every submodule needs to appear in this file TWICE. First to be included
// e.g. include("<submodule>") and then a second time to declare its project directory
// (e.g. project("<submodule>").projectDir = file("<path from oss/ to the submodule>")
// Because the directory structure in the oss repo doesn't match the actual file
// structure the project dir has to be declared explicitly. In the oss repo oss is
// the root.
include(":oss:airbyte-commons")

include(":oss:airbyte-api")
include(":oss:airbyte-api:commons")
include(":oss:airbyte-api:server-api")
include(":oss:airbyte-api:connector-builder-api")
include(":oss:airbyte-api:problems-api")
include(":oss:airbyte-api:public-api")
include(":oss:airbyte-api:workload-api")
include(":oss:airbyte-audit-logging")
include(":oss:airbyte-workload-api-server")
include(":oss:airbyte-commons-protocol")
include(":oss:airbyte-config:specs")
include(":oss:airbyte-config:init")
include(":oss:airbyte-config:config-models")
include(":oss:airbyte-data")
include(":oss:airbyte-db:db-lib")
include(":oss:airbyte-json-validation")
include(":oss:airbyte-metrics:metrics-lib")
include(":oss:airbyte-oauth")
include(":oss:airbyte-test-utils")

include(":oss:airbyte-analytics")
include(":oss:airbyte-commons-temporal")
include(":oss:airbyte-commons-temporal-core")
include(":oss:airbyte-commons-converters")
include(":oss:airbyte-commons-worker")
include(":oss:airbyte-config:config-persistence")
include(":oss:airbyte-config:config-secrets")
include(":oss:airbyte-featureflag")
include(":oss:airbyte-featureflag-server")
include(":oss:airbyte-db:jooq")
include(":oss:airbyte-micronaut-temporal")
include(":oss:airbyte-notification")
include(":oss:airbyte-persistence:job-persistence")
include(":oss:airbyte-worker-models")

include(":oss:airbyte-bootloader")
include(":oss:airbyte-commons-auth")
include(":oss:airbyte-commons-license")
include(":oss:airbyte-commons-storage")
include(":oss:airbyte-commons-micronaut")
include(":oss:airbyte-commons-micronaut-security")
include(":oss:airbyte-commons-server")
include(":oss:airbyte-commons-with-dependencies")
include(":oss:airbyte-connector-builder-server")
include(":oss:airbyte-connector-rollout-shared")
include(":oss:airbyte-connector-rollout-worker")
include(":oss:airbyte-connector-rollout-client")
include(":oss:airbyte-container-orchestrator")
include(":oss:airbyte-cron")
include(":oss:airbyte-csp-check")
include(":oss:airbyte-keycloak")
include(":oss:airbyte-keycloak-setup")
include(":oss:airbyte-mappers")
include(":oss:airbyte-metrics:reporter")
include(":oss:airbyte-pod-sweeper")
include(":oss:airbyte-server")
include(":oss:airbyte-temporal")
include(":oss:airbyte-tests")
include(":oss:airbyte-webapp")
include(":oss:airbyte-workers")
include(":oss:airbyte-workload-launcher")
include(":oss:airbyte-connector-sidecar")
include(":oss:airbyte-workload-init-container")
include(":oss:airbyte-pmd-rules")

project(":oss:airbyte-commons").projectDir = file("airbyte-commons")
project(":oss:airbyte-api").projectDir = file("airbyte-api")
project(":oss:airbyte-api:commons").projectDir = file("airbyte-api/commons")
project(":oss:airbyte-api:server-api").projectDir = file("airbyte-api/server-api")
project(":oss:airbyte-api:connector-builder-api").projectDir = file("airbyte-api/connector-builder-api")
project(":oss:airbyte-api:problems-api").projectDir = file("airbyte-api/problems-api")
project(":oss:airbyte-api:public-api").projectDir = file("airbyte-api/public-api")
project(":oss:airbyte-api:workload-api").projectDir = file("airbyte-api/workload-api")
project(":oss:airbyte-audit-logging").projectDir = file("airbyte-audit-logging")
project(":oss:airbyte-workload-api-server").projectDir = file("airbyte-workload-api-server")
project(":oss:airbyte-commons-protocol").projectDir = file("airbyte-commons-protocol")
project(":oss:airbyte-config:specs").projectDir = file("airbyte-config/specs")
project(":oss:airbyte-config:init").projectDir = file("airbyte-config/init")
project(":oss:airbyte-config:config-models").projectDir = file("airbyte-config/config-models")
project(":oss:airbyte-data").projectDir = file("airbyte-data")
project(":oss:airbyte-db:db-lib").projectDir = file("airbyte-db/db-lib")
project(":oss:airbyte-json-validation").projectDir = file("airbyte-json-validation")
project(":oss:airbyte-metrics:metrics-lib").projectDir = file("airbyte-metrics/metrics-lib")
project(":oss:airbyte-oauth").projectDir = file("airbyte-oauth")
project(":oss:airbyte-test-utils").projectDir = file("airbyte-test-utils")
project(":oss:airbyte-analytics").projectDir = file("airbyte-analytics")
project(":oss:airbyte-commons-temporal").projectDir = file("airbyte-commons-temporal")
project(":oss:airbyte-commons-temporal-core").projectDir = file("airbyte-commons-temporal-core")
project(":oss:airbyte-commons-converters").projectDir = file("airbyte-commons-converters")
project(":oss:airbyte-commons-worker").projectDir = file("airbyte-commons-worker")
project(":oss:airbyte-config:config-persistence").projectDir = file("airbyte-config/config-persistence")
project(":oss:airbyte-config:config-secrets").projectDir = file("airbyte-config/config-secrets")
project(":oss:airbyte-csp-check").projectDir = file("airbyte-csp-check")
project(":oss:airbyte-featureflag").projectDir = file("airbyte-featureflag")
project(":oss:airbyte-featureflag-server").projectDir = file("airbyte-featureflag-server")
project(":oss:airbyte-db:jooq").projectDir = file("airbyte-db/jooq")
project(":oss:airbyte-micronaut-temporal").projectDir = file("airbyte-micronaut-temporal")
project(":oss:airbyte-notification").projectDir = file("airbyte-notification")
project(":oss:airbyte-persistence:job-persistence").projectDir = file("airbyte-persistence/job-persistence")
project(":oss:airbyte-worker-models").projectDir = file("airbyte-worker-models")
project(":oss:airbyte-bootloader").projectDir = file("airbyte-bootloader")
project(":oss:airbyte-commons-auth").projectDir = file("airbyte-commons-auth")
project(":oss:airbyte-commons-license").projectDir = file("airbyte-commons-license")
project(":oss:airbyte-commons-storage").projectDir = file("airbyte-commons-storage")
project(":oss:airbyte-commons-micronaut").projectDir = file("airbyte-commons-micronaut")
project(":oss:airbyte-commons-micronaut-security").projectDir = file("airbyte-commons-micronaut-security")
project(":oss:airbyte-commons-server").projectDir = file("airbyte-commons-server")
project(":oss:airbyte-commons-with-dependencies").projectDir = file("airbyte-commons-with-dependencies")
project(":oss:airbyte-connector-builder-server").projectDir = file("airbyte-connector-builder-server")
project(":oss:airbyte-connector-rollout-shared").projectDir = file("airbyte-connector-rollout-shared")
project(":oss:airbyte-connector-rollout-worker").projectDir = file("airbyte-connector-rollout-worker")
project(":oss:airbyte-connector-rollout-client").projectDir = file("airbyte-connector-rollout-client")
project(":oss:airbyte-container-orchestrator").projectDir = file("airbyte-container-orchestrator")
project(":oss:airbyte-cron").projectDir = file("airbyte-cron")
project(":oss:airbyte-keycloak").projectDir = file("airbyte-keycloak")
project(":oss:airbyte-keycloak-setup").projectDir = file("airbyte-keycloak-setup")
project(":oss:airbyte-mappers").projectDir = file("airbyte-mappers")
project(":oss:airbyte-metrics:reporter").projectDir = file("airbyte-metrics/reporter")
project(":oss:airbyte-pod-sweeper").projectDir = file("airbyte-pod-sweeper")
project(":oss:airbyte-server").projectDir = file("airbyte-server")
project(":oss:airbyte-temporal").projectDir = file("airbyte-temporal")
project(":oss:airbyte-tests").projectDir = file("airbyte-tests")
project(":oss:airbyte-webapp").projectDir = file("airbyte-webapp")
project(":oss:airbyte-workers").projectDir = file("airbyte-workers")
project(":oss:airbyte-workload-launcher").projectDir = file("airbyte-workload-launcher")
project(":oss:airbyte-connector-sidecar").projectDir = file("airbyte-connector-sidecar")
project(":oss:airbyte-workload-init-container").projectDir = file("airbyte-workload-init-container")
project(":oss:airbyte-pmd-rules").projectDir = file("airbyte-pmd-rules")
