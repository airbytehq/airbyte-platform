// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// NOTE: this settings is only discovered when running from oss/build.gradle
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

pluginManagement {
  repositories {
    maven {
      name = "localPluginRepo"
      url = uri("${System.getProperty("user.home")}/.airbyte/gradle")
    }
    maven(url = "https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars")
    gradlePluginPortal()
    maven(url = "https://oss.sonatype.org/content/repositories/snapshots")
  }
}

buildscript {
  dependencies {
    classpath("com.bmuschko:gradle-docker-plugin:8.0.0")
    // Plugin `com.bmuschko:gradle-docker-plugin:8.0.0` transitively depends on jackson 2.10.3,
    // which is not binary compatible with jackson 2.14 that is used elsewhere.
    // This causes `NoSuchMethodError` exceptions while building.
    //
    // Dependency chain:
    // `com.bmuschko:gradle-docker-plugin:8.0.0` ->
    // `com.github.docker-java:docker-java-core:3.2.14` ->
    // `com.fasterxml.jackson.core:jackson-databind:2.10.3`
    //
    // As this is a third-party dependency, we cannot (without forking) update it to use a newer jackson version.
    // There is however a PR that was created to update this version to the latest jackson version:
    // https://github.com/docker-java/docker-java/pull/2056
    //
    // TODO: once oss has been inlined, revisit where the version of jackson is defined.
    classpath("com.fasterxml.jackson.core:jackson-core:2.14.2")

    classpath("org.codehaus.groovy:groovy-yaml:3.0.3")
  }
}

// Configure the gradle enterprise plugin to enable build scans. Enabling the plugin at the top of the settings file allows the build scan to record
// as much information as possible.
plugins {
  id("com.gradle.develocity") version "4.0.1"
  id("com.gradle.common-custom-user-data-gradle-plugin") version "2.1"
  id("com.github.burrunan.s3-build-cache") version "1.8.1"
}

val isCiServer = System.getenv().containsKey("CI")

develocity {
  buildScan {
    termsOfUseUrl = "https://gradle.com/help/legal-terms-of-use"
    termsOfUseAgree = "yes"
    uploadInBackground = !isCiServer // Disable async upload so that the containers doesn't terminate the upload
    buildScanPublished {
      file("scan-journal.log").writeText("${java.util.Date()} - $buildScanId - ${buildScanUri}\n")
    }
  }
}

buildCache {
  local {
    isEnabled = !isCiServer
  }
  remote<com.github.burrunan.s3cache.AwsS3BuildCache> {
    region = "us-east-1"
    bucket = "gradle-build-cache20250418223459104400000001"
    prefix = "platform-cache/"
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
include(":oss:airbyte-domain:models")
include(":oss:airbyte-domain:services")
include(":oss:airbyte-json-validation")
include(":oss:airbyte-metrics:metrics-lib")
include(":oss:airbyte-oauth")
include(":oss:airbyte-test-utils")
include(":oss:airbyte-base-java-image")
include(":oss:airbyte-base-java-python-image")
include(":oss:airbyte-base-nginx-image")
include(":oss:airbyte-analytics")
include(":oss:airbyte-commons-temporal")
include(":oss:airbyte-commons-temporal-core")
include(":oss:airbyte-commons-converters")
include(":oss:airbyte-commons-worker")
include(":oss:airbyte-commons-workload")
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
include(":oss:airbyte-commons-entitlements")
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
include(":oss:airbyte-server")
include(":oss:airbyte-statistics")
include(":oss:airbyte-tests")
include(":oss:airbyte-webapp")
include(":oss:airbyte-workers")
include(":oss:airbyte-workload-launcher")
include(":oss:airbyte-connector-sidecar")
include(":oss:airbyte-workload-init-container")
include(":oss:airbyte-async-profiler")
include(":oss:airbyte-pmd-rules")

// Gradle wants an "oss" folder to be under this current folder, which is also "oss".  Use the mkdirs() to
// create the folder so that builds will work from the oss folder.  This is a hack to work around changes
// in Gradle 9 that enforce that directories exist.
project(":oss").projectDir.mkdirs()

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
project(":oss:airbyte-config").projectDir = file("airbyte-config")
project(":oss:airbyte-config:specs").projectDir = file("airbyte-config/specs")
project(":oss:airbyte-config:init").projectDir = file("airbyte-config/init")
project(":oss:airbyte-config:config-models").projectDir = file("airbyte-config/config-models")
project(":oss:airbyte-data").projectDir = file("airbyte-data")
project(":oss:airbyte-db").projectDir = file("airbyte-db")
project(":oss:airbyte-db:db-lib").projectDir = file("airbyte-db/db-lib")
project(":oss:airbyte-json-validation").projectDir = file("airbyte-json-validation")
project(":oss:airbyte-metrics").projectDir = file("airbyte-metrics")
project(":oss:airbyte-metrics:metrics-lib").projectDir = file("airbyte-metrics/metrics-lib")
project(":oss:airbyte-oauth").projectDir = file("airbyte-oauth")
project(":oss:airbyte-test-utils").projectDir = file("airbyte-test-utils")
project(":oss:airbyte-analytics").projectDir = file("airbyte-analytics")
project(":oss:airbyte-commons-temporal").projectDir = file("airbyte-commons-temporal")
project(":oss:airbyte-commons-temporal-core").projectDir = file("airbyte-commons-temporal-core")
project(":oss:airbyte-commons-converters").projectDir = file("airbyte-commons-converters")
project(":oss:airbyte-commons-worker").projectDir = file("airbyte-commons-worker")
project(":oss:airbyte-commons-workload").projectDir = file("airbyte-commons-workload")
project(":oss:airbyte-config:config-persistence").projectDir = file("airbyte-config/config-persistence")
project(":oss:airbyte-config:config-secrets").projectDir = file("airbyte-config/config-secrets")
project(":oss:airbyte-csp-check").projectDir = file("airbyte-csp-check")
project(":oss:airbyte-featureflag").projectDir = file("airbyte-featureflag")
project(":oss:airbyte-featureflag-server").projectDir = file("airbyte-featureflag-server")
project(":oss:airbyte-db:jooq").projectDir = file("airbyte-db/jooq")
project(":oss:airbyte-domain").projectDir = file("airbyte-domain")
project(":oss:airbyte-domain:models").projectDir = file("airbyte-domain/models")
project(":oss:airbyte-domain:services").projectDir = file("airbyte-domain/services")
project(":oss:airbyte-micronaut-temporal").projectDir = file("airbyte-micronaut-temporal")
project(":oss:airbyte-notification").projectDir = file("airbyte-notification")
project(":oss:airbyte-persistence").projectDir = file("airbyte-persistence")
project(":oss:airbyte-persistence:job-persistence").projectDir = file("airbyte-persistence/job-persistence")
project(":oss:airbyte-worker-models").projectDir = file("airbyte-worker-models")
project(":oss:airbyte-bootloader").projectDir = file("airbyte-bootloader")
project(":oss:airbyte-commons-auth").projectDir = file("airbyte-commons-auth")
project(":oss:airbyte-commons-entitlements").projectDir = file("airbyte-commons-entitlements")
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
project(":oss:airbyte-server").projectDir = file("airbyte-server")
project(":oss:airbyte-statistics").projectDir = file("airbyte-statistics")
project(":oss:airbyte-tests").projectDir = file("airbyte-tests")
project(":oss:airbyte-webapp").projectDir = file("airbyte-webapp")
project(":oss:airbyte-workers").projectDir = file("airbyte-workers")
project(":oss:airbyte-workload-launcher").projectDir = file("airbyte-workload-launcher")
project(":oss:airbyte-connector-sidecar").projectDir = file("airbyte-connector-sidecar")
project(":oss:airbyte-workload-init-container").projectDir = file("airbyte-workload-init-container")
project(":oss:airbyte-async-profiler").projectDir = file("airbyte-async-profiler")
project(":oss:airbyte-pmd-rules").projectDir = file("airbyte-pmd-rules")
project(":oss:airbyte-base-java-image").projectDir = file("airbyte-base-java-image")
project(":oss:airbyte-base-java-python-image").projectDir = file("airbyte-base-java-python-image")
project(":oss:airbyte-base-nginx-image").projectDir = file("airbyte-base-nginx-image")
