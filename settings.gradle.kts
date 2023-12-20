pluginManagement {
    repositories {
        // uncomment for local dev
//        maven {
//            name = "localPluginRepo"
//            url = uri("../.gradle-plugins-local")
//        }
        maven(url = "https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars")
        gradlePluginPortal()
        maven(url = "https://oss.sonatype.org/content/repositories/snapshots")
    }
    resolutionStrategy {
        eachPlugin {
            // We're using the 6.1.0-SNAPSHOT version of openapi-generator which contains a fix for generating nullable arrays (https://github.com/OpenAPITools/openapi-generator/issues/13025)
            // The snapshot version isn"t available in the main Gradle Plugin Portal, so we added the Sonatype snapshot repository above.
            // The useModule command below allows us to map from the plugin id, `org.openapi.generator`, to the underlying module (https://oss.sonatype.org/content/repositories/snapshots/org/openapitools/openapi-generator-gradle-plugin/6.1.0-SNAPSHOT/_
            if (requested.id.id == "org.openapi.generator") {
                useModule("org.openapitools:openapi-generator-gradle-plugin:${requested.version}")
            }
        }
    }
}

// Configure the gradle enterprise plugin to enable build scans. Enabling the plugin at the top of the settings file allows the build scan to record
// as much information as possible.
plugins {
    id("com.gradle.enterprise") version "3.15.1"
    id("com.github.burrunan.s3-build-cache") version "1.5"
}

gradleEnterprise {
    buildScan {
        termsOfServiceUrl = "https://gradle.com/terms-of-service"
        termsOfServiceAgree = "yes"
    }
}

gradleEnterprise {
    buildScan {
        buildScanPublished {
            file("scan-journal.log").writeText("${java.util.Date()} - $buildScanId - ${buildScanUri}\n")
        }
    }
}


val isCiServer = System.getenv().containsKey("CI")
//
buildCache {
    // we use a different caching mechanism for Dagger builds
    if (System.getenv("DAGGER") == null) {
        remote<com.github.burrunan.s3cache.AwsS3BuildCache> {
            region = "us-west-2"
            bucket = "ab-ci-cache"
            prefix = "platform-ci-cache/"
            isPush = isCiServer
            isEnabled = System.getenv().containsKey("S3_BUILD_CACHE_ACCESS_KEY_ID")
        }
    }
}

rootProject.name = "airbyte"

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
// shared
include(":airbyte-commons")

include(":airbyte-api")
include(":airbyte-api-server")
include(":airbyte-workload-api-server")
include(":airbyte-commons-protocol")
include(":airbyte-config:specs")
include(":airbyte-config:init")
include(":airbyte-config:config-models") // reused by acceptance tests in connector base.
include(":airbyte-data")
include(":airbyte-db:db-lib") // reused by acceptance tests in connector base.
include(":airbyte-json-validation")
include(":airbyte-metrics:metrics-lib")
include(":airbyte-oauth")
include(":airbyte-test-utils")

// airbyte-workers has a lot of dependencies.
include(":airbyte-analytics") // transitively used by airbyte-workers.
include(":airbyte-commons-temporal")
include(":airbyte-commons-temporal-core")
include(":airbyte-commons-converters")
include(":airbyte-commons-worker")
include(":airbyte-config:config-persistence") // transitively used by airbyte-workers.
include(":airbyte-config:config-secrets") //
include(":airbyte-featureflag")
include(":airbyte-db:jooq") // transitively used by airbyte-workers.
include(":airbyte-micronaut-temporal")
include(":airbyte-notification") // transitively used by airbyte-workers.
include(":airbyte-persistence:job-persistence") // transitively used by airbyte-workers.
include(":airbyte-worker-models")

include(":airbyte-bootloader")
include(":airbyte-commons-auth")
include(":airbyte-commons-license")
include(":airbyte-commons-micronaut")
include(":airbyte-commons-server")
include(":airbyte-commons-with-dependencies")
include(":airbyte-connector-builder-server")
include(":airbyte-container-orchestrator")
include(":airbyte-cron")
include(":airbyte-keycloak")
include(":airbyte-keycloak-setup")
include(":airbyte-metrics:reporter")
include(":airbyte-proxy")
include(":airbyte-server")
include(":airbyte-temporal")
include(":airbyte-tests")
include(":airbyte-webapp")
include(":airbyte-workers")
include(":airbyte-workload-launcher")
