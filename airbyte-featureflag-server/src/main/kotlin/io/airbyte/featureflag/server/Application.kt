package io.airbyte.featureflag.server

import io.micronaut.runtime.Micronaut.build

fun main(args: Array<String>) {
  build(*args).deduceCloudEnvironment(false).deduceEnvironment(false).start()
}
