package io.airbyte.workload.server

import io.micronaut.runtime.Micronaut.run
import io.swagger.v3.oas.annotations.OpenAPIDefinition
import io.swagger.v3.oas.annotations.info.Info
import io.swagger.v3.oas.annotations.servers.Server

@OpenAPIDefinition(
  info =
    Info(
      title = "WorkloadApi",
      description = "API managing the workload",
      version = "1.0.0",
    ),
  servers = [Server(url = "http://localhost:8007/api")],
)
class Application {
  companion object {
    @JvmStatic fun main(args: Array<String>) {
      run(*args)
    }
  }
}
