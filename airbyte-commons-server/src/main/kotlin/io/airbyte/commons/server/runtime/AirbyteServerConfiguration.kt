/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.runtime

import io.micronaut.context.annotation.ConfigurationProperties

const val DEFAULT_SERVER_CONNECTION_LIMIT_MAX_DAYS_WARNING = 4L
const val DEFAULT_SERVER_CONNECTION_LIMIT_MAX_DAYS = 7L
const val DEFAULT_SERVER_CONNECTION_LIMIT_MAX_JOBS_WARNING = 20L
const val DEFAULT_SERVER_CONNECTION_LIMIT_MAX_JOBS = 30L
const val DEFAULT_SERVER_CONNECTION_LIMIT_MAX_FIELDS_PER_CONNECTION = 20000L
const val DEFAULT_SERVER_LIMIT_CONNECTIONS = 10000L
const val DEFAULT_SERVER_LIMIT_SOURCES = 10000L
const val DEFAULT_SERVER_LIMIT_DESTINATIONS = 10000L
const val DEFAULT_SERVER_LIMIT_WORKSPACES = 10000L
const val DEFAULT_SERVER_LIMIT_USERS = 10000L

@ConfigurationProperties("airbyte.server")
data class AirbyteServerConfiguration(
  val connectionLimits: AirbyteServerConnectionLimitConfiguration = AirbyteServerConnectionLimitConfiguration(),
  val limits: AirbyteServerLimitConfiguration = AirbyteServerLimitConfiguration(),
) {
  @ConfigurationProperties("connection")
  data class AirbyteServerConnectionLimitConfiguration(
    val limits: AirbyteServerConnectionLimitsConfiguration = AirbyteServerConnectionLimitsConfiguration(),
  ) {
    @ConfigurationProperties("limits")
    data class AirbyteServerConnectionLimitsConfiguration(
      val maxDaysWarning: Long = DEFAULT_SERVER_CONNECTION_LIMIT_MAX_DAYS_WARNING,
      val maxDays: Long = DEFAULT_SERVER_CONNECTION_LIMIT_MAX_DAYS,
      val maxJobsWarning: Long = DEFAULT_SERVER_CONNECTION_LIMIT_MAX_JOBS_WARNING,
      val maxJobs: Long = DEFAULT_SERVER_CONNECTION_LIMIT_MAX_JOBS,
      val maxFieldsPerConnection: Long = DEFAULT_SERVER_CONNECTION_LIMIT_MAX_FIELDS_PER_CONNECTION,
    )
  }

  @ConfigurationProperties("limits")
  data class AirbyteServerLimitConfiguration(
    val connections: Long = DEFAULT_SERVER_LIMIT_CONNECTIONS,
    val sources: Long = DEFAULT_SERVER_LIMIT_SOURCES,
    val destinations: Long = DEFAULT_SERVER_LIMIT_DESTINATIONS,
    val workspaces: Long = DEFAULT_SERVER_LIMIT_WORKSPACES,
    val users: Long = DEFAULT_SERVER_LIMIT_USERS,
  )
}
