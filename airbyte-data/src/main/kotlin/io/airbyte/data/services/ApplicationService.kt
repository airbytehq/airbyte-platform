/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser

interface ApplicationService {
  fun createApplication(
    user: AuthenticatedUser,
    name: String,
  ): Application

  fun listApplicationsByUser(user: AuthenticatedUser): List<Application>

  fun deleteApplication(
    user: AuthenticatedUser,
    applicationId: String,
  ): Application

  fun getToken(
    clientId: String,
    clientSecret: String,
  ): String
}
