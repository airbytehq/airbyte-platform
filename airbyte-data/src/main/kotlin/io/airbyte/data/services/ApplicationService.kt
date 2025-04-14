/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser

/**
 * An Application provides API access on behalf of a User.
 *
 */
interface ApplicationService {
  /**
   * Creates an Application associated with the [user].
   * @param user the [user] associated with this Application.
   * @property name the [name] of the Application that will be presented as a display name.
   * @return the new Application.
   */
  fun createApplication(
    user: AuthenticatedUser,
    name: String,
  ): Application

  /**
   * Lists all Applications for a [user].
   * @param user the [user] find Applications for.
   * @return a list of Applications.
   */
  fun listApplicationsByUser(user: AuthenticatedUser): List<Application>

  /**
   * Deletes an Application. The [user] must own the Application for it to be deleted.
   * @param user the [user] check the [applicationId] against.
   * @param applicationId the id of the Application to be deleted.
   * @return a list of Applications.
   */
  fun deleteApplication(
    user: AuthenticatedUser,
    applicationId: String,
  ): Application

  /**
   * Get an Access Token to access an API. Check that the clientId and clientSecret match an Application
   *
   * @param clientId the clientId should be used to locate the Application.
   * @param clientSecret the clientSecret must match an Applications clientSecret.
   * @return an Access Token that will provide access to an API.
   */
  fun getToken(
    clientId: String,
    clientSecret: String,
  ): String
}
