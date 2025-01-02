/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.Application;
import io.airbyte.config.AuthenticatedUser;
import java.util.List;
import java.util.Optional;

public interface ApplicationService {

  Application createApplication(AuthenticatedUser user, String name);

  List<Application> listApplicationsByUser(AuthenticatedUser user);

  Optional<Application> deleteApplication(AuthenticatedUser user, String applicationId);

  String getToken(String clientId, String clientSecret);

}
