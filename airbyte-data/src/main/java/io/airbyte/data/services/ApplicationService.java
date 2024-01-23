/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.Application;
import io.airbyte.config.User;
import java.util.List;
import java.util.Optional;

public interface ApplicationService {

  Application createApplication(User user, String name);

  List<Application> listApplicationsByUser(User user);

  Optional<Application> deleteApplication(User user, String applicationId);

  String getToken(String clientId, String clientSecret);

}
