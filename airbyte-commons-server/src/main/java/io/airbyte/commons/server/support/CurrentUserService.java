/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.AuthenticatedUser;

/**
 * Interface for retrieving the User associated with the current request.
 */
public interface CurrentUserService {

  AuthenticatedUser getCurrentUser();

}
