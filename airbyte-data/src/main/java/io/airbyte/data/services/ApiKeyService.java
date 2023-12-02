/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ApiKey;
import io.airbyte.config.User;
import java.util.List;
import java.util.Optional;

public interface ApiKeyService {

  ApiKey createApiKeyForUser(User userId);

  List<ApiKey> listApiKeysForUser(User userId);

  Optional<ApiKey> deleteApiKey(String apiKeyId);

}
