/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * OAuth Service.
 */
public interface OAuthService {

  Optional<SourceOAuthParameter> getSourceOAuthParamByDefinitionIdOptional(UUID workspaceId, UUID sourceDefinitionId) throws IOException;

  void writeSourceOAuthParam(SourceOAuthParameter sourceOAuthParameter) throws IOException;

  SourceOAuthParameter getSourceOAuthParameterWithSecrets(UUID workspaceId, UUID sourceDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  Optional<SourceOAuthParameter> getSourceOAuthParameterOptional(UUID workspaceId, UUID sourceDefinitionId)
      throws IOException;

  DestinationOAuthParameter getDestinationOAuthParameterWithSecrets(UUID workspaceId, UUID destinationDefinitionId)
      throws IOException, ConfigNotFoundException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParameterOptional(UUID workspaceId, UUID sourceDefinitionId)
      throws IOException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParamByDefinitionIdOptional(UUID workspaceId, UUID destinationDefinitionId)
      throws IOException;

  void writeDestinationOAuthParam(DestinationOAuthParameter destinationOAuthParameter) throws IOException;

}
