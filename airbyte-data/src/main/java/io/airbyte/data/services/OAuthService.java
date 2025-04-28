/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * OAuth Service.
 */
public interface OAuthService {

  void writeSourceOAuthParam(SourceOAuthParameter sourceOAuthParameter) throws IOException;

  Optional<SourceOAuthParameter> getSourceOAuthParameterWithSecretsOptional(UUID workspaceId, UUID sourceDefinitionId)
      throws IOException, ConfigNotFoundException;

  Optional<SourceOAuthParameter> getSourceOAuthParameterOptional(UUID workspaceId, UUID sourceDefinitionId)
      throws IOException;

  Optional<SourceOAuthParameter> getSourceOAuthParameterOptional(UUID workspaceId, UUID organizationId, UUID sourceDefinitionId)
      throws IOException;

  Optional<SourceOAuthParameter> getSourceOAuthParamByDefinitionIdOptional(Optional<UUID> workspaceId,
                                                                           Optional<UUID> organizationId,
                                                                           UUID sourceDefinitionId)
      throws IOException;

  void writeDestinationOAuthParam(DestinationOAuthParameter destinationOAuthParameter) throws IOException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParameterWithSecretsOptional(UUID workspaceId, UUID destinationDefinitionId)
      throws IOException, ConfigNotFoundException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParameterOptional(UUID workspaceId, UUID destinationDefinitionId)
      throws IOException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParameterOptional(UUID workspaceId, UUID organizationId, UUID destinationDefinitionId)
      throws IOException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParamByDefinitionIdOptional(Optional<UUID> workspaceId,
                                                                                     Optional<UUID> organizationId,
                                                                                     UUID destinationDefinitionId)
      throws IOException;

}
