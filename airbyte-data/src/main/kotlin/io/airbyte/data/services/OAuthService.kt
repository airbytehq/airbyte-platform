/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import java.util.Optional
import java.util.UUID

/**
 * OAuth Service.
 */
interface OAuthService {
  fun writeSourceOAuthParam(sourceOAuthParameter: SourceOAuthParameter)

  fun getSourceOAuthParameterWithSecretsOptional(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  fun getSourceOAuthParameterOptional(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  fun getSourceOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  fun getSourceOAuthParamByDefinitionIdOptional(
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  fun writeDestinationOAuthParam(destinationOAuthParameter: DestinationOAuthParameter)

  fun getDestinationOAuthParameterWithSecretsOptional(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>

  fun getDestinationOAuthParameterOptional(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>

  fun getDestinationOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>

  fun getDestinationOAuthParamByDefinitionIdOptional(
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>
}
