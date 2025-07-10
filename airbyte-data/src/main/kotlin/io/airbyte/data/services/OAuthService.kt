/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.data.ConfigNotFoundException
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * OAuth Service.
 */
interface OAuthService {
  @Throws(IOException::class)
  fun writeSourceOAuthParam(sourceOAuthParameter: SourceOAuthParameter)

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getSourceOAuthParameterWithSecretsOptional(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  @Throws(IOException::class)
  fun getSourceOAuthParameterOptional(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  @Throws(IOException::class)
  fun getSourceOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  @Throws(IOException::class)
  fun getSourceOAuthParamByDefinitionIdOptional(
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter>

  @Throws(IOException::class)
  fun writeDestinationOAuthParam(destinationOAuthParameter: DestinationOAuthParameter)

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getDestinationOAuthParameterWithSecretsOptional(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>

  @Throws(IOException::class)
  fun getDestinationOAuthParameterOptional(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>

  @Throws(IOException::class)
  fun getDestinationOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>

  @Throws(IOException::class)
  fun getDestinationOAuthParamByDefinitionIdOptional(
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter>
}
