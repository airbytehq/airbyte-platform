/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * OAuth Service.
 */
public interface OAuthService {

  Stream<SourceOAuthParameter> listSourceOauthParamQuery(Optional<UUID> configId) throws IOException;

  Optional<SourceOAuthParameter> getSourceOAuthParamByDefinitionIdOptional(UUID workspaceId, UUID sourceDefinitionId) throws IOException;

  void writeSourceOAuthParam(SourceOAuthParameter sourceOAuthParameter) throws IOException;

  void writeSourceOauthParameter(List<SourceOAuthParameter> configs);

  List<SourceOAuthParameter> listSourceOAuthParam() throws JsonValidationException, IOException;

  /**
   * List destination oauth param query. If configId is present only returns the config for that oauth
   * parameter id. if not present then lists all.
   *
   * @param configId oauth parameter id optional.
   * @return stream of destination oauth params
   * @throws IOException if there is an issue while interacting with db.
   */
  Stream<DestinationOAuthParameter> listDestinationOauthParamQuery(Optional<UUID> configId) throws IOException;

  Optional<DestinationOAuthParameter> getDestinationOAuthParamByDefinitionIdOptional(UUID workspaceId, UUID destinationDefinitionId)
      throws IOException;

  void writeDestinationOAuthParam(DestinationOAuthParameter destinationOAuthParameter) throws IOException;

  void writeDestinationOauthParameter(List<DestinationOAuthParameter> configs);

  List<DestinationOAuthParameter> listDestinationOAuthParam()
      throws JsonValidationException, IOException;

}
