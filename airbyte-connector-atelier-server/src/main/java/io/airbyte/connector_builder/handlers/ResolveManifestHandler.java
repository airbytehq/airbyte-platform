/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.requester.AirbyteCdkRequester;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle /manifest/resolve requests.
 */
@Singleton
public class ResolveManifestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResolveManifestHandler.class);
  private static final HandlerHelper helper = new HandlerHelper();

  private final AirbyteCdkRequester requester;

  @Inject
  public ResolveManifestHandler(
                                final AirbyteCdkRequester requester) {
    this.requester = requester;
  }

  /**
   * Use the requester to send the resolve_manifest request to the CDK.
   */
  public ResolveManifest resolveManifest(
                                         final ResolveManifestRequestBody resolveManifestRequestBody)
      throws AirbyteCdkInvalidInputException, ConnectorBuilderException {
    try {
      LOGGER.info("Handling resolve_manifest request.");
      return this.requester.resolveManifest(
          helper.getManifestJson(resolveManifestRequestBody.getManifest()));
    } catch (final IOException exc) {
      LOGGER.error("Error handling list_streams request.", exc);
      throw new ConnectorBuilderException("Error handling resolve_manifest request.", exc);
    }
  }

}
