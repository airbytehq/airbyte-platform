/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Singleton;
import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter to go from specs in the Connector Builder to ConnectorSpecification.
 */
@Singleton
public class ConnectorBuilderSpecAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorBuilderSpecAdapter.class);

  /**
   * Adapt from Connector Builder to platform.
   *
   * @param spec Connector Builder spec
   * @return ConnectorSpecification that can be saved in DB
   */
  public ConnectorSpecification adapt(final JsonNode spec) {
    addInjectedDeclarativeManifest(spec);
    return new ConnectorSpecification()
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
        // this should always be aligned with the airbyte-cdk version
        .withProtocolVersion(DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
        .withDocumentationUrl(parseDocumentationUrl(spec.path("documentationUrl").asText("")))
        .withConnectionSpecification(spec.get("connectionSpecification"));
  }

  private static void addInjectedDeclarativeManifest(final JsonNode spec) {
    ((ObjectNode) spec.path("connectionSpecification").path("properties"))
        .putObject("__injected_declarative_manifest")
        .put("type", "object")
        .put("additionalProperties", true);
  }

  private URI parseDocumentationUrl(final String documentationUrl) {
    try {
      return new URI(documentationUrl);
    } catch (final URISyntaxException e) {
      LOGGER.info("Documentation URL {} is not valid", documentationUrl);
      return URI.create("");
    }
  }

}
