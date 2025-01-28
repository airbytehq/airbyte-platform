/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.UUID;

/**
 * Adapter to go from Connector Builder concept to actor definitions.
 */
@Singleton
public class DeclarativeSourceManifestInjector {

  private static final String INJECTED_DECLARATIVE_MANIFEST = "__injected_declarative_manifest";

  /**
   * Add __injected_declarative_manifest to the spec.
   *
   * @param spec Connector Builder spec
   */
  public void addInjectedDeclarativeManifest(final JsonNode spec) {
    ((ObjectNode) spec.path("connectionSpecification").path("properties"))
        .putObject(INJECTED_DECLARATIVE_MANIFEST)
        .put("type", "object")
        .put("additionalProperties", true)
        .put("airbyte_hidden", true);
  }

  /**
   * Create ActorDefinitionConfigInjection with __injected_declarative_manifest.
   *
   * @param sourceDefinitionId matching the source definition the config needs to be injected
   * @param manifest to be injected
   */
  public ActorDefinitionConfigInjection createConfigInjection(final UUID sourceDefinitionId, final JsonNode manifest) {
    return new ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinitionId)
        .withInjectionPath(INJECTED_DECLARATIVE_MANIFEST)
        .withJsonToInject(manifest);
  }

  /**
   * Adapt the spec to match the actor definition.
   *
   * @param declarativeManifestSpec to be adapted to a ConnectorSpecification
   */
  public ConnectorSpecification createDeclarativeManifestConnectorSpecification(final JsonNode declarativeManifestSpec) {
    return new ConnectorSpecification()
        .withSupportsDBT(false)
        .withSupportsNormalization(false)
        // FIXME should be aligned with the airbyte-cdk version but will be addressed as part of
        // https://github.com/airbytehq/airbyte/issues/24047
        .withProtocolVersion(DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize())
        .withDocumentationUrl(URI.create(declarativeManifestSpec.path("documentationUrl").asText("")))
        .withConnectionSpecification(declarativeManifestSpec.get("connectionSpecification"))
        .withAdvancedAuth(Jsons.object(declarativeManifestSpec.get("advancedAuth"), AdvancedAuth.class));
  }

  /**
   * Get the CDK version form the manifest.
   *
   * @param manifest to extract the CDK version from
   * @return the CDK version
   */
  public Version getCdkVersion(final JsonNode manifest) {
    return new Version(manifest.get("version").asText());
  }

}
