/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.version.AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.javaparser.utils.StringEscapeUtils;
import com.google.common.hash.Hashing;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.protocol.models.v0.AdvancedAuth;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Adapter to go from Connector Builder concept to actor definitions.
 */
@Singleton
public class DeclarativeSourceManifestInjector {

  public static final String INJECTED_DECLARATIVE_MANIFEST_KEY = "__injected_declarative_manifest";
  public static final String INJECTED_COMPONENT_FILE_KEY = "__injected_components_py";
  public static final String INJECTED_COMPONENT_FILE_CHECKSUMS_KEY = "__injected_components_py_checksums";

  /**
   * Add __injected_declarative_manifest to the spec.
   *
   * @param spec Connector Builder spec
   */
  public void addInjectedDeclarativeManifest(final JsonNode spec) {
    ObjectNode properties = (ObjectNode) spec.path("connectionSpecification").path("properties");

    // Add each injection property as a top-level field
    properties.putObject(INJECTED_DECLARATIVE_MANIFEST_KEY)
        .put("type", "object")
        .put("additionalProperties", true)
        .put("airbyte_hidden", true);

    properties.putObject(INJECTED_COMPONENT_FILE_KEY)
        .put("type", "string")
        .put("airbyte_hidden", true);

    properties.putObject(INJECTED_COMPONENT_FILE_CHECKSUMS_KEY)
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
  public ActorDefinitionConfigInjection createManifestConfigInjection(final UUID sourceDefinitionId, final JsonNode manifest) {
    return new ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinitionId)
        .withInjectionPath(INJECTED_DECLARATIVE_MANIFEST_KEY)
        .withJsonToInject(manifest);
  }

  /**
   * Create ActorDefinitionConfigInjection with __injected_components_py.
   *
   * @param sourceDefinitionId matching the source definition the config needs to be injected
   * @param componentsFileContent content of the components file to be injected
   * @return ActorDefinitionConfigInjection configured to inject the components file content
   */
  public ActorDefinitionConfigInjection createComponentFileInjection(final UUID sourceDefinitionId, final JsonNode componentsFileContent) {
    return new ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinitionId)
        .withInjectionPath(INJECTED_COMPONENT_FILE_KEY)
        .withJsonToInject(componentsFileContent);
  }

  /**
   * Create ActorDefinitionConfigInjection with __injected_components_py_checksums.
   *
   * @param sourceDefinitionId matching the source definition the config needs to be injected
   * @param componentsFileContentString content of the components file to calculate checksums for
   * @return ActorDefinitionConfigInjection configured to inject the MD5 checksum of the components
   *         file
   */
  public ActorDefinitionConfigInjection createComponentFileChecksumsInjection(final UUID sourceDefinitionId,
                                                                              final String componentsFileContentString) {
    ObjectNode checksumNode = JsonNodeFactory.instance.objectNode();

    String md5Hash = computeMD5Hash(componentsFileContentString);
    checksumNode.put("md5", md5Hash);

    return new ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinitionId)
        .withInjectionPath(INJECTED_COMPONENT_FILE_CHECKSUMS_KEY)
        .withJsonToInject(checksumNode);
  }

  /**
   * Creates a list of ActorDefinitionConfigInjection objects for a declarative manifest connector.
   * This method handles both the manifest injection and optional custom code components.
   *
   * @param sourceDefinitionId The UUID identifying the source definition that needs the config
   *        injections
   * @param manifest The JsonNode containing the declarative manifest to be injected
   * @param componentFileContent The string content of any custom code components. Can be null if no
   *        custom code exists
   * @return A list of ActorDefinitionConfigInjection objects containing: - The manifest injection -
   *         The custom code injection (if custom code exists) - The custom code checksums injection
   *         (if custom code exists)
   * @throws IOException If there are issues serializing/deserializing the component file content
   */
  public List<ActorDefinitionConfigInjection> getManifestConnectorInjections(final UUID sourceDefinitionId,
                                                                             final JsonNode manifest,
                                                                             final String componentFileContent)
      throws IOException {
    List<ActorDefinitionConfigInjection> injections = new ArrayList<>();
    final ActorDefinitionConfigInjection manifestInjection = createManifestConfigInjection(sourceDefinitionId, manifest);
    injections.add(manifestInjection);

    final boolean hasCustomCode = componentFileContent != null && !componentFileContent.isEmpty();
    if (hasCustomCode) {
      final String wrappedFileContent = new ObjectMapper().writeValueAsString(componentFileContent);
      final JsonNode componentFileContentJson = Jsons.deserialize(wrappedFileContent);
      injections.add(createComponentFileInjection(sourceDefinitionId, componentFileContentJson));
      injections.add(createComponentFileChecksumsInjection(sourceDefinitionId, componentFileContent));
    }
    return injections;
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

  /**
   * Computes an MD5 hash of the provided content string.
   *
   * The content string is first unescaped using Java string unescaping rules to get the actual
   * content. This ensures the MD5 hash is computed consistently across different platforms and
   * matches what source-declarative-manifest expects.
   *
   * @param content The string content to hash, potentially containing escaped characters
   * @return The MD5 hash of the unescaped content as a hex string
   */
  private static String computeMD5Hash(String content) {
    final String unescapedContentString = StringEscapeUtils.unescapeJava(content);
    return Hashing.md5().hashBytes(unescapedContentString.getBytes()).toString();
  }

}
