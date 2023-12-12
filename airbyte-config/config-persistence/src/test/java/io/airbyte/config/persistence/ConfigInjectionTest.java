/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigInjectionTest extends BaseConfigDatabaseTest {

  private ConfigRepository configRepository;
  private ConfigInjector configInjector;

  private StandardSourceDefinition sourceDefinition;

  private JsonNode exampleConfig;

  private static final String SAMPLE_CONFIG_KEY = "my_config_key";
  private static final String SAMPLE_INJECTED_KEY = "injected_under";

  ConfigInjectionTest() {}

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        new ConnectionServiceJooqImpl(database),
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new HealthCheckServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretPersistenceConfigService),
        new OperationServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new WorkspaceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService));
    configInjector = new ConfigInjector(configRepository);
    exampleConfig = Jsons.jsonNode(Map.of(SAMPLE_CONFIG_KEY, 123));
  }

  @Test
  void testInject() throws IOException {
    createBaseObjects();

    final JsonNode injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId());
    assertEquals(123, injected.get(SAMPLE_CONFIG_KEY).longValue(), 123);
    assertEquals("a", injected.get("a").get(SAMPLE_INJECTED_KEY).asText());
    assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText());
    assertFalse(injected.has("c"));
  }

  @Test
  void testInjectOverwrite() throws IOException {
    createBaseObjects();

    ((ObjectNode) exampleConfig).set("a", new LongNode(123));
    ((ObjectNode) exampleConfig).remove(SAMPLE_CONFIG_KEY);

    final JsonNode injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId());
    assertEquals("a", injected.get("a").get(SAMPLE_INJECTED_KEY).asText());
    assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText());
    assertFalse(injected.has("c"));
  }

  @Test
  void testUpdate() throws IOException {
    createBaseObjects();

    // write an injection object with the same definition id and the same injection path - will update
    // the existing one
    configRepository.writeActorDefinitionConfigInjectionForPath(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId()).withInjectionPath("a").withJsonToInject(new TextNode("abc")));

    final JsonNode injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId());
    assertEquals(123, injected.get(SAMPLE_CONFIG_KEY).longValue(), 123);
    assertEquals("abc", injected.get("a").asText());
    assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText());
    assertFalse(injected.has("c"));
  }

  @Test
  void testCreate() throws IOException {
    createBaseObjects();

    // write an injection object with the same definition id and a new injection path - will create a
    // new one and leave the others in place
    configRepository.writeActorDefinitionConfigInjectionForPath(new ActorDefinitionConfigInjection()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId()).withInjectionPath("c").withJsonToInject(new TextNode("thirdInject")));

    final JsonNode injected = configInjector.injectConfig(exampleConfig, sourceDefinition.getSourceDefinitionId());
    assertEquals(123, injected.get(SAMPLE_CONFIG_KEY).longValue());
    assertEquals("a", injected.get("a").get(SAMPLE_INJECTED_KEY).asText());
    assertEquals("b", injected.get("b").get(SAMPLE_INJECTED_KEY).asText());
    assertEquals("thirdInject", injected.get("c").asText());
  }

  private void createBaseObjects() throws IOException {
    sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion);

    createInjection(sourceDefinition, "a");
    createInjection(sourceDefinition, "b");

    // unreachable injection, should not show up
    final StandardSourceDefinition otherSourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion2 = createBaseActorDefVersion(otherSourceDefinition.getSourceDefinitionId());
    configRepository.writeConnectorMetadata(otherSourceDefinition, actorDefinitionVersion2);
    createInjection(otherSourceDefinition, "c");
  }

  private ActorDefinitionConfigInjection createInjection(final StandardSourceDefinition definition, final String path)
      throws IOException {
    final ActorDefinitionConfigInjection injection = new ActorDefinitionConfigInjection().withActorDefinitionId(definition.getSourceDefinitionId())
        .withInjectionPath(path).withJsonToInject(Jsons.jsonNode(Map.of(SAMPLE_INJECTED_KEY, path)));

    configRepository.writeActorDefinitionConfigInjectionForPath(injection);
    return injection;
  }

  private static StandardSourceDefinition createBaseSourceDef() {
    final UUID id = UUID.randomUUID();

    return new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false);
  }

  private static ActorDefinitionVersion createBaseActorDefVersion(final UUID actorDefId) {
    return new ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag("1.0.0")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withSpec(new ConnectorSpecification().withProtocolVersion("0.1.0"));
  }

}
