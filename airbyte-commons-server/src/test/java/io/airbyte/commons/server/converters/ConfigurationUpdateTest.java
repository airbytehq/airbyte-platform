/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConfigurationUpdateTest {

  private static final String IMAGE_REPOSITORY = "foo";
  private static final String IMAGE_TAG = "bar";
  private static final UUID UUID1 = UUID.randomUUID();
  private static final UUID UUID2 = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final JsonNode SPEC = CatalogHelpers.fieldsToJsonSchema(
      Field.of(JdbcUtils.USERNAME_KEY, JsonSchemaType.STRING),
      Field.of(JdbcUtils.PASSWORD_KEY, JsonSchemaType.STRING));
  private static final ConnectorSpecification CONNECTOR_SPECIFICATION = new ConnectorSpecification().withConnectionSpecification(SPEC);
  private static final JsonNode ORIGINAL_CONFIGURATION = Jsons.jsonNode(ImmutableMap.builder()
      .put(JdbcUtils.USERNAME_KEY, "airbyte")
      .put(JdbcUtils.PASSWORD_KEY, "abc")
      .build());
  private static final JsonNode NEW_CONFIGURATION = Jsons.jsonNode(ImmutableMap.builder()
      .put(JdbcUtils.USERNAME_KEY, "airbyte")
      .put(JdbcUtils.PASSWORD_KEY, "xyz")
      .build());
  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition();
  private static final ActorDefinitionVersion DEFINITION_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(IMAGE_REPOSITORY)
      .withDockerImageTag(IMAGE_TAG)
      .withSpec(CONNECTOR_SPECIFICATION);
  private static final SourceConnection ORIGINAL_SOURCE_CONNECTION = new SourceConnection()
      .withSourceId(UUID1)
      .withSourceDefinitionId(UUID2)
      .withWorkspaceId(WORKSPACE_ID)
      .withConfiguration(ORIGINAL_CONFIGURATION);
  private static final SourceConnection NEW_SOURCE_CONNECTION = new SourceConnection()
      .withSourceId(UUID1)
      .withSourceDefinitionId(UUID2)
      .withWorkspaceId(WORKSPACE_ID)
      .withConfiguration(NEW_CONFIGURATION);
  private static final StandardDestinationDefinition DESTINATION_DEFINITION = new StandardDestinationDefinition();
  private static final DestinationConnection ORIGINAL_DESTINATION_CONNECTION = new DestinationConnection()
      .withDestinationId(UUID1)
      .withDestinationDefinitionId(UUID2)
      .withWorkspaceId(WORKSPACE_ID)
      .withConfiguration(ORIGINAL_CONFIGURATION);
  private static final DestinationConnection NEW_DESTINATION_CONNECTION = new DestinationConnection()
      .withDestinationId(UUID1)
      .withDestinationDefinitionId(UUID2)
      .withWorkspaceId(WORKSPACE_ID)
      .withConfiguration(NEW_CONFIGURATION);

  private ConfigRepository configRepository;
  private SecretsRepositoryReader secretsRepositoryReader;
  private JsonSecretsProcessor secretsProcessor;
  private ConfigurationUpdate configurationUpdate;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private SourceService sourceService;
  private DestinationService destinationService;

  @BeforeEach
  void setup() {
    configRepository = mock(ConfigRepository.class);
    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);

    configurationUpdate = new ConfigurationUpdate(configRepository, secretsRepositoryReader, secretsProcessor, actorDefinitionVersionHelper,
        sourceService, destinationService);
  }

  @Test
  void testSourceUpdate() throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(sourceService.getSourceConnectionWithSecrets(UUID1)).thenReturn(ORIGINAL_SOURCE_CONNECTION);
    when(configRepository.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION);
    when(actorDefinitionVersionHelper.getSourceVersion(SOURCE_DEFINITION, WORKSPACE_ID, UUID1)).thenReturn(DEFINITION_VERSION);
    when(secretsProcessor.copySecrets(ORIGINAL_CONFIGURATION, NEW_CONFIGURATION, SPEC)).thenReturn(NEW_CONFIGURATION);

    final SourceConnection actual = configurationUpdate.source(UUID1, ORIGINAL_SOURCE_CONNECTION.getName(), NEW_CONFIGURATION);

    assertEquals(NEW_SOURCE_CONNECTION, actual);
    Mockito.verify(actorDefinitionVersionHelper).getSourceVersion(SOURCE_DEFINITION, WORKSPACE_ID, UUID1);
  }

  @Test
  void testDestinationUpdate()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(destinationService.getDestinationConnectionWithSecrets(UUID1)).thenReturn(ORIGINAL_DESTINATION_CONNECTION);
    when(configRepository.getStandardDestinationDefinition(UUID2)).thenReturn(DESTINATION_DEFINITION);
    when(actorDefinitionVersionHelper.getDestinationVersion(DESTINATION_DEFINITION, WORKSPACE_ID, UUID1)).thenReturn(DEFINITION_VERSION);
    when(secretsProcessor.copySecrets(ORIGINAL_CONFIGURATION, NEW_CONFIGURATION, SPEC)).thenReturn(NEW_CONFIGURATION);

    final DestinationConnection actual = configurationUpdate.destination(UUID1, ORIGINAL_DESTINATION_CONNECTION.getName(), NEW_CONFIGURATION);

    assertEquals(NEW_DESTINATION_CONNECTION, actual);
    Mockito.verify(actorDefinitionVersionHelper).getDestinationVersion(DESTINATION_DEFINITION, WORKSPACE_ID, UUID1);
  }

  @Test
  void testPartialUpdateSourceNoUpdate()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(sourceService.getSourceConnectionWithSecrets(UUID1)).thenReturn(Jsons.clone(ORIGINAL_SOURCE_CONNECTION));
    when(configRepository.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION);

    // Test updating nothing
    final SourceConnection noUpdate = configurationUpdate.partialSource(UUID1, null, null);
    assertEquals(ORIGINAL_SOURCE_CONNECTION, noUpdate);
  }

  @Test
  void testPartialUpdateSourceName()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(sourceService.getSourceConnectionWithSecrets(UUID1)).thenReturn(Jsons.clone(ORIGINAL_SOURCE_CONNECTION));
    when(configRepository.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION);

    // Test only giving a name
    final SourceConnection updatedName = configurationUpdate.partialSource(UUID1, "TEST", null);
    assertEquals(Jsons.clone(ORIGINAL_SOURCE_CONNECTION).withName("TEST"), updatedName);
  }

  @Test
  void testPartialUpdateSourceConfig()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(sourceService.getSourceConnectionWithSecrets(UUID1)).thenReturn(Jsons.clone(ORIGINAL_SOURCE_CONNECTION));
    when(configRepository.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION);

    // Test updating only configuration
    final SourceConnection updatedConfiguration = configurationUpdate.partialSource(UUID1, null, NEW_CONFIGURATION);
    assertEquals(NEW_SOURCE_CONNECTION, updatedConfiguration);
  }

  @Test
  void testPartialUpdateSourcePartialConfig()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(sourceService.getSourceConnectionWithSecrets(UUID1)).thenReturn(Jsons.clone(ORIGINAL_SOURCE_CONNECTION));
    when(configRepository.getStandardSourceDefinition(UUID2)).thenReturn(SOURCE_DEFINITION);

    // Test partial configuration update
    final JsonNode partialConfig = Jsons.jsonNode(Map.of(JdbcUtils.PASSWORD_KEY, "123"));
    final JsonNode expectedConfiguration = Jsons.jsonNode(ImmutableMap.builder()
        .put(JdbcUtils.USERNAME_KEY, "airbyte")
        .put(JdbcUtils.PASSWORD_KEY, "123")
        .build());
    final SourceConnection partialUpdateConfiguration = configurationUpdate.partialSource(UUID1, null, partialConfig);
    assertEquals(Jsons.clone(NEW_SOURCE_CONNECTION).withConfiguration(expectedConfiguration), partialUpdateConfiguration);
  }

}
