/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.StandardDestinationDefinition;
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
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionBreakingChangePersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID ACTOR_DEFINITION_ID_1 = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID_2 = UUID.randomUUID();

  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition()
      .withName("Test Source")
      .withSourceDefinitionId(ACTOR_DEFINITION_ID_1);

  private static final StandardDestinationDefinition DESTINATION_DEFINITION = new StandardDestinationDefinition()
      .withName("Test Destination")
      .withDestinationDefinitionId(ACTOR_DEFINITION_ID_2);

  private static final BreakingChangeScope BREAKING_CHANGE_SCOPE = new BreakingChangeScope()
      .withScopeType(ScopeType.STREAM)
      .withImpactedScopes(List.of("stream1", "stream2"));

  private static final ActorDefinitionBreakingChange BREAKING_CHANGE = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("1.0.0"))
      .withMessage("This is an older breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
      .withUpgradeDeadline("2025-01-21")
      .withScopedImpact(List.of(BREAKING_CHANGE_SCOPE));
  private static final ActorDefinitionBreakingChange BREAKING_CHANGE_2 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("2.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#2.0.0")
      .withUpgradeDeadline("2025-02-21");
  private static final ActorDefinitionBreakingChange BREAKING_CHANGE_3 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("3.0.0"))
      .withMessage("This is another breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#3.0.0")
      .withUpgradeDeadline("2025-03-21");
  private static final ActorDefinitionBreakingChange BREAKING_CHANGE_4 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("4.0.0"))
      .withMessage("This is some future breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#4.0.0")
      .withUpgradeDeadline("2025-03-21");
  private static final ActorDefinitionBreakingChange OTHER_CONNECTOR_BREAKING_CHANGE = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_2)
      .withVersion(new Version("1.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration-2#1.0.0")
      .withUpgradeDeadline("2025-01-21");

  final ActorDefinitionVersion createActorDefVersion(final UUID actorDefinitionId) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerImageTag("1.0.0")
        .withDockerRepository("repo")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withSpec(new ConnectorSpecification().withProtocolVersion("0.1.0"));
  }

  final ActorDefinitionVersion createActorDefVersion(final UUID actorDefinitionId, final String dockerImageTag) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerImageTag(dockerImageTag)
        .withDockerRepository("repo")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withSpec(new ConnectorSpecification().withProtocolVersion("0.1.0"));
  }

  private ConfigRepository configRepository;

  @BeforeEach
  void setup() throws SQLException, JsonValidationException, IOException {
    truncateAllTables();

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = spy(
        new ConfigRepository(
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
                secretPersistenceConfigService)));

    configRepository.writeConnectorMetadata(SOURCE_DEFINITION, createActorDefVersion(SOURCE_DEFINITION.getSourceDefinitionId()),
        List.of(BREAKING_CHANGE, BREAKING_CHANGE_2, BREAKING_CHANGE_3, BREAKING_CHANGE_4));
    configRepository.writeConnectorMetadata(DESTINATION_DEFINITION,
        createActorDefVersion(DESTINATION_DEFINITION.getDestinationDefinitionId()), List.of(OTHER_CONNECTOR_BREAKING_CHANGE));
  }

  @Test
  void testGetBreakingChanges() throws IOException {
    final List<ActorDefinitionBreakingChange> breakingChangesForDef1 = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(4, breakingChangesForDef1.size());
    assertEquals(BREAKING_CHANGE, breakingChangesForDef1.get(0));

    final List<ActorDefinitionBreakingChange> breakingChangesForDef2 = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_2);
    assertEquals(1, breakingChangesForDef2.size());
    assertEquals(OTHER_CONNECTOR_BREAKING_CHANGE, breakingChangesForDef2.get(0));
  }

  @Test
  void testUpdateActorDefinitionBreakingChange() throws IOException {
    // Update breaking change
    final ActorDefinitionBreakingChange updatedBreakingChange = new ActorDefinitionBreakingChange()
        .withActorDefinitionId(BREAKING_CHANGE.getActorDefinitionId())
        .withVersion(BREAKING_CHANGE.getVersion())
        .withMessage("Updated message")
        .withUpgradeDeadline("2025-12-12") // Updated date
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#updated-miration-url")
        .withScopedImpact(List.of(new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(List.of("stream3"))));
    configRepository.writeConnectorMetadata(SOURCE_DEFINITION, createActorDefVersion(SOURCE_DEFINITION.getSourceDefinitionId()),
        List.of(updatedBreakingChange, BREAKING_CHANGE_2, BREAKING_CHANGE_3, BREAKING_CHANGE_4));

    // Check updated breaking change
    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(4, breakingChanges.size());
    assertEquals(updatedBreakingChange, breakingChanges.get(0));
  }

  @Test
  void testListBreakingChanges() throws IOException {
    final List<ActorDefinitionBreakingChange> expectedAllBreakingChanges =
        List.of(BREAKING_CHANGE, BREAKING_CHANGE_2, BREAKING_CHANGE_3, BREAKING_CHANGE_4, OTHER_CONNECTOR_BREAKING_CHANGE);
    assertThat(expectedAllBreakingChanges).containsExactlyInAnyOrderElementsOf(configRepository.listBreakingChanges());
  }

  @Test
  void testListBreakingChangesForVersion() throws IOException {
    final ActorDefinitionVersion ADV_4_0_0 = createActorDefVersion(ACTOR_DEFINITION_ID_1, "4.0.0");
    configRepository.writeConnectorMetadata(SOURCE_DEFINITION, ADV_4_0_0);

    // no breaking changes for latest default
    assertEquals(4, configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1).size());
    assertEquals(0, configRepository.listBreakingChangesForActorDefinitionVersion(ADV_4_0_0).size());

    // should see future breaking changes for 2.0.0
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefVersion(ACTOR_DEFINITION_ID_1, "2.0.0");
    assertEquals(2, configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0).size());
    assertEquals(List.of(BREAKING_CHANGE_3, BREAKING_CHANGE_4), configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0));

    // move back default version for Actor Definition to 3.0.0, should stop seeing "rolled back"
    // breaking changes
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefVersion(ACTOR_DEFINITION_ID_1, "3.0.0");
    configRepository.writeConnectorMetadata(SOURCE_DEFINITION, ADV_3_0_0);
    assertEquals(1, configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0).size());
    assertEquals(List.of(BREAKING_CHANGE_3), configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0));
  }

}
