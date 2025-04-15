/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionServiceJooqImplTest extends BaseConfigDatabaseTest {

  private JooqTestDbSetupHelper jooqTestDbSetupHelper;
  private SourceService sourceService;
  private ActorDefinitionServiceJooqImpl actorDefinitionService;

  @BeforeEach
  void setUp() throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    this.actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any())).thenReturn("3600");

    final MetricClient metricClient = mock(MetricClient.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater =
        new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService, scopedConfigurationService,
            connectionTimelineEventService);
    this.sourceService = new SourceServiceJooqImpl(database, featureFlagClient,
        secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater, metricClient);

    jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();
  }

  @Test
  void updateActorDefinitionDefaultVersionId() throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID actorDefinitionId = jooqTestDbSetupHelper.getSourceDefinition().getSourceDefinitionId();
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId);
    final UUID initialSourceDefVersionId = sourceDefinition.getDefaultVersionId();

    final ActorDefinitionVersion newVersion =
        Jsons.clone(jooqTestDbSetupHelper.getSourceDefinitionVersion()).withVersionId(UUID.randomUUID()).withDockerImageTag("5.0.0");
    actorDefinitionService.writeActorDefinitionVersion(newVersion);

    actorDefinitionService.updateActorDefinitionDefaultVersionId(actorDefinitionId, newVersion.getVersionId());

    final StandardSourceDefinition updatedSourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId);
    assertEquals(updatedSourceDefinition.getDefaultVersionId(), newVersion.getVersionId());
    assertNotEquals(updatedSourceDefinition.getDefaultVersionId(), initialSourceDefVersionId);
  }

  @Test
  void testGetActorIdsForDefinition() throws IOException {
    final UUID actorDefinitionId = jooqTestDbSetupHelper.getSourceDefinition().getSourceDefinitionId();

    final UUID sourceActorId = jooqTestDbSetupHelper.getSource().getSourceId();
    final UUID workspaceId = jooqTestDbSetupHelper.getWorkspace().getWorkspaceId();
    final UUID organizationId = jooqTestDbSetupHelper.getOrganization().getOrganizationId();

    final List<ActorWorkspaceOrganizationIds> actorIds = actorDefinitionService.getActorIdsForDefinition(actorDefinitionId);
    assertEquals(List.of(new ActorWorkspaceOrganizationIds(sourceActorId, workspaceId, organizationId)), actorIds);
  }

}
