/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.airbyte.api.client.WebUrlHelper;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.Configs;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Report errors from Jobs. Common error information that can be sent to any of the reporting
 * clients that we support.
 */
public class JobErrorReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobErrorReporter.class);
  private static final String FROM_TRACE_MESSAGE = "from_trace_message";
  public static final String AIRBYTE_EDITION_META_KEY = "airbyte_edition";
  public static final String AIRBYTE_VERSION_META_KEY = "airbyte_version";
  public static final String FAILURE_ORIGIN_META_KEY = "failure_origin";
  public static final String FAILURE_TYPE_META_KEY = "failure_type";
  public static final String WORKSPACE_ID_META_KEY = "workspace_id";
  public static final String WORKSPACE_URL_META_KEY = "workspace_url";
  public static final String CONNECTION_ID_META_KEY = "connection_id";
  public static final String CONNECTION_URL_META_KEY = "connection_url";
  public static final String CONNECTOR_NAME_META_KEY = "connector_name";
  public static final String CONNECTOR_REPOSITORY_META_KEY = "connector_repository";
  public static final String CONNECTOR_DEFINITION_ID_META_KEY = "connector_definition_id";
  public static final String CONNECTOR_RELEASE_STAGE_META_KEY = "connector_release_stage";
  private static final String CONNECTOR_INTERNAL_SUPPORT_LEVEL_META_KEY = "connector_internal_support_level";
  private static final String CONNECTOR_COMMAND_META_KEY = "connector_command";
  public static final String JOB_ID_KEY = "job_id";

  private static final Set<FailureType> UNSUPPORTED_FAILURETYPES =
      ImmutableSet.of(FailureType.CONFIG_ERROR, FailureType.MANUAL_CANCELLATION, FailureType.TRANSIENT_ERROR);

  private final ActorDefinitionService actorDefinitionService;
  private final DestinationService destinationService;
  private final SourceService sourceService;
  private final WorkspaceService workspaceService;
  private final Configs.AirbyteEdition airbyteEdition;
  private final String airbyteVersion;
  private final WebUrlHelper webUrlHelper;
  private final JobErrorReportingClient jobErrorReportingClient;

  public JobErrorReporter(final ActorDefinitionService actorDefinitionService,
                          final SourceService sourceService,
                          final DestinationService destinationService,
                          final WorkspaceService workspaceService,
                          final Configs.AirbyteEdition airbyteEdition,
                          final String airbyteVersion,
                          final WebUrlHelper webUrlHelper,
                          final JobErrorReportingClient jobErrorReportingClient) {
    this.actorDefinitionService = actorDefinitionService;
    this.destinationService = destinationService;
    this.sourceService = sourceService;
    this.workspaceService = workspaceService;
    this.airbyteEdition = airbyteEdition;
    this.airbyteVersion = airbyteVersion;
    this.webUrlHelper = webUrlHelper;
    this.jobErrorReportingClient = jobErrorReportingClient;
  }

  /**
   * Reports a Sync Job's connector-caused FailureReasons to the JobErrorReportingClient.
   *
   * @param connectionId - connection that had the failure
   * @param failureSummary - final attempt failure summary
   * @param jobContext - sync job reporting context
   */
  public void reportSyncJobFailure(final UUID connectionId,
                                   final AttemptFailureSummary failureSummary,
                                   final SyncJobReportingContext jobContext,
                                   @Nullable final AttemptConfigReportingContext attemptConfig) {
    Exceptions.swallow(() -> {
      try {
        LOGGER.info("{} failures incoming for jobId '{}' connectionId '{}'",
            failureSummary.getFailures() == null ? 0 : failureSummary.getFailures().size(), jobContext.jobId(), connectionId);
        final List<FailureReason> traceMessageFailures = failureSummary.getFailures().stream()
            .filter(failure -> failure.getMetadata() != null && failure.getMetadata().getAdditionalProperties().containsKey(FROM_TRACE_MESSAGE))
            .toList();

        final StandardWorkspace workspace = workspaceService.getStandardWorkspaceFromConnection(connectionId, true);
        final Map<String, String> commonMetadata = MoreMaps.merge(
            Map.of(JOB_ID_KEY, String.valueOf(jobContext.jobId())),
            getConnectionMetadata(workspace.getWorkspaceId(), connectionId));

        LOGGER.info("{} failures to report for jobId '{}' connectionId '{}'", traceMessageFailures.size(), jobContext.jobId(), connectionId);
        for (final FailureReason failureReason : traceMessageFailures) {
          final FailureOrigin failureOrigin = failureReason.getFailureOrigin();
          LOGGER.info("Reporting failure for jobId '{}' connectionId '{}' origin '{}'", jobContext.jobId(), connectionId, failureOrigin);

          // We only care about the failure origins listed below, i.e. those that come from connectors.
          // The rest are ignored.
          if (failureOrigin == FailureOrigin.SOURCE) {
            final StandardSourceDefinition sourceDefinition = sourceService.getSourceDefinitionFromConnection(connectionId);
            final ActorDefinitionVersion sourceVersion = actorDefinitionService.getActorDefinitionVersion(jobContext.sourceVersionId());
            final String dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceVersion);
            final Map<String, String> metadata =
                MoreMaps.merge(commonMetadata,
                    getSourceMetadata(sourceDefinition, dockerImage, sourceVersion.getReleaseStage(), sourceVersion.getInternalSupportLevel()));

            reportJobFailureReason(workspace, failureReason, dockerImage, metadata, attemptConfig);
          } else if (failureOrigin == FailureOrigin.DESTINATION) {
            final StandardDestinationDefinition destinationDefinition = destinationService.getDestinationDefinitionFromConnection(connectionId);
            final ActorDefinitionVersion destinationVersion = actorDefinitionService.getActorDefinitionVersion(jobContext.destinationVersionId());
            final String dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationVersion);
            final Map<String, String> metadata =
                MoreMaps.merge(commonMetadata, getDestinationMetadata(destinationDefinition, dockerImage, destinationVersion.getReleaseStage(),
                    destinationVersion.getInternalSupportLevel()));

            reportJobFailureReason(workspace, failureReason, dockerImage, metadata, attemptConfig);
          }
        }
      } catch (final Exception e) {
        LOGGER.error("Failed to report status for jobId '{}' connectionId '{}': {}", jobContext.jobId(), connectionId, e);
        throw e;
      }
    });
  }

  /**
   * Reports a FailureReason from a connector Check job for a Source to the JobErrorReportingClient.
   *
   * @param workspaceId - workspace for which the check failed
   * @param failureReason - failure reason from the check connection job
   * @param jobContext - connector job reporting context
   */
  public void reportSourceCheckJobFailure(final UUID sourceDefinitionId,
                                          @Nullable final UUID workspaceId,
                                          final FailureReason failureReason,
                                          final ConnectorJobReportingContext jobContext)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    if (failureReason.getFailureOrigin() != FailureOrigin.SOURCE) {
      return;
    }
    final StandardWorkspace workspace = workspaceId != null ? workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) : null;
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId);
    final Map<String, String> metadata = MoreMaps.merge(
        getSourceMetadata(sourceDefinition, jobContext.dockerImage(), jobContext.releaseStage(), jobContext.internalSupportLevel()),
        Map.of(JOB_ID_KEY, jobContext.jobId().toString()));
    reportJobFailureReason(workspace, failureReason.withFailureOrigin(FailureOrigin.SOURCE), jobContext.dockerImage(), metadata, null);
  }

  /**
   * Reports a FailureReason from a connector Check job for a Destination to the
   * JobErrorReportingClient.
   *
   * @param workspaceId - workspace for which the check failed
   * @param failureReason - failure reason from the check connection job
   * @param jobContext - connector job reporting context
   */
  public void reportDestinationCheckJobFailure(final UUID destinationDefinitionId,
                                               @Nullable final UUID workspaceId,
                                               final FailureReason failureReason,
                                               final ConnectorJobReportingContext jobContext)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    if (failureReason.getFailureOrigin() != FailureOrigin.DESTINATION) {
      return;
    }
    final StandardWorkspace workspace = workspaceId != null ? workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) : null;
    final StandardDestinationDefinition destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId);
    final Map<String, String> metadata = MoreMaps.merge(
        getDestinationMetadata(destinationDefinition, jobContext.dockerImage(), jobContext.releaseStage(), jobContext.internalSupportLevel()),
        Map.of(JOB_ID_KEY, jobContext.jobId().toString()));
    reportJobFailureReason(workspace, failureReason.withFailureOrigin(FailureOrigin.DESTINATION), jobContext.dockerImage(), metadata, null);
  }

  /**
   * Reports a FailureReason from a connector Deploy job for a Source to the JobErrorReportingClient.
   *
   * @param workspaceId - workspace for which the Discover job failed
   * @param failureReason - failure reason from the Discover job
   * @param jobContext - connector job reporting context
   */
  public void reportDiscoverJobFailure(final UUID actorDefinitionId,
                                       final ActorType actorType,
                                       @Nullable final UUID workspaceId,
                                       final FailureReason failureReason,
                                       final ConnectorJobReportingContext jobContext)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    if (failureReason.getFailureOrigin() != FailureOrigin.SOURCE && failureReason.getFailureOrigin() != FailureOrigin.DESTINATION) {
      return;
    }
    final StandardWorkspace workspace = workspaceId != null ? workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) : null;
    final Map<String, String> actorDefMetadata = actorType == ActorType.DESTINATION
        ? getDestinationMetadata(destinationService.getStandardDestinationDefinition(actorDefinitionId), jobContext.dockerImage(),
            jobContext.releaseStage(), jobContext.internalSupportLevel())
        : getSourceMetadata(sourceService.getStandardSourceDefinition(actorDefinitionId), jobContext.dockerImage(), jobContext.releaseStage(),
            jobContext.internalSupportLevel());
    final Map<String, String> metadata = MoreMaps.merge(
        actorDefMetadata,
        Map.of(JOB_ID_KEY, jobContext.jobId().toString()));
    reportJobFailureReason(workspace, failureReason, jobContext.dockerImage(), metadata, null);
  }

  /**
   * Reports a FailureReason from a connector Spec job to the JobErrorReportingClient.
   *
   * @param failureReason - failure reason from the Deploy job
   * @param jobContext - connector job reporting context
   */
  public void reportSpecJobFailure(final FailureReason failureReason, final ConnectorJobReportingContext jobContext) {
    if (failureReason.getFailureOrigin() != FailureOrigin.SOURCE && failureReason.getFailureOrigin() != FailureOrigin.DESTINATION) {
      return;
    }
    final String dockerImage = jobContext.dockerImage();
    final String connectorRepository = dockerImage.split(":")[0];
    final Map<String, String> metadata = Map.of(
        JOB_ID_KEY, jobContext.jobId().toString(),
        CONNECTOR_REPOSITORY_META_KEY, connectorRepository);
    reportJobFailureReason(null, failureReason, dockerImage, metadata, null);
  }

  private Map<String, String> getConnectionMetadata(final UUID workspaceId, final UUID connectionId) {
    final String connectionUrl = webUrlHelper.getConnectionUrl(workspaceId, connectionId);
    return Map.ofEntries(
        Map.entry(CONNECTION_ID_META_KEY, connectionId.toString()),
        Map.entry(CONNECTION_URL_META_KEY, connectionUrl));
  }

  private Map<String, String> getDestinationMetadata(final StandardDestinationDefinition destinationDefinition,
                                                     final String dockerImage,
                                                     @Nullable final ReleaseStage releaseStage,
                                                     @Nullable final Long internalSupportLevel) {
    final String connectorRepository = dockerImage.split(":")[0];

    final Map<String, String> metadata = new HashMap<>(Map.ofEntries(
        Map.entry(CONNECTOR_DEFINITION_ID_META_KEY, destinationDefinition.getDestinationDefinitionId().toString()),
        Map.entry(CONNECTOR_NAME_META_KEY, destinationDefinition.getName()),
        Map.entry(CONNECTOR_REPOSITORY_META_KEY, connectorRepository)));
    if (releaseStage != null) {
      metadata.put(CONNECTOR_RELEASE_STAGE_META_KEY, releaseStage.value());
    }
    if (internalSupportLevel != null) {
      metadata.put(CONNECTOR_INTERNAL_SUPPORT_LEVEL_META_KEY, Long.toString(internalSupportLevel));
    }

    return metadata;
  }

  private Map<String, String> getSourceMetadata(final StandardSourceDefinition sourceDefinition,
                                                final String dockerImage,
                                                @Nullable final ReleaseStage releaseStage,
                                                @Nullable final Long internalSupportLevel) {
    final String connectorRepository = dockerImage.split(":")[0];
    final Map<String, String> metadata = new HashMap<>(Map.ofEntries(
        Map.entry(CONNECTOR_DEFINITION_ID_META_KEY, sourceDefinition.getSourceDefinitionId().toString()),
        Map.entry(CONNECTOR_NAME_META_KEY, sourceDefinition.getName()),
        Map.entry(CONNECTOR_REPOSITORY_META_KEY, connectorRepository)));
    if (releaseStage != null) {
      metadata.put(CONNECTOR_RELEASE_STAGE_META_KEY, releaseStage.value());
    }
    if (internalSupportLevel != null) {
      metadata.put(CONNECTOR_INTERNAL_SUPPORT_LEVEL_META_KEY, Long.toString(internalSupportLevel));
    }

    return metadata;
  }

  private Map<String, String> getFailureReasonMetadata(final FailureReason failureReason) {
    final Map<String, Object> failureReasonAdditionalProps =
        failureReason.getMetadata() != null ? failureReason.getMetadata().getAdditionalProperties() : Map.of();
    final Map<String, String> outMetadata = new HashMap<>();

    if (failureReasonAdditionalProps.containsKey(CONNECTOR_COMMAND_META_KEY)
        && failureReasonAdditionalProps.get(CONNECTOR_COMMAND_META_KEY) != null) {
      outMetadata.put(CONNECTOR_COMMAND_META_KEY, failureReasonAdditionalProps.get(CONNECTOR_COMMAND_META_KEY).toString());
    }

    if (failureReason.getFailureOrigin() != null) {
      outMetadata.put(FAILURE_ORIGIN_META_KEY, failureReason.getFailureOrigin().value());
    }

    if (failureReason.getFailureType() != null) {
      outMetadata.put(FAILURE_TYPE_META_KEY, failureReason.getFailureType().value());
    }

    return outMetadata;
  }

  private Map<String, String> getWorkspaceMetadata(final UUID workspaceId) {
    final String workspaceUrl = webUrlHelper.getWorkspaceUrl(workspaceId);
    return Map.ofEntries(
        Map.entry(WORKSPACE_ID_META_KEY, workspaceId.toString()),
        Map.entry(WORKSPACE_URL_META_KEY, workspaceUrl));
  }

  private void reportJobFailureReason(@Nullable final StandardWorkspace workspace,
                                      final FailureReason failureReason,
                                      final String dockerImage,
                                      final Map<String, String> metadata,
                                      @Nullable final AttemptConfigReportingContext attemptConfig) {
    // Failure types associated with a config-error or a manual-cancellation should NOT be reported.
    if (UNSUPPORTED_FAILURETYPES.contains(failureReason.getFailureType())) {
      return;
    }

    final Map<String, String> commonMetadata = new HashMap<>(Map.ofEntries(
        Map.entry(AIRBYTE_VERSION_META_KEY, airbyteVersion),
        Map.entry(AIRBYTE_EDITION_META_KEY, airbyteEdition.name())));

    if (workspace != null) {
      commonMetadata.putAll(getWorkspaceMetadata(workspace.getWorkspaceId()));
    }

    final Map<String, String> allMetadata = MoreMaps.merge(
        commonMetadata,
        getFailureReasonMetadata(failureReason),
        metadata);

    try {
      jobErrorReportingClient.reportJobFailureReason(workspace, failureReason, dockerImage, allMetadata, attemptConfig);
    } catch (final Exception e) {
      LOGGER.error("Error when reporting job failure reason: {}", failureReason, e);
    }
  }

}
