/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import com.google.common.base.Preconditions;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.Schedule;
import io.airbyte.config.ScheduleData;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.ScheduleType;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

// todo (cgardens) - we are not getting any value out of instantiating this class. we should just
// use it as statics. not doing it now, because already in the middle of another refactor.
/**
 * Connection Helpers.
 */
@Singleton
public class ConnectionHelper {

  private final ConnectionService connectionService;
  private final WorkspaceHelper workspaceHelper;

  public ConnectionHelper(final ConnectionService connectionService, final WorkspaceHelper workspaceHelper) {
    this.connectionService = connectionService;
    this.workspaceHelper = workspaceHelper;
  }

  public void deleteConnection(final UUID connectionId) throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardSync update = Jsons.clone(connectionService.getStandardSync(connectionId).withStatus(StandardSync.Status.DEPRECATED));
    updateConnection(update);
  }

  /**
   * Given a connection update, fetches an existing connection, applies the update, and then persists
   * the update.
   *
   * @param update - updated sync info to be merged with original sync.
   * @return new sync object
   * @throws JsonValidationException - if provided object is invalid
   * @throws ConfigNotFoundException - if there is no sync already persisted
   * @throws IOException - you never know when you io
   */
  public StandardSync updateConnection(final StandardSync update)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync original = connectionService.getStandardSync(update.getConnectionId());
    final StandardSync newConnection = updateConnectionObject(workspaceHelper, original, update);
    connectionService.writeStandardSync(newConnection);
    return newConnection;
  }

  /**
   * Core logic for merging an existing connection configuration with an update.
   *
   * @param workspaceHelper - helper class
   * @param original - already persisted sync
   * @param update - updated sync info to be merged with original sync.
   * @return new sync object
   */
  public static StandardSync updateConnectionObject(final WorkspaceHelper workspaceHelper, final StandardSync original, final StandardSync update) {
    validateWorkspace(workspaceHelper, original.getSourceId(), original.getDestinationId(), update.getOperationIds());

    final StandardSync newConnection = Jsons.clone(original)
        .withNamespaceDefinition(Enums.convertTo(update.getNamespaceDefinition(), NamespaceDefinitionType.class))
        .withNamespaceFormat(update.getNamespaceFormat())
        .withPrefix(update.getPrefix())
        .withOperationIds(update.getOperationIds())
        .withCatalog(update.getCatalog())
        .withStatus(update.getStatus())
        .withSourceCatalogId(update.getSourceCatalogId());

    // update name
    if (update.getName() != null) {
      newConnection.withName(update.getName());
    }

    // update Resource Requirements
    if (update.getResourceRequirements() != null) {
      newConnection.withResourceRequirements(Jsons.clone(update.getResourceRequirements()));
    } else {
      newConnection.withResourceRequirements(original.getResourceRequirements());
    }

    if (update.getScheduleType() != null) {
      newConnection.withScheduleType(update.getScheduleType());
      newConnection.withManual(update.getManual());
      if (update.getScheduleData() != null) {
        newConnection.withScheduleData(Jsons.clone(update.getScheduleData()));
      }
    } else if (update.getSchedule() != null) {
      final Schedule newSchedule = new Schedule()
          .withTimeUnit(update.getSchedule().getTimeUnit())
          .withUnits(update.getSchedule().getUnits());
      newConnection.withManual(false).withSchedule(newSchedule);
      // Also write into the new field. This one will be consumed if populated.
      newConnection
          .withScheduleType(ScheduleType.BASIC_SCHEDULE);
      newConnection.withScheduleData(new ScheduleData().withBasicSchedule(
          new BasicSchedule().withTimeUnit(convertTimeUnitSchema(update.getSchedule().getTimeUnit()))
              .withUnits(update.getSchedule().getUnits())));
    } else {
      newConnection.withManual(true).withSchedule(null);
      newConnection.withScheduleType(ScheduleType.MANUAL).withScheduleData(null);
    }

    return newConnection;
  }

  /**
   * Validate all resources are from the same workspace.
   *
   * @param workspaceHelper workspace helper
   * @param sourceId source id
   * @param destinationId destination id
   * @param operationIds operation ids
   */
  public static void validateWorkspace(final WorkspaceHelper workspaceHelper,
                                       final UUID sourceId,
                                       final UUID destinationId,
                                       final @Nullable List<UUID> operationIds) {
    final UUID sourceWorkspace = workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(sourceId);
    final UUID destinationWorkspace = workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId);

    Preconditions.checkArgument(
        sourceWorkspace.equals(destinationWorkspace),
        String.format(
            "Source and destination do not belong to the same workspace. "
                + "Source id: %s, "
                + "Source workspace id: %s, "
                + "Destination id: %s, "
                + "Destination workspace id: %s",
            sourceId,
            sourceWorkspace,
            destinationId,
            destinationWorkspace));

    if (operationIds != null) {
      for (final UUID operationId : operationIds) {
        final UUID operationWorkspace = workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId);
        Preconditions.checkArgument(
            sourceWorkspace.equals(operationWorkspace),
            String.format(
                "Operation and connection do not belong to the same workspace. Workspace id: %s, Operation id: %s, Operation workspace id: %s",
                sourceWorkspace,
                operationId,
                operationWorkspace));
      }
    }
  }

  // Helper method to convert between TimeUnit enums for old and new schedule schemas.
  private static BasicSchedule.TimeUnit convertTimeUnitSchema(final Schedule.TimeUnit timeUnit) {
    switch (timeUnit) {
      case MINUTES:
        return BasicSchedule.TimeUnit.MINUTES;
      case HOURS:
        return BasicSchedule.TimeUnit.HOURS;
      case DAYS:
        return BasicSchedule.TimeUnit.DAYS;
      case WEEKS:
        return BasicSchedule.TimeUnit.WEEKS;
      case MONTHS:
        return BasicSchedule.TimeUnit.MONTHS;
      default:
        throw new RuntimeException("Unhandled TimeUnitEnum: " + timeUnit);
    }
  }

  public static void validateCatalogDoesntContainDuplicateStreamNames(final AirbyteCatalog syncCatalog) {
    final Set<StreamName> streamNames = new HashSet<>();
    for (final AirbyteStreamAndConfiguration s : syncCatalog.getStreams()) {
      final AirbyteStream stream = s.getStream();
      if (!streamNames.add(new StreamName(stream.getNamespace(), stream.getName()))) {
        throw new IllegalArgumentException(String.format("Catalog contains duplicate stream names: %s.%s", stream.getNamespace(), stream.getName()));
      }
    }
  }

  public record StreamName(String namespace, String name) {

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final StreamName that = (StreamName) o;
      return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      return result;
    }

  }

}
