/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler;
import io.airbyte.commons.server.handlers.AttemptHandler;
import io.airbyte.commons.server.handlers.ConnectionsHandler;
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler;
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler;
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler;
import io.airbyte.commons.server.handlers.DestinationHandler;
import io.airbyte.commons.server.handlers.HealthCheckHandler;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.LogsHandler;
import io.airbyte.commons.server.handlers.NotificationsHandler;
import io.airbyte.commons.server.handlers.OAuthHandler;
import io.airbyte.commons.server.handlers.OpenApiConfigHandler;
import io.airbyte.commons.server.handlers.OperationsHandler;
import io.airbyte.commons.server.handlers.OrganizationsHandler;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler;
import io.airbyte.commons.server.handlers.SourceHandler;
import io.airbyte.commons.server.handlers.StateHandler;
import io.airbyte.commons.server.handlers.UserHandler;
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler;
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler;
import io.airbyte.commons.server.handlers.WebBackendGeographiesHandler;
import io.airbyte.commons.server.handlers.WorkspacesHandler;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.db.Database;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.security.utils.SecurityService;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.jooq.DSLContext;
import org.mockito.Mockito;

/**
 * This is the base class for the test of the controllers. This allows to test that:
 * <ul>
 * <li>The path defined at the moment of writing the test exists,</li>
 * <li>The return code is the expected one. It could have help to catch that during the migration to
 * micronaut, some endpoint return value switch from a 204 NO_CONTENT return code to a 200 OK which
 * was a regression,</li>
 * <li>It allow to test that the exception thrown by the handler are properly catch by the exception
 * handlers and then return an expected HTTP return code,</li>
 * <li>It could help with testing the authorization by injecting a user and workspace in the header
 * and check that the authorization is properly applied.</li>
 * </ul>
 */
@MicronautTest
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
abstract class BaseControllerTest {

  ActorDefinitionVersionHandler actorDefinitionVersionHandler = Mockito.mock(ActorDefinitionVersionHandler.class);

  @MockBean(ActorDefinitionVersionHandler.class)
  @Replaces(ActorDefinitionVersionHandler.class)
  ActorDefinitionVersionHandler mmActorDefinitionVersionHandler() {
    return actorDefinitionVersionHandler;
  }

  AttemptHandler attemptHandler = Mockito.mock(AttemptHandler.class);

  @MockBean(AttemptHandler.class)
  @Replaces(AttemptHandler.class)
  AttemptHandler mmAttemptHandler() {
    return attemptHandler;
  }

  ConnectionsHandler connectionsHandler = Mockito.mock(ConnectionsHandler.class);

  @MockBean(ConnectionsHandler.class)
  @Replaces(ConnectionsHandler.class)
  ConnectionsHandler mmConnectionsHandler() {
    return connectionsHandler;
  }

  UserHandler userHandler = Mockito.mock(UserHandler.class);

  @MockBean(UserHandler.class)
  @Replaces(UserHandler.class)
  UserHandler mmUserHandler() {
    return userHandler;
  }

  PermissionHandler permissionHandler = Mockito.mock(PermissionHandler.class);

  @MockBean(PermissionHandler.class)
  @Replaces(PermissionHandler.class)
  PermissionHandler mmPermissionHandler() {
    return permissionHandler;
  }

  DestinationHandler destinationHandler = Mockito.mock(DestinationHandler.class);

  @MockBean(DestinationHandler.class)
  @Replaces(DestinationHandler.class)
  DestinationHandler mmDestinationHandler() {
    return destinationHandler;
  }

  DestinationDefinitionsHandler destinationDefinitionsHandler = Mockito.mock(DestinationDefinitionsHandler.class);

  @MockBean(DestinationDefinitionsHandler.class)
  @Replaces(DestinationDefinitionsHandler.class)
  DestinationDefinitionsHandler mmDestinationDefinitionsHandler() {
    return destinationDefinitionsHandler;
  }

  HealthCheckHandler healthCheckHandler = Mockito.mock(HealthCheckHandler.class);

  @MockBean(HealthCheckHandler.class)
  @Replaces(HealthCheckHandler.class)
  HealthCheckHandler mmHealthCheckHandler() {
    return healthCheckHandler;
  }

  JobHistoryHandler jobHistoryHandler = Mockito.mock(JobHistoryHandler.class);

  @MockBean(JobHistoryHandler.class)
  @Replaces(JobHistoryHandler.class)
  JobHistoryHandler mmJobHistoryHandler() {
    return jobHistoryHandler;
  }

  LogsHandler logsHandler = Mockito.mock(LogsHandler.class);

  @MockBean(LogsHandler.class)
  @Replaces(LogsHandler.class)
  LogsHandler mmLogsHandler() {
    return logsHandler;
  }

  NotificationsHandler notificationsHandler = Mockito.mock(NotificationsHandler.class);

  @MockBean(NotificationsHandler.class)
  @Replaces(NotificationsHandler.class)
  NotificationsHandler mmNotificationsHandler() {
    return notificationsHandler;
  }

  OAuthHandler oAuthHandler = Mockito.mock(OAuthHandler.class);

  @MockBean(OAuthHandler.class)
  @Replaces(OAuthHandler.class)
  OAuthHandler mmOAuthHandler() {
    return oAuthHandler;
  }

  OpenApiConfigHandler openApiConfigHandler = Mockito.mock(OpenApiConfigHandler.class);

  @MockBean(OpenApiConfigHandler.class)
  @Replaces(OpenApiConfigHandler.class)
  OpenApiConfigHandler mmOpenApiConfigHandler() {
    return openApiConfigHandler;
  }

  OperationsHandler operationsHandler = Mockito.mock(OperationsHandler.class);

  @MockBean(OperationsHandler.class)
  @Replaces(OperationsHandler.class)
  OperationsHandler mmOperationsHandler() {
    return operationsHandler;
  }

  SchedulerHandler schedulerHandler = Mockito.mock(SchedulerHandler.class);

  @MockBean(SchedulerHandler.class)
  @Replaces(SchedulerHandler.class)
  SchedulerHandler mmSchedulerHandler() {
    return schedulerHandler;
  }

  ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler = Mockito.mock(ConnectorDefinitionSpecificationHandler.class);

  @MockBean(ConnectorDefinitionSpecificationHandler.class)
  @Replaces(ConnectorDefinitionSpecificationHandler.class)

  ConnectorDefinitionSpecificationHandler mmConnectorDefinitionSpecificationHandler() {
    return connectorDefinitionSpecificationHandler;
  }

  SourceDefinitionsHandler sourceDefinitionsHandler = Mockito.mock(SourceDefinitionsHandler.class);

  @MockBean(SourceDefinitionsHandler.class)
  @Replaces(SourceDefinitionsHandler.class)
  SourceDefinitionsHandler mmSourceDefinitionsHandler() {
    return sourceDefinitionsHandler;
  }

  ActorDefinitionAccessValidator actorDefinitionAccessValidator = Mockito.mock(ActorDefinitionAccessValidator.class);

  @MockBean(ActorDefinitionAccessValidator.class)
  @Replaces(ActorDefinitionAccessValidator.class)
  ActorDefinitionAccessValidator mmActorDefinitionAccessValidator() {
    return actorDefinitionAccessValidator;
  }

  SourceHandler sourceHandler = Mockito.mock(SourceHandler.class);

  @MockBean(SourceHandler.class)
  @Replaces(SourceHandler.class)
  SourceHandler mmSourceHandler() {
    return sourceHandler;
  }

  StateHandler stateHandler = Mockito.mock(StateHandler.class);

  @MockBean(StateHandler.class)
  @Replaces(StateHandler.class)
  StateHandler mmStateHandler() {
    return stateHandler;
  }

  WebBackendConnectionsHandler webBackendConnectionsHandler = Mockito.mock(WebBackendConnectionsHandler.class);

  @MockBean(WebBackendConnectionsHandler.class)
  @Replaces(WebBackendConnectionsHandler.class)
  WebBackendConnectionsHandler mmWebBackendConnectionsHandler() {
    return webBackendConnectionsHandler;
  }

  WebBackendGeographiesHandler webBackendGeographiesHandler = Mockito.mock(WebBackendGeographiesHandler.class);

  @MockBean(WebBackendGeographiesHandler.class)
  @Replaces(WebBackendGeographiesHandler.class)
  WebBackendGeographiesHandler mmWebBackendGeographiesHandler() {
    return webBackendGeographiesHandler;
  }

  WebBackendCheckUpdatesHandler webBackendCheckUpdatesHandler = Mockito.mock(WebBackendCheckUpdatesHandler.class);

  @MockBean(WebBackendCheckUpdatesHandler.class)
  @Replaces(WebBackendCheckUpdatesHandler.class)
  WebBackendCheckUpdatesHandler mmWebBackendCheckUpdatesHandler() {
    return webBackendCheckUpdatesHandler;
  }

  WorkspacesHandler workspacesHandler = Mockito.mock(WorkspacesHandler.class);

  @MockBean(WorkspacesHandler.class)
  @Replaces(WorkspacesHandler.class)
  WorkspacesHandler mmWorkspacesHandler() {
    return workspacesHandler;
  }

  OrganizationsHandler organizationsHandler = Mockito.mock(OrganizationsHandler.class);

  @MockBean(OrganizationsHandler.class)
  @Replaces(OrganizationsHandler.class)
  OrganizationsHandler mmOrganizationsHandler() {
    return organizationsHandler;
  }

  DeploymentMetadataHandler deploymentMetadataHandler = Mockito.mock(DeploymentMetadataHandler.class);

  @MockBean(DeploymentMetadataHandler.class)
  @Replaces(DeploymentMetadataHandler.class)
  DeploymentMetadataHandler mmDeploymentMetadataHandler() {
    return deploymentMetadataHandler;
  }

  @MockBean(SynchronousSchedulerClient.class)
  @Replaces(SynchronousSchedulerClient.class)
  SynchronousSchedulerClient mmSynchronousSchedulerClient() {
    return Mockito.mock(SynchronousSchedulerClient.class);
  }

  @MockBean(Database.class)
  @Replaces(Database.class)
  @Named("configDatabase")
  Database mmDatabase() {
    return Mockito.mock(Database.class);
  }

  @MockBean(TrackingClient.class)
  @Replaces(TrackingClient.class)
  TrackingClient mmTrackingClient() {
    return Mockito.mock(TrackingClient.class);
  }

  @MockBean(WorkflowClient.class)
  @Replaces(WorkflowClient.class)
  WorkflowClient mmWorkflowClient() {
    return Mockito.mock(WorkflowClient.class);
  }

  @MockBean(WorkflowServiceStubs.class)
  @Replaces(WorkflowServiceStubs.class)
  WorkflowServiceStubs mmWorkflowServiceStubs() {
    return Mockito.mock(WorkflowServiceStubs.class);
  }

  @MockBean(TemporalClient.class)
  @Replaces(TemporalClient.class)
  TemporalClient mmTemporalClient() {
    return Mockito.mock(TemporalClient.class);
  }

  @MockBean(SecurityService.class)
  @Replaces(SecurityService.class)
  SecurityService mmSecurityService() {
    return Mockito.mock(SecurityService.class);
  }

  @MockBean(JobNotifier.class)
  @Replaces(JobNotifier.class)
  JobNotifier mmJobNotifier() {
    return Mockito.mock(JobNotifier.class);
  }

  @MockBean(JobTracker.class)
  @Replaces(JobTracker.class)
  JobTracker mmJobTracker() {
    return Mockito.mock(JobTracker.class);
  }

  @Replaces(DSLContext.class)
  @Named("config")
  DSLContext mmDSLContext() {
    return Mockito.mock(DSLContext.class);
  }

  @Inject
  HealthApiController healthApiController;

  @Inject
  EmbeddedServer embeddedServer;

  @Inject
  @Client("/")
  HttpClient client;

  void testEndpointStatus(final HttpRequest request, final HttpStatus expectedStatus) {
    assertEquals(expectedStatus, client.toBlocking().exchange(request).getStatus());
  }

  void testErrorEndpointStatus(final HttpRequest request, final HttpStatus expectedStatus) {
    Assertions.assertThatThrownBy(() -> client.toBlocking().exchange(request))
        .isInstanceOf(HttpClientResponseException.class)
        .asInstanceOf(new InstanceOfAssertFactory(HttpClientResponseException.class, Assertions::assertThat))
        .has(new Condition<HttpClientResponseException>(exception -> exception.getStatus() == expectedStatus,
            "Http status to be %s", expectedStatus));
  }

}
