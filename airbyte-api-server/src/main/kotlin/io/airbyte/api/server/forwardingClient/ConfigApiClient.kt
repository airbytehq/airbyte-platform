/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.forwardingClient

import io.airbyte.api.client.model.generated.CompleteOAuthResponse
import io.airbyte.api.client.model.generated.CompleteSourceOauthRequest
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionReadList
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.DestinationCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.DestinationReadList
import io.airbyte.api.client.model.generated.DestinationUpdate
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobInfoRead
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.JobListRequestBody
import io.airbyte.api.client.model.generated.JobReadList
import io.airbyte.api.client.model.generated.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.OAuthConsentRead
import io.airbyte.api.client.model.generated.PartialDestinationUpdate
import io.airbyte.api.client.model.generated.PartialSourceUpdate
import io.airbyte.api.client.model.generated.SourceCreate
import io.airbyte.api.client.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.SourceOauthConsentRequest
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.SourceReadList
import io.airbyte.api.client.model.generated.SourceUpdate
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceOverrideOauthParamsRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.api.client.model.generated.WorkspaceReadList
import io.airbyte.api.client.model.generated.WorkspaceUpdate
import io.airbyte.api.server.constants.ANALYTICS_HEADER
import io.airbyte.api.server.constants.ANALYTICS_HEADER_VALUE
import io.airbyte.api.server.constants.AUTH_HEADER
import io.airbyte.api.server.constants.ENDPOINT_API_USER_INFO_HEADER
import io.airbyte.api.server.constants.INTERNAL_API_HOST
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Header
import io.micronaut.http.annotation.Post
import io.micronaut.http.client.annotation.Client

/**
 * The ConfigApiClient is a micronaut http client used to hit the internal airbyte config server.
 *
 * The X-Endpoint-API-UserInfo which populates endpointUserInfo is not used in OSS.
 * In OSS the value can always be assumed to be null.
 *
 * Worth noting that status codes > 400 will throw an HttpClientResponseException EXCEPT 404s which
 * will just return an HttpResponse with statusCode 404
 * https://docs.micronaut.io/latest/guide/index.html#clientError
 */
@Client(INTERNAL_API_HOST)
@Header(name = HttpHeaders.USER_AGENT, value = "Micronaut HTTP Client")
@Header(name = HttpHeaders.ACCEPT, value = MediaType.APPLICATION_JSON)
@Header(name = ANALYTICS_HEADER, value = ANALYTICS_HEADER_VALUE)
interface ConfigApiClient {
  // Connections
  @Post(value = "/api/v1/connections/create", processes = [MediaType.APPLICATION_JSON])
  fun createConnection(
    @Body connectionId: ConnectionCreate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<String>

  @Post(value = "/api/v1/connections/delete", processes = [MediaType.APPLICATION_JSON])
  fun deleteConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<String>

  @Post(value = "/api/v1/connections/sync", processes = [MediaType.APPLICATION_JSON])
  fun sync(
    @Body connectionId: ConnectionIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<JobInfoRead>

  @Post(value = "/api/v1/connections/reset", processes = [MediaType.APPLICATION_JSON])
  fun reset(
    @Body connectionId: ConnectionIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<JobInfoRead>

  @Post(value = "/api/v1/connections/get", processes = [MediaType.APPLICATION_JSON])
  fun getConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<ConnectionRead>

  @Post(value = "/api/v1/connections/update", processes = [MediaType.APPLICATION_JSON])
  fun updateConnection(
    @Body connectionUpdate: ConnectionUpdate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<String>

  // OAuth
  @Post(value = "/api/v1/source_oauths/get_consent_url", processes = [MediaType.APPLICATION_JSON])
  fun getSourceConsentUrl(
    @Body consentRequest: SourceOauthConsentRequest,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<OAuthConsentRead>

  @Post(value = "/api/v1/source_oauths/complete_oauth", processes = [MediaType.APPLICATION_JSON])
  fun completeSourceOAuth(
    @Body completeSourceOauthRequest: CompleteSourceOauthRequest,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<CompleteOAuthResponse>

  // Sources
  @Post(value = "/api/v1/sources/create", processes = [MediaType.APPLICATION_JSON])
  fun createSource(
    @Body sourceCreate: SourceCreate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceRead>

  @Post(value = "/api/v1/sources/delete", processes = [MediaType.APPLICATION_JSON])
  fun deleteSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<String>

  @Post(value = "/api/v1/sources/get", processes = [MediaType.APPLICATION_JSON])
  fun getSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceRead>

  @Post(value = "/api/v1/sources/partial_update", processes = [MediaType.APPLICATION_JSON])
  fun partialUpdateSource(
    @Body partialSourceUpdate: PartialSourceUpdate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceRead>

  @Post(value = "/api/v1/sources/update", processes = [MediaType.APPLICATION_JSON])
  fun updateSource(
    @Body sourceUpdate: SourceUpdate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceRead>

  @Post(value = "/api/v1/sources/discover_schema", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun getSourceSchema(
    @Body sourceId: SourceDiscoverSchemaRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceDiscoverSchemaRead>

  @Post(
    value = "/api/v1/source_definition_specifications/get",
    processes = [MediaType.APPLICATION_JSON],
    produces = [MediaType.APPLICATION_JSON],
  )
  fun getSourceDefinitionSpecification(
    @Body sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceDefinitionSpecificationRead>

  // Destinations
  @Post(value = "/api/v1/destinations/create", processes = [MediaType.APPLICATION_JSON])
  fun createDestination(
    @Body destinationCreate: DestinationCreate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<DestinationRead>

  @Post(value = "/api/v1/destinations/get", processes = [MediaType.APPLICATION_JSON])
  fun getDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<DestinationRead>

  @Post(value = "/api/v1/destinations/update", processes = [MediaType.APPLICATION_JSON])
  fun updateDestination(
    @Body destinationUpdate: DestinationUpdate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<DestinationRead>

  @Post(value = "/api/v1/destinations/partial_update", processes = [MediaType.APPLICATION_JSON])
  fun partialUpdateDestination(
    @Body partialDestinationUpdate: PartialDestinationUpdate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<DestinationRead>

  @Post(value = "/api/v1/destinations/delete", processes = [MediaType.APPLICATION_JSON])
  fun deleteDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<String>

  @Post(value = "/api/v1/destination_definition_specifications/get", processes = [MediaType.APPLICATION_JSON])
  fun getDestinationSpec(
    @Body destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<DestinationDefinitionSpecificationRead>

  // Jobs
  @Post(value = "/api/v1/jobs/get_without_logs", processes = [MediaType.APPLICATION_JSON])
  fun getJobInfoWithoutLogs(
    @Body jobId: JobIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<JobInfoRead>

  @Post(value = "/api/v1/jobs/list", processes = [MediaType.APPLICATION_JSON])
  fun getJobList(
    @Body jobListRequestBody: JobListRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<JobReadList>

  @Post(value = "/api/v1/jobs/list_for_workspaces", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun getJobListForWorkspaces(
    @Body listForWorkspacesRequestBody: JobListForWorkspacesRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) userInfo: String?,
  ): HttpResponse<JobReadList>

  @Post(value = "/api/v1/jobs/cancel", processes = [MediaType.APPLICATION_JSON])
  fun cancelJob(
    @Body jobIdRequestBody: JobIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<JobInfoRead>

  // Workspaces
  @Post(value = "/api/v1/workspaces/get", processes = [MediaType.APPLICATION_JSON])
  fun getWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<WorkspaceRead>

  @Post(value = "/api/v1/workspaces/list", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun listAllWorkspaces(
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<WorkspaceReadList>

  @Post(value = "/api/v1/workspaces/create", processes = [MediaType.APPLICATION_JSON])
  fun createWorkspace(
    @Body workspaceCreate: WorkspaceCreate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<WorkspaceRead>

  @Post(value = "/api/v1/workspaces/update", processes = [MediaType.APPLICATION_JSON])
  fun updateWorkspace(
    @Body workspaceUpdate: WorkspaceUpdate,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<WorkspaceRead>

  @Post(value = "/api/v1/workspaces/delete", processes = [MediaType.APPLICATION_JSON])
  fun deleteWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<Unit>

  @Post(value = "/api/v1/connections/list_paginated", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun listConnectionsForWorkspaces(
    @Body listConnectionsForWorkspacesRequestBody: ListConnectionsForWorkspacesRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<ConnectionReadList>

  @Post(value = "/api/v1/sources/list_paginated", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun listSourcesForWorkspaces(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<SourceReadList>

  @Post(value = "/api/v1/destinations/list_paginated", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun listDestinationsForWorkspaces(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<DestinationReadList>

  @Post(value = "/api/v1/workspaces/list_paginated", processes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  fun listWorkspaces(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<WorkspaceReadList>

  @Post(
    value = "/api/internal/v1/oauth_params/workspace_override/create",
    processes = [MediaType.APPLICATION_JSON],
    produces = [MediaType.APPLICATION_JSON],
  )
  fun setWorkspaceOverrideOAuthParams(
    @Body workspaceOverrideOauthParamsRequestBody: WorkspaceOverrideOauthParamsRequestBody?,
    @Header(AUTH_HEADER) authorization: String?,
    @Header(ENDPOINT_API_USER_INFO_HEADER) endpointUserInfo: String?,
  ): HttpResponse<*>?
}
