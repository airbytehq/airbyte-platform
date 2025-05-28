/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.constants

const val SOURCE_TYPE = "sourceType"
const val DESTINATION_TYPE = "destinationType"

const val API_PATH = "/api"
const val ROOT_PATH = "/public"
const val APPLICATIONS_PATH = "$ROOT_PATH/v1/applications"
const val APPLICATIONS_PATH_WITH_ID = "$ROOT_PATH/v1/applications/{applicationId}"
const val CONNECTIONS_PATH = "$ROOT_PATH/v1/connections"
const val CONNECTIONS_WITH_ID_PATH = "$CONNECTIONS_PATH/{connectionId}"
const val CONNECTOR_DEFINITIONS_PATH = "$ROOT_PATH/v1/connector_definitions"
const val STREAMS_PATH = "$ROOT_PATH/v1/streams"
const val JOBS_PATH = "$ROOT_PATH/v1/jobs"
const val JOBS_WITH_ID_PATH = "$JOBS_PATH/{jobId}"
const val SOURCES_PATH = "$ROOT_PATH/v1/sources"
const val INITIATE_OAUTH_PATH = "$SOURCES_PATH/initiateOAuth"
const val SOURCES_WITH_ID_PATH = "$SOURCES_PATH/{sourceId}"
const val DESTINATIONS_PATH = "$ROOT_PATH/v1/destinations"
const val DESTINATIONS_WITH_ID_PATH = "$DESTINATIONS_PATH/{destinationId}"
const val OAUTH_PATH = "$ROOT_PATH/v1/oauth"
const val REGIONS_PATH = "$ROOT_PATH/v1/regions"
const val REGIONS_WITH_ID_PATH = "$REGIONS_PATH/{regionId}"
const val DATAPLANES_PATH = "$ROOT_PATH/v1/dataplanes"
const val DATAPLANES_WITH_ID_PATH = "$DATAPLANES_PATH/{dataplaneId}"
const val WORKSPACES_PATH = "$ROOT_PATH/v1/workspaces"
const val WORKSPACES_WITH_ID_PATH = "$WORKSPACES_PATH/{workspaceId}"
const val WORKSPACES_WITH_ID_AND_OAUTH_PATH = "$WORKSPACES_WITH_ID_PATH/oauth_credentials"
const val PERMISSIONS_PATH = "$ROOT_PATH/v1/permissions"
const val PERMISSIONS_WITH_ID_PATH = "$PERMISSIONS_PATH/{permissionId}"
const val USERS_PATH = "$ROOT_PATH/v1/users"
const val ORGANIZATIONS_PATH = "$ROOT_PATH/v1/organizations"
const val TAGS_PATH = "$ROOT_PATH/v1/tags"

val POST = io.micronaut.http.HttpMethod.POST.name
val GET = io.micronaut.http.HttpMethod.GET.name
val PATCH = io.micronaut.http.HttpMethod.PATCH.name
val DELETE = io.micronaut.http.HttpMethod.DELETE.name
val PUT = io.micronaut.http.HttpMethod.PUT.name

const val WORKSPACE_IDS = "workspaceIds"
const val JOB_TYPE = "jobType"
const val INCLUDE_DELETED = "includeDeleted"

const val OAUTH_CALLBACK_PATH = "$ROOT_PATH/v1/oauth/callback"

const val MESSAGE = "message"

const val HTTP_RESPONSE_BODY_DEBUG_MESSAGE = "HttpResponse body: "
