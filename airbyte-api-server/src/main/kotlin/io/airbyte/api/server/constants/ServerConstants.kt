/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.constants

const val SOURCE_TYPE = "sourceType"
const val DESTINATION_TYPE = "destinationType"

const val ROOT_PATH = "/"
const val CONNECTIONS_PATH = "/v1/connections"
const val CONNECTIONS_WITH_ID_PATH = "$CONNECTIONS_PATH/{connectionId}"
const val STREAMS_PATH = "/v1/streams"
const val JOBS_PATH = "/v1/jobs"
const val JOBS_WITH_ID_PATH = "$JOBS_PATH/{jobId}"
const val SOURCES_PATH = "/v1/sources"
const val INITIATE_OAUTH_PATH = "$SOURCES_PATH/initiateOAuth"
const val SOURCES_WITH_ID_PATH = "$SOURCES_PATH/{sourceId}"
const val DESTINATIONS_PATH = "/v1/destinations"
const val DESTINATIONS_WITH_ID_PATH = "$DESTINATIONS_PATH/{destinationId}"
const val OAUTH_PATH = "/v1/oauth"
const val WORKSPACES_PATH = "/v1/workspaces"
const val WORKSPACES_WITH_ID_PATH = "$WORKSPACES_PATH/{workspaceId}"
const val WORKSPACES_WITH_ID_AND_OAUTH_PATH = "$WORKSPACES_WITH_ID_PATH/oauth_credentials"

val POST = io.micronaut.http.HttpMethod.POST.name
val GET = io.micronaut.http.HttpMethod.GET.name
val PATCH = io.micronaut.http.HttpMethod.PATCH.name
val DELETE = io.micronaut.http.HttpMethod.DELETE.name
val PUT = io.micronaut.http.HttpMethod.PUT.name

const val WORKSPACE_IDS = "workspaceIds"
const val INCLUDE_DELETED = "includeDeleted"

const val OAUTH_CALLBACK_PATH = "/v1/oauth/callback"

const val MESSAGE = "message"

const val PARTIAL_UPDATE_OAUTH_KEY = "PARTIAL_UPDATE_OAUTH_KEY"

const val HTTP_RESPONSE_BODY_DEBUG_MESSAGE = "HttpResponse body: "
