/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule

fun publicApiRuleNames(
  controller: Class<*>,
  cloud: Boolean,
): Set<String> {
  val rules =
    controller.methods
      .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java)?.rule?.java }
      .filter { it != OperationRule::class.java }
      .map { it.name }
      .toSet()
  return if (cloud && PublicSourceOAuthRule::class.java.name in rules) rules + "io.airbyte.server.wrapped.auth.PublicCloudSourceOAuthRule" else rules
}

data class PublicApiHttpScenario(
  val method: String,
  val path: String,
  val body: String = "",
)

/** Explicit requests for the published public surface, independent of the rules selected by the router. */
fun publicApiHttpScenarios(): Map<Class<*>, List<PublicApiHttpScenario>> {
  val id = "00000000-0000-0000-0000-000000000101"
  val destination = "00000000-0000-0000-0000-000000000104"
  val workspace = "00000000-0000-0000-0000-000000000102"
  val organization = "00000000-0000-0000-0000-000000000103"
  val api = "/api/public/v1"
  val oauth = """{"actorType":"source","name":"postgres","configuration":{}}"""
  val definition = """{"name":"fixture","dockerRepository":"airbyte/source-fixture","dockerImageTag":"1.0.0"}"""
  val controllers = mutableMapOf<Class<*>, List<PublicApiHttpScenario>>()

  fun family(
    name: String,
    scenarios: List<PublicApiHttpScenario>,
  ) {
    controllers[Class.forName("io.airbyte.server.apis.publicapi.controllers.${name}Controller")] = scenarios
  }

  fun request(
    method: String,
    path: String,
    body: String = "",
  ) = PublicApiHttpScenario(method, "$api$path", body)

  fun saved(
    path: String,
    update: String = "PATCH",
    body: String = "{}",
  ) = listOf(request("GET", path), request("DELETE", path), request(update, path, body))

  family(
    "Sources",
    saved("/sources/$id") + request("PUT", "/sources/$id", """{"name":"fixture","configuration":{}}""") +
      request("GET", "/sources?workspaceIds=$workspace") +
      request("POST", "/sources", """{"name":"fixture","workspaceId":"$workspace","definitionId":"$id","configuration":{}}""") +
      request("POST", "/sources/initiateOAuth", """{"workspaceId":"$workspace","redirectUrl":"https://example.com","sourceType":"postgres"}"""),
  )
  family(
    "Destinations",
    saved("/destinations/$id") + request("PUT", "/destinations/$id", """{"name":"fixture","configuration":{}}""") +
      request("GET", "/destinations?workspaceIds=$workspace") +
      request("POST", "/destinations", """{"name":"fixture","workspaceId":"$workspace","definitionId":"$id","configuration":{}}"""),
  )
  family(
    "Connections",
    saved("/connections/$id") + request("GET", "/connections?workspaceIds=$workspace") +
      request("POST", "/connections", """{"sourceId":"$id","destinationId":"$destination"}"""),
  )
  family("Streams", listOf(request("GET", "/streams?sourceId=$id&destinationId=$destination")))
  family(
    "Jobs",
    listOf(
      request("GET", "/jobs?connectionId=$id"),
      request("GET", "/jobs/1"),
      request("DELETE", "/jobs/1"),
      request("POST", "/jobs", """{"connectionId":"$id","jobType":"sync"}"""),
    ),
  )
  family(
    "Workspaces",
    saved("/workspaces/$workspace") + request("GET", "/workspaces?workspaceIds=$workspace") +
      request("POST", "/workspaces", """{"name":"fixture","organizationId":"$organization"}""") +
      request("PUT", "/workspaces/$workspace/oauthCredentials", oauth) +
      request("DELETE", "/workspaces/$workspace/oauthCredentials/source/postgres"),
  )
  family(
    "Definitions",
    listOf("sources", "destinations", "declarative_sources").flatMap { kind ->
      val path = "/workspaces/$workspace/definitions/$kind"
      saved(
        "$path/$id",
        "PUT",
        if (kind ==
          "declarative_sources"
        ) {
          """{"manifest":{"spec":{"connection_specification":{}}}}"""
        } else {
          """{"name":"fixture","dockerImageTag":"1.0.0"}"""
        },
      ) +
        request("GET", path) +
        request(
          "POST",
          path,
          if (kind ==
            "declarative_sources"
          ) {
            """{"name":"fixture","manifest":{"spec":{"connection_specification":{}}}}"""
          } else {
            definition
          },
        )
    },
  )
  family("ConnectorDefinitions", listOf(request("GET", "/connector_definitions?workspaceId=$workspace&type=source")))
  family(
    "Tags",
    saved("/tags/$id", body = """{"name":"fixture","color":"#123456"}""") + request("GET", "/tags?workspaceIds=$workspace") +
      request("POST", "/tags", """{"name":"fixture","color":"#123456","workspaceId":"$workspace"}"""),
  )
  family(
    "Organizations",
    listOf(
      request("GET", "/organizations"),
      request("PUT", "/organizations/$organization/oauthCredentials", oauth),
      request("DELETE", "/organizations/$organization/oauthCredentials/source/postgres"),
    ),
  )
  family("Users", listOf(request("GET", "/users?organizationId=$organization")))
  family(
    "Permissions",
    saved("/permissions/$id", body = """{"permissionType":"workspace_reader"}""") + request("GET", "/permissions") +
      request("POST", "/permissions", """{"permissionType":"workspace_reader","userId":"$id","workspaceId":"$workspace"}"""),
  )
  family(
    "Groups",
    saved("/groups/$id") + request("GET", "/groups?organizationId=$organization") +
      request("POST", "/groups", """{"name":"fixture","organizationId":"$organization"}"""),
  )
  family(
    "GroupMembers",
    listOf(
      request("GET", "/groups/$id/members"),
      request("POST", "/groups/$id/members", """{"userId":"$id"}"""),
      request("DELETE", "/groups/$id/members/$id"),
    ),
  )
  family(
    "GroupPermissions",
    listOf(
      request("GET", "/groups/$id/permissions"),
      request("POST", "/groups/$id/permissions", """{"permissionType":"workspace_reader","workspaceId":"$workspace"}"""),
      request("DELETE", "/groups/$id/permissions/$id"),
    ),
  )
  family(
    "Region",
    saved("/regions/$id") + request("GET", "/regions?organizationId=$organization") +
      request("POST", "/regions", """{"name":"fixture","organizationId":"$organization"}"""),
  )
  family(
    "Dataplane",
    saved("/dataplanes/$id") + request("GET", "/dataplanes?regionIds=$id") +
      request("POST", "/dataplanes", """{"name":"fixture","regionId":"$id"}"""),
  )
  family(
    "Applications",
    listOf(
      request("GET", "/applications"),
      request("POST", "/applications", """{"name":"fixture"}"""),
      request("GET", "/applications/$id"),
      request("DELETE", "/applications/$id"),
    ),
  )
  family(
    "Embedded",
    listOf(
      request("GET", "/embedded/organizations"),
      request("POST", "/embedded/scoped_token", """{"workspaceId":"$workspace"}"""),
      request(
        "POST",
        "/embedded/widget_token",
        """{"organizationId":"$organization","allowedOrigin":"https://example.com","externalUserId":"fixture"}""",
      ),
    ),
  )
  return controllers
}
