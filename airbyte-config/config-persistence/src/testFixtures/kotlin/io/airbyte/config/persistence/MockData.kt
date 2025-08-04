/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.version.Version
import io.airbyte.config.ActiveDeclarativeManifest
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalog.CatalogType
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Notification
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.Schedule
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.SsoConfig
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncState
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.config.SupportLevel
import io.airbyte.config.User
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.config.WorkspaceServiceAccount
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.Field
import io.airbyte.protocol.models.v0.SyncMode
import java.net.URI
import java.time.Instant
import java.time.OffsetDateTime
import java.util.Arrays
import java.util.List
import java.util.Map
import java.util.Objects
import java.util.TreeMap
import java.util.UUID
import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors

object MockData {
  val WORKSPACE_ID_1: UUID = UUID.randomUUID()
  val WORKSPACE_ID_2: UUID = UUID.randomUUID()
  private val WORKSPACE_ID_3: UUID = UUID.randomUUID()
  private val WORKSPACE_CUSTOMER_ID: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_ID_1: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_ID_2: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_ID_3: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_ID_4: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_VERSION_ID_1: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_VERSION_ID_2: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_VERSION_ID_3: UUID = UUID.randomUUID()
  private val SOURCE_DEFINITION_VERSION_ID_4: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_ID_1: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_ID_2: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_ID_3: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_ID_4: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_VERSION_ID_1: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_VERSION_ID_2: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_VERSION_ID_3: UUID = UUID.randomUUID()
  private val DESTINATION_DEFINITION_VERSION_ID_4: UUID = UUID.randomUUID()
  val SOURCE_ID_1: UUID = UUID.randomUUID()
  val SOURCE_ID_2: UUID = UUID.randomUUID()
  private val SOURCE_ID_3: UUID = UUID.randomUUID()
  val DESTINATION_ID_1: UUID = UUID.randomUUID()
  val DESTINATION_ID_2: UUID = UUID.randomUUID()
  val DESTINATION_ID_3: UUID = UUID.randomUUID()
  private val OPERATION_ID_1: UUID = UUID.randomUUID()
  private val OPERATION_ID_2: UUID = UUID.randomUUID()
  private val OPERATION_ID_3: UUID = UUID.randomUUID()
  private val CONNECTION_ID_1: UUID = UUID.randomUUID()
  private val CONNECTION_ID_2: UUID = UUID.randomUUID()
  private val CONNECTION_ID_3: UUID = UUID.randomUUID()
  private val CONNECTION_ID_4: UUID = UUID.randomUUID()
  private val CONNECTION_ID_5: UUID = UUID.randomUUID()
  private val CONNECTION_ID_6: UUID = UUID.randomUUID()
  private val SOURCE_OAUTH_PARAMETER_ID_1: UUID = UUID.randomUUID()
  private val SOURCE_OAUTH_PARAMETER_ID_2: UUID = UUID.randomUUID()
  private val SOURCE_OAUTH_PARAMETER_ID_3: UUID = UUID.randomUUID()
  private val SOURCE_OAUTH_PARAMETER_ID_4: UUID = UUID.randomUUID()
  private val DESTINATION_OAUTH_PARAMETER_ID_1: UUID = UUID.randomUUID()
  private val DESTINATION_OAUTH_PARAMETER_ID_2: UUID = UUID.randomUUID()
  private val DESTINATION_OAUTH_PARAMETER_ID_3: UUID = UUID.randomUUID()
  private val DESTINATION_OAUTH_PARAMETER_ID_4: UUID = UUID.randomUUID()
  val ACTOR_CATALOG_ID_1: UUID = UUID.randomUUID()
  private val ACTOR_CATALOG_ID_2: UUID = UUID.randomUUID()
  val ACTOR_CATALOG_ID_3: UUID = UUID.randomUUID()
  private val ACTOR_CATALOG_FETCH_EVENT_ID_1: UUID = UUID.randomUUID()
  private val ACTOR_CATALOG_FETCH_EVENT_ID_2: UUID = UUID.randomUUID()
  private val ACTOR_CATALOG_FETCH_EVENT_ID_3: UUID = UUID.randomUUID()
  const val DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES: Long = 3600
  val DATAPLANE_GROUP_ID_DEFAULT: UUID = UUID.randomUUID()
  val DATAPLANE_GROUP_ID_ORG_1: UUID = UUID.randomUUID()
  val DATAPLANE_GROUP_ID_ORG_2: UUID = UUID.randomUUID()
  val DATAPLANE_GROUP_ID_ORG_3: UUID = UUID.randomUUID()

  // User
  val CREATOR_USER_ID_1: UUID = UUID.randomUUID()
  val CREATOR_USER_ID_2: UUID = UUID.randomUUID()
  val CREATOR_USER_ID_3: UUID = UUID.randomUUID()
  val CREATOR_USER_ID_4: UUID = UUID.randomUUID()
  val CREATOR_USER_ID_5: UUID = UUID.randomUUID()
  val DUP_EMAIL_USER_ID_1: UUID = UUID.randomUUID()
  val DUP_EMAIL_USER_ID_2: UUID = UUID.randomUUID()
  const val DUP_EMAIL: String = "dup-email@airbyte.io"
  const val EMAIL_1: String = "user-1@whatever.com"
  const val EMAIL_2: String = "user-2@whatever.com"

  // Permission
  val PERMISSION_ID_1: UUID = UUID.randomUUID()
  val PERMISSION_ID_2: UUID = UUID.randomUUID()
  val PERMISSION_ID_3: UUID = UUID.randomUUID()
  val PERMISSION_ID_4: UUID = UUID.randomUUID()

  val PERMISSION_ID_5: UUID = UUID.randomUUID()
  val PERMISSION_ID_6: UUID = UUID.randomUUID()
  val PERMISSION_ID_7: UUID = UUID.randomUUID()
  val PERMISSION_ID_8: UUID = UUID.randomUUID()

  val ORGANIZATION_ID_1: UUID = UUID.randomUUID()
  val ORGANIZATION_ID_2: UUID = UUID.randomUUID()
  val ORGANIZATION_ID_3: UUID = UUID.randomUUID()
  val DEFAULT_ORGANIZATION_ID: UUID = ORGANIZATION_ID_1

  val SSO_CONFIG_ID_1: UUID = UUID.randomUUID()
  val SSO_CONFIG_ID_2: UUID = UUID.randomUUID()

  val MOCK_SERVICE_ACCOUNT_1: String = (
    "{\n" +
      "  \"type\" : \"service_account\",\n" +
      "  \"project_id\" : \"random-gcp-project\",\n" +
      "  \"private_key_id\" : \"123a1234ab1a123ab12345678a1234ab1abc1a12\",\n" +
      "  \"private_key\" : \"-----BEGIN RSA PRIVATE KEY-----\\n" +
      "MIIEoQIBAAKCAQBtkKBs9oe9pFhEWjBls9OrY0PXE/QN6nL4Bfw4+UqcBpTyItXo\\n" +
      "3aBXuVqDIZ377zjbJUcYuc4NzAsLImy7VVT1XrdAkkCKQEMoA9pQgONA/3kD8Xff\\n" +
      "SUGfdup8UJg925paaRhM7u81e3XKGwGyL/qcxpuHtfqimeWWfSPy5AawyOFl+l25\\n" +
      "OqbmPK4/QVqk4pcorQuISUkrehY0Ji0gVQF+ZeBvg7lvBtjNEl//eysGtcZvk7X\\n" +
      "Hqg+EIBqRjVNDsViHj0xeoDFcFgXDeWzxeQ0c7gMsDthfm4SjgaVFdQwsJUeoC6X\\n" +
      "lwUoBbFIVVKW0n+SH+kxLc7mhaGjyRYJLS6tAgMBAAECggEAaowetlf4IR/VBoN+\\n" +
      "VSjPSvg5XMr2pyG7tB597RngyGJOLjpaMx5zc1u4/ZSPghRdAh/6R71I+HnYs3dC\\n" +
      "rdqJyCPXqV+Qi+F6bUtx3p+4X9kQ4hjMLcOboWuPFF1774vDSvCwxQAGd8gb//LL\\n" +
      "b3DhEdzCGvOJTN7EOdhwQSAmsXsfj0qKlmm8vv0HBQDvjYYWhy/UcPry5sAGQ8KU\\n" +
      "nUPTkz/OMS56nBIgKXgZtGRTP1Q7Q9a6oLmlvbDxuKGUByUPNlveZplzyWDO3RUN\\n" +
      "NPt9dwgGk6rZK0umunGr0lq+WOK33Ue1RJy2VIvvV6dt32x20ehfVKND8N8q+wJ3\\n" +
      "eJQggQKBgQC//dOX8RwkmIloRzzmbu+qY8o44/F5gtxj8maR+OJhvbpFEID49bBr\\n" +
      "zYqcMKfcgHJr6638CXVGSO66IiKtQcTMJ/Vd8TQVPcNPI1h/RD+wT/nkWX6R/0YH\\n" +
      "jwwNmikeUDH2/hLQlRZ8O45hc4frDGRMeHn3MSS2YsBDSl6YL/zHpQKBgQCSF9Ka\\n" +
      "yCZmw5eS63G5/X9SVXbLRPuc6Fus+IbRPttOzSRviUXHaBjwwVEJgIKODx/eVXgD\\n" +
      "A/OvFUmwIn73uZD/XgJrhkwAendaa+yhWKAkO5pO/EdAslxRmgxqTXfRcyslKBbo\\n" +
      "s4YAgeYUgzOaMH4UxY4pJ7H6BLsFlboL+8BcaQKBgDSCM1Cm/M91eH8wnJNZW+r6\\n" +
      "B+CvVueoxqX/MdZSf3fD8CHbdaqhZ3LUcEhvdjl0V9b0Sk1YON7UK5Z0p49DIZPE\\n" +
      "ifL7eQcmMTh/rkCAZfrOpMWzRE6hxoFiuiUuOHi17jRjILozTEcF8tbsRgwfA392\\n" +
      "o8Tbh/Lp5zOAL4bn+PaRAoGAZ2AgEJJsSe9BRB8CPF+aRoJfKvrHKIJqzHyXuVzH\\n" +
      "Bn22uI3kKHQKoeHJG/Ypa6hcHpFP+KJFPrDLkaz3NwfCCFFXWQqQoQ4Hgp43tPvn\\n" +
      "ZXwfdqChMrCDDuL4wgfLLxRVhVdWzpapzZYdXopwazzBGqWoMIr8LzRFum/2VCBy\\n" +
      "P3ECgYBGqjuYud6gtrzaQwmMfcA0pSYsii96d2LKwWzjgcMzLxge59PIWXeQJqOb\\n" +
      "h97m3qCkkPzbceD6Id8m/EyrNb04V8Zr0ERlcK/a4nRSHoIWQZY01lDSGhneRKn1\\n" +
      "ncBvRqCfz6ajf+zBg3zK0af98IHL0FI2NsNJLPrOBFMcthjx/g==\\n-----END RSA PRIVATE KEY-----\",\n" +
      "  \"client_email\" : \"a1e5ac98-7531-48e1-943b-b46636@random-gcp-project.abc.abcdefghijklmno.com\",\n" +
      "  \"client_id\" : \"123456789012345678901\",\n" +
      "  \"auth_uri\" : \"https://blah.blah.com/x/blah1/blah\",\n" +
      "  \"token_uri\" : \"https://blah.blah.com/blah\",\n" +
      "  \"auth_provider_x509_cert_url\" : \"https://www.blah.com/blah/v1/blah\",\n" +
      "  \"client_x509_cert_url\" : \"https://www.blah.com/blah/v1/blah/a123/" +
      "a1e5ac98-7531-48e1-943b-b46636%40random-gcp-project.abc.abcdefghijklmno.com\"\n" +
      "}"
  )

  val HMAC_SECRET_PAYLOAD_1: JsonNode =
    jsonNode<MutableMap<String?, String?>?>(
      sortMap(
        Map.of<String?, String?>(
          "access_id",
          "ABCD1A1ABCDEFG1ABCDEFGH1ABC12ABCDEF1ABCDE1ABCDE1ABCDE12ABCDEF",
          "secret",
          "AB1AbcDEF//ABCDeFGHijKlmNOpqR1ABC1aBCDeF",
        ),
      ),
    )
  val HMAC_SECRET_PAYLOAD_2: JsonNode =
    jsonNode<MutableMap<String?, String?>?>(
      sortMap(
        Map.of<String?, String?>(
          "access_id",
          "ABCD1A1ABCDEFG1ABCDEFGH1ABC12ABCDEF1ABCDE1ABCDE1ABCDE12ABCDEX",
          "secret",
          "AB1AbcDEF//ABCDeFGHijKlmNOpqR1ABC1aBCDeX",
        ),
      ),
    )

  private val NOW: Instant = Instant.parse("2021-12-15T20:30:40.00Z")

  private const val CONNECTION_SPECIFICATION = "{\"name\":\"John\", \"age\":30, \"car\":null}"
  private val OPERATION_ID_4: UUID = UUID.randomUUID()
  private val WEBHOOK_CONFIG_ID: UUID = UUID.randomUUID()
  private const val WEBHOOK_OPERATION_EXECUTION_URL = "test-webhook-url"
  private const val WEBHOOK_OPERATION_EXECUTION_BODY = "test-webhook-body"
  const val CONFIG_HASH: String = "1394"
  const val CONNECTOR_VERSION: String = "1.2.0"
  val permission1: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_1)
      .withUserId(CREATOR_USER_ID_1)
      .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)

  val permission2: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_2)
      .withUserId(CREATOR_USER_ID_2)
      .withWorkspaceId(WORKSPACE_ID_2)
      .withPermissionType(Permission.PermissionType.WORKSPACE_OWNER)

  val permission3: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_3)
      .withUserId(CREATOR_USER_ID_3)
      .withWorkspaceId(WORKSPACE_ID_1)
      .withPermissionType(Permission.PermissionType.WORKSPACE_OWNER)

  val permission4: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_4)
      .withUserId(CREATOR_USER_ID_1)
      .withWorkspaceId(WORKSPACE_ID_1)
      .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

  val permission5: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_5)
      .withUserId(CREATOR_USER_ID_4)
      .withOrganizationId(ORGANIZATION_ID_1)
      .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

  val permission6: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_6)
      .withUserId(CREATOR_USER_ID_5)
      .withWorkspaceId(WORKSPACE_ID_2)
      .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

  val permission7: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_7)
      .withUserId(CREATOR_USER_ID_5)
      .withOrganizationId(ORGANIZATION_ID_2)
      .withPermissionType(Permission.PermissionType.ORGANIZATION_READER)

  val permission8: Permission =
    Permission()
      .withPermissionId(PERMISSION_ID_8)
      .withUserId(CREATOR_USER_ID_1)
      .withOrganizationId(DEFAULT_ORGANIZATION_ID)
      .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)

  fun users(): MutableList<AuthenticatedUser?> {
    val user1 =
      AuthenticatedUser()
        .withUserId(CREATOR_USER_ID_1)
        .withName("user-1")
        .withAuthUserId(CREATOR_USER_ID_1.toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withDefaultWorkspaceId(WORKSPACE_ID_1)
        .withStatus(User.Status.DISABLED)
        .withCompanyName("company-1")
        .withEmail(EMAIL_1)
        .withNews(true)
        .withUiMetadata(null)

    val user2 =
      AuthenticatedUser()
        .withUserId(CREATOR_USER_ID_2)
        .withName("user-2")
        .withAuthUserId(CREATOR_USER_ID_2.toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withDefaultWorkspaceId(WORKSPACE_ID_2)
        .withStatus(User.Status.INVITED)
        .withCompanyName("company-2")
        .withEmail(EMAIL_2)
        .withNews(false)
        .withUiMetadata(null)

    val user3 =
      AuthenticatedUser()
        .withUserId(CREATOR_USER_ID_3)
        .withName("user-3")
        .withAuthUserId(CREATOR_USER_ID_3.toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withDefaultWorkspaceId(null)
        .withStatus(User.Status.REGISTERED)
        .withCompanyName("company-3")
        .withEmail("user-3@whatever.com")
        .withNews(true)
        .withUiMetadata(null)

    val user4 =
      AuthenticatedUser()
        .withUserId(CREATOR_USER_ID_4)
        .withName("user-4")
        .withAuthUserId(CREATOR_USER_ID_4.toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withDefaultWorkspaceId(null)
        .withStatus(User.Status.REGISTERED)
        .withCompanyName("company-4")
        .withEmail("user-4@whatever.com")
        .withNews(true)
        .withUiMetadata(null)

    val user5 =
      AuthenticatedUser()
        .withUserId(CREATOR_USER_ID_5)
        .withName("user-5")
        .withAuthUserId(CREATOR_USER_ID_5.toString())
        .withAuthProvider(AuthProvider.KEYCLOAK)
        .withDefaultWorkspaceId(null)
        .withStatus(User.Status.REGISTERED)
        .withCompanyName("company-5")
        .withEmail("user-5@whatever.com")
        .withNews(true)
        .withUiMetadata(null)

    return Arrays.asList<AuthenticatedUser?>(user1, user2, user3, user4, user5)
  }

  fun permissions(): MutableList<Permission?> =
    Arrays.asList<Permission?>(permission1, permission2, permission3, permission4, permission5, permission6, permission7, permission8)

  fun organizations(): MutableList<Organization?> {
    val organization1 =
      Organization().withOrganizationId(ORGANIZATION_ID_1).withName("organization-1").withEmail("email@email.com")
    val organization2 =
      Organization().withOrganizationId(ORGANIZATION_ID_2).withName("organization-2").withEmail("email2@email.com")
    val organization3 =
      Organization().withOrganizationId(ORGANIZATION_ID_3).withName("organization-3").withEmail("emai3l@email.com")
    return Arrays.asList<Organization?>(organization1, organization2, organization3)
  }

  fun ssoConfigs(): MutableList<SsoConfig?> {
    val ssoConfig1 =
      SsoConfig()
        .withSsoConfigId(SSO_CONFIG_ID_1)
        .withOrganizationId(ORGANIZATION_ID_1)
        .withKeycloakRealm("realm-1")
    val ssoConfig2 =
      SsoConfig()
        .withSsoConfigId(SSO_CONFIG_ID_2)
        .withOrganizationId(ORGANIZATION_ID_2)
        .withKeycloakRealm("realm-2")
    return Arrays.asList<SsoConfig?>(ssoConfig1, ssoConfig2)
  }

  fun standardWorkspaces(): MutableList<StandardWorkspace?> {
    val notification =
      Notification()
        .withNotificationType(Notification.NotificationType.SLACK)
        .withSendOnFailure(true)
        .withSendOnSuccess(true)
        .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("webhook-url"))

    val workspace1 =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID_1)
        .withCustomerId(WORKSPACE_CUSTOMER_ID)
        .withName("test-workspace")
        .withSlug("random-string")
        .withEmail("abc@xyz.com")
        .withInitialSetupComplete(true)
        .withAnonymousDataCollection(true)
        .withNews(true)
        .withSecurityUpdates(true)
        .withDisplaySetupWizard(true)
        .withTombstone(false)
        .withNotifications(mutableListOf<Notification?>(notification))
        .withFirstCompletedSync(true)
        .withFeedbackDone(true)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_DEFAULT)
        .withWebhookOperationConfigs(
          jsonNode<WebhookOperationConfigs?>(
            WebhookOperationConfigs().withWebhookConfigs(List.of<WebhookConfig?>(WebhookConfig().withId(WEBHOOK_CONFIG_ID).withName("name"))),
          ),
        ).withOrganizationId(DEFAULT_ORGANIZATION_ID)

    val workspace2 =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID_2)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_DEFAULT)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)

    val workspace3 =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID_3)
        .withName("Tombstoned")
        .withSlug("tombstoned")
        .withInitialSetupComplete(true)
        .withTombstone(true)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_DEFAULT)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)

    return Arrays.asList<StandardWorkspace?>(workspace1, workspace2, workspace3)
  }

  fun publicSourceDefinition(): StandardSourceDefinition? =
    StandardSourceDefinition()
      .withSourceDefinitionId(SOURCE_DEFINITION_ID_1)
      .withDefaultVersionId(SOURCE_DEFINITION_VERSION_ID_1)
      .withSourceType(StandardSourceDefinition.SourceType.API)
      .withName("random-source-1")
      .withIcon("icon-1")
      .withTombstone(false)
      .withPublic(true)
      .withCustom(false)
      .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))
      .withMaxSecondsBetweenMessages(DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)

  fun grantableSourceDefinition1(): StandardSourceDefinition? =
    StandardSourceDefinition()
      .withSourceDefinitionId(SOURCE_DEFINITION_ID_2)
      .withDefaultVersionId(SOURCE_DEFINITION_VERSION_ID_2)
      .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
      .withName("random-source-2")
      .withIcon("icon-2")
      .withTombstone(false)
      .withPublic(false)
      .withCustom(false)
      .withMaxSecondsBetweenMessages(DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)

  fun grantableSourceDefinition2(): StandardSourceDefinition? =
    StandardSourceDefinition()
      .withSourceDefinitionId(SOURCE_DEFINITION_ID_3)
      .withDefaultVersionId(SOURCE_DEFINITION_VERSION_ID_3)
      .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
      .withName("random-source-3")
      .withIcon("icon-3")
      .withTombstone(false)
      .withPublic(false)
      .withCustom(false)
      .withMaxSecondsBetweenMessages(DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)

  fun customSourceDefinition(): StandardSourceDefinition? =
    StandardSourceDefinition()
      .withSourceDefinitionId(SOURCE_DEFINITION_ID_4)
      .withDefaultVersionId(SOURCE_DEFINITION_VERSION_ID_4)
      .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
      .withName("random-source-4")
      .withIcon("icon-4")
      .withTombstone(false)
      .withPublic(false)
      .withCustom(true)
      .withMaxSecondsBetweenMessages(DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES)

  fun actorDefinitionVersion(): ActorDefinitionVersion? =
    ActorDefinitionVersion()
      .withDockerImageTag("0.0.1")
      .withDockerRepository("repository-4")
      .withSpec(connectorSpecification())
      .withSupportLevel(SupportLevel.COMMUNITY)
      .withInternalSupportLevel(100L)
      .withProtocolVersion("0.2.0")

  fun actorDefinitionBreakingChange(version: String): ActorDefinitionBreakingChange? =
    ActorDefinitionBreakingChange()
      .withVersion(Version(version))
      .withMessage("This is a breaking change for version " + version)
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#" + version)
      .withUpgradeDeadline("2020-01-01")

  fun standardSourceDefinitions(): MutableList<StandardSourceDefinition?> =
    Arrays.asList<StandardSourceDefinition?>(
      publicSourceDefinition(),
      grantableSourceDefinition1(),
      grantableSourceDefinition2(),
      customSourceDefinition(),
    )

  fun connectorSpecification(): ConnectorSpecification? =
    ConnectorSpecification()
      .withConnectionSpecification(jsonNode<String?>(CONNECTION_SPECIFICATION))
      .withDocumentationUrl(URI.create("whatever"))
      .withAdvancedAuth(AdvancedAuth().withAuthFlowType(AdvancedAuth.AuthFlowType.OAUTH_2_0))
      .withChangelogUrl(URI.create("whatever"))
      .withSupportedDestinationSyncModes(
        Arrays.asList<DestinationSyncMode?>(
          DestinationSyncMode.APPEND,
          DestinationSyncMode.OVERWRITE,
          DestinationSyncMode.APPEND_DEDUP,
        ),
      ).withSupportsDBT(true)
      .withSupportsIncremental(true)
      .withSupportsNormalization(true)

  fun publicDestinationDefinition(): StandardDestinationDefinition? =
    StandardDestinationDefinition()
      .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_1)
      .withDefaultVersionId(DESTINATION_DEFINITION_VERSION_ID_1)
      .withName("random-destination-1")
      .withIcon("icon-3")
      .withTombstone(false)
      .withPublic(true)
      .withCustom(false)
      .withResourceRequirements(ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2")))

  fun grantableDestinationDefinition1(): StandardDestinationDefinition? =
    StandardDestinationDefinition()
      .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_2)
      .withDefaultVersionId(DESTINATION_DEFINITION_VERSION_ID_2)
      .withName("random-destination-2")
      .withIcon("icon-4")
      .withTombstone(false)
      .withPublic(false)
      .withCustom(false)

  fun grantableDestinationDefinition2(): StandardDestinationDefinition? =
    StandardDestinationDefinition()
      .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_3)
      .withDefaultVersionId(DESTINATION_DEFINITION_VERSION_ID_3)
      .withName("random-destination-3")
      .withIcon("icon-3")
      .withTombstone(false)
      .withPublic(false)
      .withCustom(false)

  fun customDestinationDefinition(): StandardDestinationDefinition? =
    StandardDestinationDefinition()
      .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_4)
      .withDefaultVersionId(DESTINATION_DEFINITION_VERSION_ID_4)
      .withName("random-destination-4")
      .withIcon("icon-4")
      .withTombstone(false)
      .withPublic(false)
      .withCustom(true)

  fun standardDestinationDefinitions(): MutableList<StandardDestinationDefinition?> =
    Arrays.asList<StandardDestinationDefinition?>(
      publicDestinationDefinition(),
      grantableDestinationDefinition1(),
      grantableDestinationDefinition2(),
      customDestinationDefinition(),
    )

  fun sourceConnections(): MutableList<SourceConnection?> {
    val sourceConnection1 =
      SourceConnection()
        .withName("source-1")
        .withTombstone(false)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_1)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withConfiguration(deserialize(CONNECTION_SPECIFICATION))
        .withSourceId(SOURCE_ID_1)
    val sourceConnection2 =
      SourceConnection()
        .withName("source-2")
        .withTombstone(false)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_2)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withConfiguration(deserialize(CONNECTION_SPECIFICATION))
        .withSourceId(SOURCE_ID_2)
    val sourceConnection3 =
      SourceConnection()
        .withName("source-3")
        .withTombstone(false)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_1)
        .withWorkspaceId(WORKSPACE_ID_2)
        .withConfiguration(emptyObject())
        .withSourceId(SOURCE_ID_3)
    return Arrays.asList<SourceConnection?>(sourceConnection1, sourceConnection2, sourceConnection3)
  }

  fun destinationConnections(): MutableList<DestinationConnection?> {
    val destinationConnection1 =
      DestinationConnection()
        .withName("destination-1")
        .withTombstone(false)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_1)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withConfiguration(deserialize(CONNECTION_SPECIFICATION))
        .withDestinationId(DESTINATION_ID_1)
    val destinationConnection2 =
      DestinationConnection()
        .withName("destination-2")
        .withTombstone(false)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_2)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withConfiguration(deserialize(CONNECTION_SPECIFICATION))
        .withDestinationId(DESTINATION_ID_2)
    val destinationConnection3 =
      DestinationConnection()
        .withName("destination-3")
        .withTombstone(true)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_2)
        .withWorkspaceId(WORKSPACE_ID_2)
        .withConfiguration(emptyObject())
        .withDestinationId(DESTINATION_ID_3)
    return Arrays.asList<DestinationConnection?>(destinationConnection1, destinationConnection2, destinationConnection3)
  }

  fun sourceOauthParameters(): MutableList<SourceOAuthParameter?> {
    val sourceOAuthParameter1 =
      SourceOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withWorkspaceId(WORKSPACE_ID_1)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_1)
        .withOauthParameterId(SOURCE_OAUTH_PARAMETER_ID_1)
    val sourceOAuthParameter2 =
      SourceOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withWorkspaceId(WORKSPACE_ID_1)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_2)
        .withOauthParameterId(SOURCE_OAUTH_PARAMETER_ID_2)
    val sourceOAuthParameter3 =
      SourceOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withOrganizationId(ORGANIZATION_ID_1)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_3)
        .withOauthParameterId(SOURCE_OAUTH_PARAMETER_ID_3)
    val sourceOAuthParameter4 =
      SourceOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withSourceDefinitionId(SOURCE_DEFINITION_ID_4)
        .withOauthParameterId(SOURCE_OAUTH_PARAMETER_ID_4)
    return Arrays.asList<SourceOAuthParameter?>(sourceOAuthParameter1, sourceOAuthParameter2, sourceOAuthParameter3, sourceOAuthParameter4)
  }

  fun destinationOauthParameters(): MutableList<DestinationOAuthParameter?> {
    val destinationOAuthParameter1 =
      DestinationOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withWorkspaceId(WORKSPACE_ID_1)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_1)
        .withOauthParameterId(DESTINATION_OAUTH_PARAMETER_ID_1)
    val destinationOAuthParameter2 =
      DestinationOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withWorkspaceId(WORKSPACE_ID_1)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_2)
        .withOauthParameterId(DESTINATION_OAUTH_PARAMETER_ID_2)
    val destinationOAuthParameter3 =
      DestinationOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withOrganizationId(ORGANIZATION_ID_1)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_3)
        .withOauthParameterId(DESTINATION_OAUTH_PARAMETER_ID_3)
    val destinationOAuthParameter4 =
      DestinationOAuthParameter()
        .withConfiguration(jsonNode<String?>(CONNECTION_SPECIFICATION))
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID_4)
        .withOauthParameterId(DESTINATION_OAUTH_PARAMETER_ID_4)
    return Arrays.asList<DestinationOAuthParameter?>(
      destinationOAuthParameter1,
      destinationOAuthParameter2,
      destinationOAuthParameter3,
      destinationOAuthParameter4,
    )
  }

  fun standardSyncOperations(): MutableList<StandardSyncOperation?> {
    val standardSyncOperation1 =
      StandardSyncOperation()
        .withName("operation-1")
        .withTombstone(false)
        .withOperationId(OPERATION_ID_1)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
            .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY),
        )
    val standardSyncOperation2 =
      StandardSyncOperation()
        .withName("operation-1")
        .withTombstone(false)
        .withOperationId(OPERATION_ID_2)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
            .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY),
        )
    val standardSyncOperation3 =
      StandardSyncOperation()
        .withName("operation-3")
        .withTombstone(false)
        .withOperationId(OPERATION_ID_3)
        .withWorkspaceId(WORKSPACE_ID_2)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
            .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY),
        )
    val standardSyncOperation4 =
      StandardSyncOperation()
        .withName("webhook-operation")
        .withTombstone(false)
        .withOperationId(OPERATION_ID_4)
        .withWorkspaceId(WORKSPACE_ID_1)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
            .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY),
        )
    return Arrays.asList<StandardSyncOperation?>(standardSyncOperation1, standardSyncOperation2, standardSyncOperation3, standardSyncOperation4)
  }

  fun standardSyncs(): MutableList<StandardSync?> {
    val resourceRequirements =
      ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("1")
        .withMemoryRequest("1")
        .withMemoryLimit("1")
    val schedule = Schedule().withTimeUnit(Schedule.TimeUnit.DAYS).withUnits(1L)
    val standardSync1 =
      StandardSync()
        .withOperationIds(Arrays.asList<UUID?>(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_1)
        .withSourceId(SOURCE_ID_1)
        .withDestinationId(DESTINATION_ID_1)
        .withCatalog(configuredCatalog)
        .withFieldSelectionData(FieldSelectionData().withAdditionalProperty("foo", true))
        .withName("standard-sync-1")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)

    val standardSync2 =
      StandardSync()
        .withOperationIds(Arrays.asList<UUID?>(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_2)
        .withSourceId(SOURCE_ID_1)
        .withDestinationId(DESTINATION_ID_2)
        .withCatalog(configuredCatalog)
        .withName("standard-sync-2")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)

    val standardSync3 =
      StandardSync()
        .withOperationIds(Arrays.asList<UUID?>(OPERATION_ID_1, OPERATION_ID_2))
        .withConnectionId(CONNECTION_ID_3)
        .withSourceId(SOURCE_ID_2)
        .withDestinationId(DESTINATION_ID_1)
        .withCatalog(configuredCatalog)
        .withName("standard-sync-3")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.DESTINATION)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)

    val standardSync4 =
      StandardSync()
        .withOperationIds(mutableListOf<UUID?>())
        .withConnectionId(CONNECTION_ID_4)
        .withSourceId(SOURCE_ID_2)
        .withDestinationId(DESTINATION_ID_2)
        .withCatalog(configuredCatalog)
        .withName("standard-sync-4")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.DEPRECATED)
        .withSchedule(schedule)
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)

    val standardSync5 =
      StandardSync()
        .withOperationIds(List.of<UUID?>(OPERATION_ID_3))
        .withConnectionId(CONNECTION_ID_5)
        .withSourceId(SOURCE_ID_3)
        .withDestinationId(DESTINATION_ID_3)
        .withCatalog(configuredCatalog)
        .withName("standard-sync-5")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.ACTIVE)
        .withSchedule(schedule)
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)

    val standardSync6 =
      StandardSync()
        .withOperationIds(mutableListOf<UUID?>())
        .withConnectionId(CONNECTION_ID_6)
        .withSourceId(SOURCE_ID_3)
        .withDestinationId(DESTINATION_ID_3)
        .withCatalog(configuredCatalog)
        .withName("standard-sync-6")
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withResourceRequirements(resourceRequirements)
        .withStatus(StandardSync.Status.DEPRECATED)
        .withSchedule(schedule)
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)

    return Arrays.asList<StandardSync?>(standardSync1, standardSync2, standardSync3, standardSync4, standardSync5, standardSync6)
  }

  private val configuredCatalog: ConfiguredAirbyteCatalog?
    get() {
      val catalog =
        AirbyteCatalog().withStreams(
          List.of<AirbyteStream?>(
            CatalogHelpers
              .createAirbyteStream(
                "models",
                "models_schema",
                Field.of("id", JsonSchemaType.NUMBER),
                Field.of("make_id", JsonSchemaType.NUMBER),
                Field.of("model", JsonSchemaType.STRING),
              ).withSupportedSyncModes(
                Lists.newArrayList<SyncMode?>(
                  SyncMode.FULL_REFRESH,
                  SyncMode.INCREMENTAL,
                ),
              ).withSourceDefinedPrimaryKey(
                List.of<MutableList<String?>?>(
                  mutableListOf<String?>(
                    "id",
                  ),
                ),
              ),
          ),
        )
      return convertToInternal(CatalogHelpers.toDefaultConfiguredCatalog(catalog))
    }

  private fun convertToInternal(catalog: io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog?): ConfiguredAirbyteCatalog? =
    Jsons.convertValue(catalog, ConfiguredAirbyteCatalog::class.java)

  fun standardSyncStates(): MutableList<StandardSyncState?> {
    val standardSyncState1 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_1)
        .withState(State().withState(jsonNode<String?>(CONNECTION_SPECIFICATION)))
    val standardSyncState2 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_2)
        .withState(State().withState(jsonNode<String?>(CONNECTION_SPECIFICATION)))
    val standardSyncState3 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_3)
        .withState(State().withState(jsonNode<String?>(CONNECTION_SPECIFICATION)))
    val standardSyncState4 =
      StandardSyncState()
        .withConnectionId(CONNECTION_ID_4)
        .withState(State().withState(jsonNode<String?>(CONNECTION_SPECIFICATION)))
    return Arrays.asList<StandardSyncState?>(standardSyncState1, standardSyncState2, standardSyncState3, standardSyncState4)
  }

  fun actorCatalogs(): MutableList<ActorCatalog?> {
    val actorCatalog1 =
      ActorCatalog()
        .withId(ACTOR_CATALOG_ID_1)
        .withCatalog(deserialize("{}"))
        .withCatalogType(CatalogType.SOURCE_CATALOG)
        .withCatalogHash("TESTHASH")
    val actorCatalog2 =
      ActorCatalog()
        .withId(ACTOR_CATALOG_ID_2)
        .withCatalog(deserialize("{}"))
        .withCatalogType(CatalogType.SOURCE_CATALOG)
        .withCatalogHash("12345")
    val actorCatalog3 =
      ActorCatalog()
        .withId(ACTOR_CATALOG_ID_3)
        .withCatalog(deserialize("{}"))
        .withCatalogType(CatalogType.SOURCE_CATALOG)
        .withCatalogHash("SomeOtherHash")
    return Arrays.asList<ActorCatalog?>(actorCatalog1, actorCatalog2, actorCatalog3)
  }

  fun actorCatalogFetchEvents(): MutableList<ActorCatalogFetchEvent?> {
    val actorCatalogFetchEvent1 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_1)
        .withActorCatalogId(ACTOR_CATALOG_ID_1)
        .withActorId(SOURCE_ID_1)
        .withConfigHash("CONFIG_HASH")
        .withConnectorVersion("1.0.0")
    val actorCatalogFetchEvent2 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_2)
        .withActorCatalogId(ACTOR_CATALOG_ID_2)
        .withActorId(SOURCE_ID_2)
        .withConfigHash("1395")
        .withConnectorVersion("1.42.0")
    return Arrays.asList<ActorCatalogFetchEvent?>(actorCatalogFetchEvent1, actorCatalogFetchEvent2)
  }

  fun actorCatalogFetchEventsSameSource(): MutableList<ActorCatalogFetchEvent?> {
    val actorCatalogFetchEvent1 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_1)
        .withActorCatalogId(ACTOR_CATALOG_ID_1)
        .withActorId(SOURCE_ID_1)
        .withConfigHash("CONFIG_HASH")
        .withConnectorVersion("1.0.0")
    val actorCatalogFetchEvent2 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_2)
        .withActorCatalogId(ACTOR_CATALOG_ID_2)
        .withActorId(SOURCE_ID_1)
        .withConfigHash(CONFIG_HASH)
        .withConnectorVersion(CONNECTOR_VERSION)
    return Arrays.asList<ActorCatalogFetchEvent?>(actorCatalogFetchEvent1, actorCatalogFetchEvent2)
  }

  fun defaultOrganization(): Organization =
    Organization()
      .withOrganizationId(DEFAULT_ORGANIZATION_ID)
      .withName("default org")
      .withEmail("test@test.com")

  fun actorCatalogFetchEventsForAggregationTest(): MutableList<ActorCatalogFetchEventWithCreationDate?> {
    val now = OffsetDateTime.now()
    val yesterday = OffsetDateTime.now().minusDays(1L)

    val actorCatalogFetchEvent1 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_1)
        .withActorCatalogId(ACTOR_CATALOG_ID_1)
        .withActorId(SOURCE_ID_1)
        .withConfigHash("CONFIG_HASH")
        .withConnectorVersion("1.0.0")
    val actorCatalogFetchEvent2 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_2)
        .withActorCatalogId(ACTOR_CATALOG_ID_2)
        .withActorId(SOURCE_ID_2)
        .withConfigHash(CONFIG_HASH)
        .withConnectorVersion(CONNECTOR_VERSION)
    val actorCatalogFetchEvent3 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_3)
        .withActorCatalogId(ACTOR_CATALOG_ID_3)
        .withActorId(SOURCE_ID_2)
        .withConfigHash(CONFIG_HASH)
        .withConnectorVersion(CONNECTOR_VERSION)
    val actorCatalogFetchEvent4 =
      ActorCatalogFetchEvent()
        .withId(ACTOR_CATALOG_FETCH_EVENT_ID_3)
        .withActorCatalogId(ACTOR_CATALOG_ID_3)
        .withActorId(SOURCE_ID_3)
        .withConfigHash(CONFIG_HASH)
        .withConnectorVersion(CONNECTOR_VERSION)
    return Arrays.asList<ActorCatalogFetchEventWithCreationDate?>(
      ActorCatalogFetchEventWithCreationDate(actorCatalogFetchEvent1, now),
      ActorCatalogFetchEventWithCreationDate(actorCatalogFetchEvent2, yesterday),
      ActorCatalogFetchEventWithCreationDate(actorCatalogFetchEvent3, now),
      ActorCatalogFetchEventWithCreationDate(actorCatalogFetchEvent4, now),
    )
  }

  fun workspaceServiceAccounts(): MutableList<WorkspaceServiceAccount?> {
    val workspaceServiceAccount =
      WorkspaceServiceAccount()
        .withWorkspaceId(WORKSPACE_ID_1)
        .withHmacKey(HMAC_SECRET_PAYLOAD_1)
        .withServiceAccountId("a1e5ac98-7531-48e1-943b-b46636")
        .withServiceAccountEmail("a1e5ac98-7531-48e1-943b-b46636@random-gcp-project.abc.abcdefghijklmno.com")
        .withJsonCredential(deserialize(MOCK_SERVICE_ACCOUNT_1))

    return mutableListOf<WorkspaceServiceAccount?>(workspaceServiceAccount)
  }

  fun declarativeManifest(): DeclarativeManifest? {
    try {
      return DeclarativeManifest()
        .withActorDefinitionId(UUID.randomUUID())
        .withVersion(0L)
        .withDescription("a description")
        .withManifest(ObjectMapper().readTree("{\"manifest\": \"manifest\"}"))
        .withSpec(ObjectMapper().readTree("{\"spec\": \"spec\"}"))
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  fun actorDefinitionConfigInjection(): ActorDefinitionConfigInjection? {
    try {
      return ActorDefinitionConfigInjection()
        .withActorDefinitionId(UUID.randomUUID())
        .withJsonToInject(ObjectMapper().readTree("{\"json_to_inject\": \"a json value\"}"))
        .withInjectionPath("an_injection_path")
    } catch (e: JsonProcessingException) {
      throw RuntimeException(e)
    }
  }

  fun activeDeclarativeManifest(): ActiveDeclarativeManifest? = ActiveDeclarativeManifest().withActorDefinitionId(UUID.randomUUID()).withVersion(1L)

  private fun sortMap(originalMap: MutableMap<String?, String?>): MutableMap<String?, String?> =
    originalMap.entries
      .stream()
      .collect(
        Collectors.toMap(
          Function { entry -> entry.key },
          Function { entry -> entry.value },
          BinaryOperator { oldValue: String?, newValue: String? -> newValue },
          Supplier { TreeMap<String?, String?>() },
        ),
      )

  fun now(): Instant = NOW

  class ActorCatalogFetchEventWithCreationDate(
    val actorCatalogFetchEvent: ActorCatalogFetchEvent?,
    val createdAt: OffsetDateTime?,
  ) {
    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as ActorCatalogFetchEventWithCreationDate
      return actorCatalogFetchEvent == that.actorCatalogFetchEvent && createdAt == that.createdAt
    }

    override fun hashCode(): Int = Objects.hash(actorCatalogFetchEvent, createdAt)

    override fun toString(): String =
      (
        "ActorCatalogFetchEventWithCreationDate{" +
          "actorCatalogFetchEvent=" + actorCatalogFetchEvent +
          ", createdAt=" + createdAt +
          '}'
      )
  }
}
