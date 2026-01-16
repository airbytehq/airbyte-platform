/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_GROUP_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_TYPE_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.config.Dataplane
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.util.UUID

internal class AuthenticationHeaderResolverTest {
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var userPersistence: UserPersistence
  private lateinit var dataplaneGroupService: DataplaneGroupService
  private lateinit var dataplaneService: DataplaneService
  private lateinit var resolver: AuthenticationHeaderResolver

  @BeforeEach
  fun setup() {
    this.workspaceHelper = Mockito.mock(WorkspaceHelper::class.java)
    this.permissionHandler = Mockito.mock(PermissionHandler::class.java)
    this.userPersistence = Mockito.mock(UserPersistence::class.java)
    this.dataplaneGroupService = Mockito.mock(DataplaneGroupService::class.java)
    this.dataplaneService = Mockito.mock(DataplaneService::class.java)
    this.resolver = AuthenticationHeaderResolver(workspaceHelper, permissionHandler, userPersistence, dataplaneGroupService, dataplaneService)
  }

  @Test
  fun testResolvingFromWorkspaceId() {
    val workspaceId = UUID.randomUUID()
    val properties = mapOf(WORKSPACE_ID_HEADER to workspaceId.toString())

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromConnectionId() {
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val properties = mapOf(CONNECTION_ID_HEADER to connectionId.toString())
    Mockito.`when`(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromConnectionIds() {
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val connectionId2 = UUID.randomUUID()

    val properties = mapOf(CONNECTION_IDS_HEADER to serialize(listOf(connectionId.toString(), connectionId2.toString())))
    Mockito.`when`(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenReturn(workspaceId)
    Mockito.`when`(workspaceHelper.getWorkspaceForConnectionId(connectionId2)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId, workspaceId), result)
  }

  @Test
  fun testResolvingFromSourceAndDestinationId() {
    val workspaceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val properties = mapOf(DESTINATION_ID_HEADER to destinationId.toString(), SOURCE_ID_HEADER to sourceId.toString())
    Mockito.`when`(workspaceHelper.getWorkspaceForConnection(sourceId, destinationId)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromDestinationId() {
    val workspaceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val properties = mapOf(DESTINATION_ID_HEADER to destinationId.toString())
    Mockito.`when`(workspaceHelper.getWorkspaceForDestinationId(destinationId)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromJobId() {
    val workspaceId = UUID.randomUUID()
    val jobId = System.currentTimeMillis()
    val properties = mapOf(JOB_ID_HEADER to jobId.toString())
    Mockito.`when`(workspaceHelper.getWorkspaceForJobId(jobId)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromSourceId() {
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val properties = mapOf(SOURCE_ID_HEADER to sourceId.toString())
    Mockito.`when`(workspaceHelper.getWorkspaceForSourceId(sourceId)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromOperationId() {
    val workspaceId = UUID.randomUUID()
    val operationId = UUID.randomUUID()
    val properties = mapOf(OPERATION_ID_HEADER to operationId.toString())
    Mockito.`when`(workspaceHelper.getWorkspaceForOperationId(operationId)).thenReturn(workspaceId)

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingFromNoMatchingProperties() {
    val properties = mapOf<String, String>()
    val workspaceId: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertNull(workspaceId)
  }

  @ParameterizedTest
  @ValueSource(classes = [JsonValidationException::class, NumberFormatException::class, ConfigNotFoundException::class])
  fun testResolvingWithException(exceptionType: Class<Throwable?>) {
    val connectionId = UUID.randomUUID()
    val properties = mapOf(CONNECTION_ID_HEADER to connectionId.toString())
    val exception: Throwable =
      (
        if (exceptionType == ConfigNotFoundException::class.java) {
          ConfigNotFoundException("type", "id")
        } else {
          exceptionType.getDeclaredConstructor(String::class.java).newInstance("test")
        }
      )!!
    Mockito.`when`(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenAnswer { throw exception }

    val workspaceId: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertNull(workspaceId)
  }

  @Test
  fun testResolvingMultiple() {
    val workspaceIds = listOf(UUID.randomUUID(), UUID.randomUUID())
    val properties = mapOf(WORKSPACE_IDS_HEADER to serialize(workspaceIds))

    val resolvedWorkspaceIds: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(workspaceIds, resolvedWorkspaceIds)
  }

  @Test
  fun testResolvingOrganizationDirectlyFromHeader() {
    val organizationId = UUID.randomUUID()
    val properties = mapOf(ORGANIZATION_ID_HEADER to organizationId.toString())

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertEquals(listOf(organizationId), result)
  }

  @Test
  fun testResolvingOrganizationFromWorkspaceHeader() {
    val organizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val properties = mapOf(WORKSPACE_ID_HEADER to workspaceId.toString())
    Mockito.`when`(workspaceHelper.getOrganizationForWorkspace(workspaceId)).thenReturn(organizationId)

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertEquals(listOf(organizationId), result)
  }

  @Test
  fun testResolvingWorkspaceFromPermissionHeader() {
    val workspaceId = UUID.randomUUID()
    val permissionId = UUID.randomUUID()
    val properties = mapOf(PERMISSION_ID_HEADER to permissionId.toString())
    Mockito
      .`when`(permissionHandler.getPermissionRead(PermissionIdRequestBody().permissionId(permissionId)))
      .thenReturn(PermissionRead().workspaceId(workspaceId))

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingOrganizationFromPermissionHeader() {
    val organizationId = UUID.randomUUID()
    val permissionId = UUID.randomUUID()
    val properties = mapOf(PERMISSION_ID_HEADER to permissionId.toString())
    Mockito
      .`when`(permissionHandler.getPermissionRead(PermissionIdRequestBody().permissionId(permissionId)))
      .thenReturn(PermissionRead().organizationId(organizationId))

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertEquals(listOf(organizationId), result)
  }

  @Test
  fun testResolvingAuthUserFromUserId() {
    val userId = UUID.randomUUID()
    val properties = mapOf<String?, String?>(AIRBYTE_USER_ID_HEADER to userId.toString())
    val expectedAuthUserIds = setOf(AUTH_USER_ID, "some-other-id")
    Mockito.`when`(userPersistence.listAuthUserIdsForUser(userId)).thenReturn(expectedAuthUserIds.stream().toList())

    val resolvedAuthUserIds: Set<String>? = resolver.resolveAuthUserIds(properties)

    Assertions.assertEquals(expectedAuthUserIds, resolvedAuthUserIds)
  }

  @Test
  fun testResolvingAuthUserFromCreatorUserId() {
    val userId = UUID.randomUUID()
    val properties = mapOf<String?, String?>(CREATOR_USER_ID_HEADER to userId.toString())
    val expectedAuthUserIds = setOf(AUTH_USER_ID, "some-other-id")
    Mockito.`when`(userPersistence.listAuthUserIdsForUser(userId)).thenReturn(expectedAuthUserIds.stream().toList())

    val resolvedAuthUserIds: Set<String>? = resolver.resolveAuthUserIds(properties)

    Assertions.assertEquals(expectedAuthUserIds, resolvedAuthUserIds)
  }

  @Test
  fun testResolvingAuthUserFromExternalAuthUserId() {
    val properties = mapOf<String?, String?>(EXTERNAL_AUTH_ID_HEADER to AUTH_USER_ID)

    val resolvedAuthUserIds: Set<String>? = resolver.resolveAuthUserIds(properties)

    Assertions.assertEquals(setOf(AUTH_USER_ID), resolvedAuthUserIds)
  }

  @Test
  fun testResolvingWorkspaceIdFromScopeTypeAndScopeId() {
    val workspaceId = UUID.randomUUID()
    val properties = mapOf(SCOPE_TYPE_HEADER to "workspace", SCOPE_ID_HEADER to workspaceId.toString())

    val result: List<UUID>? = resolver.resolveWorkspace(properties)
    Assertions.assertEquals(listOf(workspaceId), result)
  }

  @Test
  fun testResolvingOrganizationIdFromScopeTypeAndScopeId() {
    val organizationId = UUID.randomUUID()
    val properties = mapOf(SCOPE_TYPE_HEADER to "organization", SCOPE_ID_HEADER to organizationId.toString())

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertEquals(listOf(organizationId), result)
  }

  @Test
  fun testResolvingOrganizationFromDataplaneGroupId() {
    val organizationId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val properties = mapOf(DATAPLANE_GROUP_ID_HEADER to dataplaneGroupId.toString())
    Mockito.`when`(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)).thenReturn(organizationId)

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertEquals(listOf(organizationId), result)
  }

  @Test
  fun testResolvingOrganizationFromDataplaneGroupIdWhenNotFound() {
    val dataplaneGroupId = UUID.randomUUID()
    val properties = mapOf(DATAPLANE_GROUP_ID_HEADER to dataplaneGroupId.toString())
    Mockito.`when`(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)).thenReturn(null)

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertNull(result)
  }

  @Test
  fun testResolvingOrganizationFromDataplaneId() {
    val organizationId = UUID.randomUUID()
    val dataplaneId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val mockDataplane =
      Dataplane().apply {
        id = dataplaneId
        this.dataplaneGroupId = dataplaneGroupId
      }
    val properties = mapOf(DATAPLANE_ID_HEADER to dataplaneId.toString())
    Mockito.`when`(dataplaneService.getDataplane(dataplaneId)).thenReturn(mockDataplane)
    Mockito.`when`(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)).thenReturn(organizationId)

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertEquals(listOf(organizationId), result)
  }

  @Test
  fun testResolvingOrganizationFromDataplaneIdWhenDataplaneNotFound() {
    val dataplaneId = UUID.randomUUID()
    val properties = mapOf(DATAPLANE_ID_HEADER to dataplaneId.toString())
    Mockito.`when`(dataplaneService.getDataplane(dataplaneId)).thenAnswer { throw RuntimeException("Dataplane not found: $dataplaneId") }

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertNull(result)
  }

  @Test
  fun testResolvingOrganizationFromDataplaneIdWhenDataplaneGroupNotFound() {
    val dataplaneId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val mockDataplane =
      Dataplane().apply {
        id = dataplaneId
        this.dataplaneGroupId = dataplaneGroupId
      }
    val properties = mapOf(DATAPLANE_ID_HEADER to dataplaneId.toString())
    Mockito.`when`(dataplaneService.getDataplane(dataplaneId)).thenReturn(mockDataplane)
    Mockito.`when`(dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId)).thenReturn(null)

    val result: List<UUID>? = resolver.resolveOrganization(properties)
    Assertions.assertNull(result)
  }

  companion object {
    private const val AUTH_USER_ID = "authUserId"
  }
}
