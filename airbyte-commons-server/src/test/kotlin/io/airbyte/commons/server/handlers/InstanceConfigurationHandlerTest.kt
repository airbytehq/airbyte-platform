/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.AuthConfiguration
import io.airbyte.api.model.generated.InstanceConfigurationResponse
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum
import io.airbyte.api.model.generated.InstanceConfigurationResponse.TrackingStrategyEnum
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody
import io.airbyte.api.model.generated.LicenseStatus
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.airbyte.commons.auth.config.AuthConfigs
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.AirbyteLicense
import io.airbyte.commons.license.AirbyteLicense.LicenseType
import io.airbyte.commons.server.helpers.KubernetesClientPermissionHelper
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Organization
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.User
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.validation.json.JsonValidationException
import io.fabric8.kubernetes.api.model.Node
import io.fabric8.kubernetes.api.model.NodeList
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation
import io.fabric8.kubernetes.client.dsl.Resource
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mock
import org.mockito.Mockito.mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.util.Arrays
import java.util.Date
import java.util.Optional
import java.util.UUID

@ExtendWith(MockitoExtension::class)
internal class InstanceConfigurationHandlerTest {
  @Mock
  private lateinit var mWorkspacePersistence: WorkspacePersistence

  @Mock
  private lateinit var mUserPersistence: UserPersistence

  @Mock
  private lateinit var mWorkspacesHandler: WorkspacesHandler

  @Mock
  private lateinit var mOrganizationPersistence: OrganizationPersistence

  @Mock
  private lateinit var mAuthConfigs: AuthConfigs

  @Mock
  private lateinit var permissionHandler: PermissionHandler

  @Mock
  private lateinit var mKubernetesClientHelper: Optional<KubernetesClientPermissionHelper>

  @Mock
  private lateinit var kubernetesClientPermissionHelperMock: KubernetesClientPermissionHelper

  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration
  private lateinit var activeAirbyteLicense: ActiveAirbyteLicense
  private lateinit var instanceConfigurationHandler: InstanceConfigurationHandler

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    keycloakConfiguration = AirbyteKeycloakConfiguration()
    keycloakConfiguration.airbyteRealm = AIRBYTE_REALM
    keycloakConfiguration.webClientId = WEB_CLIENT_ID

    activeAirbyteLicense = ActiveAirbyteLicense()
    activeAirbyteLicense.license =
      AirbyteLicense(LicenseType.ENTERPRISE, EXPIRATION_DATE, MAX_NODES, MAX_EDITORS, ENTERPRISE_CONNECTOR_IDS, false)
  }

  @ParameterizedTest
  @CsvSource(
    "true, true",
    "true, false",
    "false, true",
    "false, false",
  )
  @Throws(IOException::class)
  fun testGetInstanceConfiguration(
    isEnterprise: Boolean,
    isInitialSetupComplete: Boolean,
  ) {
    stubGetDefaultUser()
    stubGetDefaultOrganization()
    if (isEnterprise) {
      stubEnterpriseAuthConfigs()
    } else {
      stubDefaultAuthConfigs()
    }

    whenever(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(isInitialSetupComplete)
    instanceConfigurationHandler = getInstanceConfigurationHandler(isEnterprise)

    val expected =
      InstanceConfigurationResponse()
        .edition(if (isEnterprise) EditionEnum.ENTERPRISE else EditionEnum.COMMUNITY)
        .version("0.50.1")
        .airbyteUrl(AIRBYTE_URL)
        .licenseStatus(if (isEnterprise) LicenseStatus.PRO else null)
        .auth(
          if (isEnterprise) {
            AuthConfiguration()
              .mode(AuthConfiguration.ModeEnum.OIDC)
              .clientId(WEB_CLIENT_ID)
              .authorizationServerUrl(AUTHORIZATION_SERVER_URL)
              .defaultRealm(AIRBYTE_REALM)
          } else {
            AuthConfiguration().mode(AuthConfiguration.ModeEnum.NONE)
          },
        ).initialSetupComplete(isInitialSetupComplete)
        .defaultUserId(USER_ID)
        .defaultOrganizationId(ORGANIZATION_ID)
        .defaultOrganizationEmail(EMAIL)
        .trackingStrategy(TrackingStrategyEnum.LOGGING)
        .licenseExpirationDate(if (isEnterprise) EXPIRATION_DATE.toInstant().getEpochSecond() else null)

    val actual = instanceConfigurationHandler.instanceConfiguration

    Assertions.assertEquals(expected, actual)
  }

  @ParameterizedTest
  @CsvSource(
    "logging, LOGGING", // lower case env works
    "LOGGING, LOGGING", // upper case env works
    "segment, SEGMENT", // lower case segment env works
    "SEGMENT, SEGMENT", // upper case segment env works
    "'' ,LOGGING", // empty env variable will become logging
    "unknownValue, LOGGING", // Unknown value will be treated as logging (since servers won't send segment events either)
  )
  @Throws(IOException::class)
  fun testGetInstanceConfigurationTrackingStrategy(
    envValue: String,
    expectedResult: TrackingStrategyEnum?,
  ) {
    stubGetDefaultUser()
    stubGetDefaultOrganization()
    stubDefaultAuthConfigs()

    whenever(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(true)

    val handler =
      InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        envValue,
        AirbyteEdition.COMMUNITY,
        AirbyteVersion("0.50.1"),
        Optional.empty(),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionHandler,
        Optional.empty(),
        Optional.empty(),
        mKubernetesClientHelper,
      )

    val result = handler.instanceConfiguration

    Assertions.assertEquals(expectedResult, result.getTrackingStrategy())
  }

  @Test
  @Throws(IOException::class)
  fun testSetupInstanceConfigurationAlreadySetup() {
    stubGetDefaultOrganization()

    whenever(mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID)).thenReturn(
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withInitialSetupComplete(true),
    ) // already setup, should trigger an error

    instanceConfigurationHandler = getInstanceConfigurationHandler(true)

    Assertions.assertThrows(IllegalStateException::class.java) {
      instanceConfigurationHandler.setupInstanceConfiguration(
        InstanceConfigurationSetupRequestBody()
          .email("test@mail.com")
          .displaySetupWizard(false)
          .initialSetupComplete(false)
          .anonymousDataCollection(false),
      )
    }
  }

  @ParameterizedTest
  @CsvSource(
    "true, true, true",
    "true, true, false",
    "true, false, true",
    "true, false, false",
    "false, true, true",
    "false, true, false",
    "false, false, true",
    "false, false, false",
  )
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testSetupInstanceConfiguration(
    userNamePresent: Boolean,
    orgNamePresent: Boolean,
    userWithEmailAlreadyExists: Boolean,
  ) {
    stubGetDefaultOrganization()
    stubGetDefaultUser()

    // first time default workspace is fetched, initial setup complete is false.
    // second time is after the workspace is updated, so initial setup complete is true.
    whenever(mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID))
      .thenReturn(
        StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withInitialSetupComplete(false),
      ).thenReturn(
        StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withInitialSetupComplete(true),
      )
    whenever(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(true)
    whenever(mAuthConfigs.authMode).thenReturn(AuthMode.OIDC)
    whenever(mAuthConfigs.keycloakConfig).thenReturn(keycloakConfiguration)

    instanceConfigurationHandler = getInstanceConfigurationHandler(true)

    val expected =
      InstanceConfigurationResponse()
        .edition(EditionEnum.ENTERPRISE)
        .version("0.50.1")
        .airbyteUrl(AIRBYTE_URL)
        .licenseStatus(LicenseStatus.PRO)
        .auth(
          AuthConfiguration()
            .mode(AuthConfiguration.ModeEnum.OIDC)
            .clientId(WEB_CLIENT_ID)
            .defaultRealm(AIRBYTE_REALM)
            .authorizationServerUrl(AUTHORIZATION_SERVER_URL),
        ).initialSetupComplete(true)
        .defaultUserId(USER_ID)
        .defaultOrganizationId(ORGANIZATION_ID)
        .defaultOrganizationEmail(EMAIL)
        .trackingStrategy(TrackingStrategyEnum.LOGGING)
        .licenseExpirationDate(EXPIRATION_DATE.toInstant().getEpochSecond())

    val requestBody =
      InstanceConfigurationSetupRequestBody()
        .email(EMAIL)
        .displaySetupWizard(true)
        .anonymousDataCollection(true)
        .initialSetupComplete(true)

    var expectedUserName: String = DEFAULT_USER_NAME
    if (userNamePresent) {
      expectedUserName = "test user"
      requestBody.setUserName(expectedUserName)
    }

    var expectedOrgName: String = DEFAULT_ORG_NAME
    if (orgNamePresent) {
      expectedOrgName = "test org"
      requestBody.setOrganizationName(expectedOrgName)
    }

    val expectedEmailUpdate: String
    if (userWithEmailAlreadyExists) {
      expectedEmailUpdate = DEFAULT_USER_EMAIL // email should not be updated if it would conflict with an existing user
      whenever(mUserPersistence.getUserByEmail(EMAIL)).thenReturn(Optional.of(User().withEmail(EMAIL)))
    } else {
      expectedEmailUpdate = EMAIL
    }

    val actual = instanceConfigurationHandler.setupInstanceConfiguration(requestBody)

    Assertions.assertEquals(expected, actual)

    // verify the user was updated with the expected email and name from the request
    verify(mUserPersistence).writeAuthenticatedUser(
      eq(
        AuthenticatedUser()
          .withUserId(USER_ID)
          .withEmail(expectedEmailUpdate)
          .withName(expectedUserName),
      ),
    )

    // verify the organization was updated with the name from the request
    verify(mOrganizationPersistence).updateOrganization(
      eq(
        Organization()
          .withOrganizationId(ORGANIZATION_ID)
          .withName(expectedOrgName)
          .withEmail(EMAIL)
          .withUserId(USER_ID),
      ),
    )

    verify(mWorkspacesHandler).updateWorkspace(
      eq(
        WorkspaceUpdate()
          .workspaceId(WORKSPACE_ID)
          .email(EMAIL)
          .displaySetupWizard(true)
          .anonymousDataCollection(true)
          .initialSetupComplete(true),
      ),
    )
  }

  @Test
  fun testLicenseInfo() {
    val handler = getInstanceConfigurationHandler(true)
    val licenseInfoResponse = handler.licenseInfo()

    Assertions.assertEquals(licenseInfoResponse?.getExpirationDate(), EXPIRATION_DATE.toInstant().getEpochSecond())
    Assertions.assertEquals(licenseInfoResponse?.getMaxEditors(), MAX_EDITORS)
    Assertions.assertEquals(licenseInfoResponse?.getMaxNodes(), MAX_NODES)
    Assertions.assertEquals(licenseInfoResponse?.getUsedNodes(), null)
  }

  @Test
  fun testLicenseInfoWithUsedNodes() {
    val mockNodesOperation = mock<NonNamespaceOperation<Node, NodeList, Resource<Node>>>()
    val nodeList = NodeList()
    nodeList.setItems(Arrays.asList(Node(), Node(), Node(), Node(), Node()))

    whenever(kubernetesClientPermissionHelperMock.listNodes())
      .thenReturn(mockNodesOperation)
    whenever(mockNodesOperation.list()).thenReturn(nodeList)

    val handler =
      InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        "logging",
        AirbyteEdition.ENTERPRISE,
        AirbyteVersion("0.50.1"),
        Optional.of(activeAirbyteLicense),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionHandler,
        Optional.empty(),
        Optional.empty(),
        Optional.of(kubernetesClientPermissionHelperMock),
      )

    val licenseInfoResponse = handler.licenseInfo()

    Assertions.assertEquals(licenseInfoResponse?.getExpirationDate(), EXPIRATION_DATE.toInstant().getEpochSecond())
    Assertions.assertEquals(licenseInfoResponse?.getMaxEditors(), MAX_EDITORS)
    Assertions.assertEquals(licenseInfoResponse?.getMaxNodes(), MAX_NODES)
    Assertions.assertEquals(licenseInfoResponse?.getUsedNodes(), nodeList.getItems().size)
  }

  @Test
  fun testInvalidLicenseTest() {
    val license = ActiveAirbyteLicense()
    license.license = null
    val handler =
      InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        "logging",
        AirbyteEdition.ENTERPRISE,
        AirbyteVersion("0.50.1"),
        Optional.of(license),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionHandler,
        Optional.empty(),
        Optional.empty(),
        mKubernetesClientHelper,
      )
    Assertions.assertEquals(handler.currentLicenseStatus(), LicenseStatus.INVALID)
  }

  @Test
  fun testExpiredLicenseTest() {
    val handler =
      InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        "logging",
        AirbyteEdition.ENTERPRISE,
        AirbyteVersion("0.50.1"),
        Optional.of(activeAirbyteLicense),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionHandler,
        Optional.of(Clock.fixed(Instant.MAX, ZoneId.systemDefault())),
        Optional.empty(),
        mKubernetesClientHelper,
      )
    Assertions.assertEquals(handler.currentLicenseStatus(), LicenseStatus.EXPIRED)
  }

  @Test
  fun testExceededEditorsLicenseTest() {
    val handler =
      InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        "logging",
        AirbyteEdition.ENTERPRISE,
        AirbyteVersion("0.50.1"),
        Optional.of(activeAirbyteLicense),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionHandler,
        Optional.empty(),
        Optional.empty(),
        mKubernetesClientHelper,
      )
    whenever(permissionHandler.countInstanceEditors()).thenReturn(MAX_EDITORS + 10)
    Assertions.assertEquals(handler.currentLicenseStatus(), LicenseStatus.EXCEEDED)
  }

  @Throws(IOException::class)
  private fun stubGetDefaultUser() {
    whenever(mUserPersistence.getDefaultUser()).thenReturn(
      Optional.of(
        AuthenticatedUser()
          .withUserId(USER_ID)
          .withName(DEFAULT_USER_NAME)
          .withEmail(DEFAULT_USER_EMAIL),
      ),
    )
  }

  @Throws(IOException::class)
  private fun stubGetDefaultOrganization() {
    whenever(mOrganizationPersistence.defaultOrganization).thenReturn(
      Optional.of(
        Organization()
          .withOrganizationId(ORGANIZATION_ID)
          .withName(DEFAULT_ORG_NAME)
          .withUserId(USER_ID)
          .withEmail(EMAIL),
      ),
    )
  }

  private fun stubDefaultAuthConfigs() {
    whenever(mAuthConfigs.authMode).thenReturn(AuthMode.NONE)
  }

  private fun stubEnterpriseAuthConfigs() {
    whenever(mAuthConfigs.authMode).thenReturn(AuthMode.OIDC)
    whenever(mAuthConfigs.keycloakConfig).thenReturn(keycloakConfiguration)
  }

  private fun getInstanceConfigurationHandler(isEnterprise: Boolean): InstanceConfigurationHandler =
    InstanceConfigurationHandler(
      Optional.of(AIRBYTE_URL),
      "logging",
      if (isEnterprise) AirbyteEdition.ENTERPRISE else AirbyteEdition.COMMUNITY,
      AirbyteVersion("0.50.1"),
      if (isEnterprise) Optional.of(activeAirbyteLicense) else Optional.empty(),
      mWorkspacePersistence,
      mWorkspacesHandler,
      mUserPersistence,
      mOrganizationPersistence,
      mAuthConfigs,
      permissionHandler,
      Optional.empty(),
      Optional.empty(),
      mKubernetesClientHelper,
    )

  companion object {
    private const val AIRBYTE_URL = "http://localhost:8000"
    private const val AIRBYTE_REALM = "airbyte"
    private const val AUTHORIZATION_SERVER_URL = "http://localhost:8000/auth/realms/airbyte"
    private const val WEB_CLIENT_ID = "airbyte-webapp"
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val USER_ID: UUID = UUID.randomUUID()
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private const val EMAIL = "org@airbyte.io"
    private const val DEFAULT_ORG_NAME = "Default Org Name"
    private const val DEFAULT_USER_NAME = "Default User Name"
    private const val DEFAULT_USER_EMAIL = "" // matches what we do in production code
    private const val MAX_NODES = 12
    private const val MAX_EDITORS = 50
    private val ENTERPRISE_CONNECTOR_IDS = mutableSetOf<UUID>()
    private val EXPIRATION_DATE = Date(2025, 12, 3)
  }
}
