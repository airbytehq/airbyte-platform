/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.AuthConfiguration
import io.airbyte.api.model.generated.InstanceConfigurationResponse
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum
import io.airbyte.api.model.generated.InstanceConfigurationResponse.TrackingStrategyEnum
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.commons.auth.config.AuthConfigs
import io.airbyte.commons.auth.config.AuthMode
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Organization
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.OrganizationService
import io.airbyte.micronaut.runtime.AirbyteAnalyticsConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.airbyte.micronaut.runtime.AnalyticsTrackingStrategy
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
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
  private lateinit var mOrganizationService: OrganizationService

  @Mock
  private lateinit var mAuthConfigs: AuthConfigs

  private lateinit var keycloakConfiguration: AirbyteKeycloakConfig
  private lateinit var instanceConfigurationHandler: InstanceConfigurationHandler

  @BeforeEach
  fun setup() {
    keycloakConfiguration = AirbyteKeycloakConfig(airbyteRealm = AIRBYTE_REALM, webClientId = WEB_CLIENT_ID)
  }

  @ParameterizedTest
  @CsvSource(
    "true, true",
    "true, false",
    "false, true",
    "false, false",
  )
  fun testGetInstanceConfiguration(
    isOidc: Boolean,
    isInitialSetupComplete: Boolean,
  ) {
    stubGetDefaultUser()
    stubGetDefaultOrganization()
    if (isOidc) {
      stubOidcAuthConfigs()
    } else {
      stubDefaultAuthConfigs()
    }

    whenever(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(isInitialSetupComplete)
    instanceConfigurationHandler = getInstanceConfigurationHandler()

    val expected =
      InstanceConfigurationResponse()
        .edition(EditionEnum.COMMUNITY)
        .version("0.50.1")
        .airbyteUrl(AIRBYTE_URL)
        .auth(
          if (isOidc) {
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

    val actual = instanceConfigurationHandler.instanceConfiguration

    Assertions.assertEquals(expected, actual)
  }

  @ParameterizedTest
  @CsvSource(
    "logging, LOGGING", // lower case env works
    "LOGGING, LOGGING", // upper case env works
    "segment, SEGMENT", // lower case segment env works
    "SEGMENT, SEGMENT", // upper case segment env works
  )
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
        AirbyteConfig(airbyteUrl = AIRBYTE_URL, edition = AirbyteEdition.COMMUNITY, version = "0.50.1"),
        AirbyteAuthConfig(),
        // Micronaut handles mapping lower and upper case to the enumerated class, so simulate that here
        AirbyteAnalyticsConfig(strategy = AnalyticsTrackingStrategy.valueOf(envValue.uppercase())),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationService,
        mAuthConfigs,
      )

    val result = handler.instanceConfiguration

    Assertions.assertEquals(expectedResult, result.getTrackingStrategy())
  }

  @Test
  fun testSetupInstanceConfigurationAlreadySetup() {
    stubGetDefaultOrganization()

    whenever(mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID)).thenReturn(
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withInitialSetupComplete(true),
    ) // already setup, should trigger an error

    instanceConfigurationHandler = getInstanceConfigurationHandler()

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

    instanceConfigurationHandler = getInstanceConfigurationHandler()

    val expected =
      InstanceConfigurationResponse()
        .edition(EditionEnum.COMMUNITY)
        .version("0.50.1")
        .airbyteUrl(AIRBYTE_URL)
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
    verify(mOrganizationService).writeOrganization(
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

  private fun stubGetDefaultOrganization() {
    whenever(mOrganizationService.getDefaultOrganization()).thenReturn(
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

  private fun stubOidcAuthConfigs() {
    whenever(mAuthConfigs.authMode).thenReturn(AuthMode.OIDC)
    whenever(mAuthConfigs.keycloakConfig).thenReturn(keycloakConfiguration)
  }

  private fun getInstanceConfigurationHandler(): InstanceConfigurationHandler =
    InstanceConfigurationHandler(
      AirbyteConfig(
        airbyteUrl = AIRBYTE_URL,
        edition = AirbyteEdition.COMMUNITY,
        version = "0.50.1",
      ),
      AirbyteAuthConfig(),
      AirbyteAnalyticsConfig(strategy = AnalyticsTrackingStrategy.LOGGING),
      mWorkspacePersistence,
      mWorkspacesHandler,
      mUserPersistence,
      mOrganizationService,
      mAuthConfigs,
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
  }
}
