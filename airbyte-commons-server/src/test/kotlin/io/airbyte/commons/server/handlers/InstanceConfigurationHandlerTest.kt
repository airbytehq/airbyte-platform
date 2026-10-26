/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.AuthConfiguration
import io.airbyte.api.server.generated.models.InstanceConfigurationResponse
import io.airbyte.api.server.generated.models.InstanceConfigurationSetupRequestBody
import io.airbyte.api.server.generated.models.WorkspaceRead
import io.airbyte.api.server.generated.models.WorkspaceUpdate
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
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.Optional
import java.util.UUID

internal class InstanceConfigurationHandlerTest {
  private val mWorkspacePersistence: WorkspacePersistence = mockk()

  private val mUserPersistence: UserPersistence = mockk()

  private val mWorkspacesHandler: WorkspacesHandler = mockk()

  private val mOrganizationService: OrganizationService = mockk()

  private val mAuthConfigs: AuthConfigs = mockk()

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

    every { mWorkspacePersistence.getInitialSetupComplete() } returns isInitialSetupComplete
    instanceConfigurationHandler = getInstanceConfigurationHandler()

    val expected =
      InstanceConfigurationResponse(
        edition = InstanceConfigurationResponse.Edition.COMMUNITY,
        version = "0.50.1",
        airbyteUrl = AIRBYTE_URL,
        auth =
          if (isOidc) {
            AuthConfiguration(
              mode = AuthConfiguration.Mode.OIDC,
              clientId = WEB_CLIENT_ID,
              authorizationServerUrl = AUTHORIZATION_SERVER_URL,
              defaultRealm = AIRBYTE_REALM,
            )
          } else {
            AuthConfiguration(mode = AuthConfiguration.Mode.NONE)
          },
        initialSetupComplete = isInitialSetupComplete,
        defaultUserId = USER_ID,
        defaultOrganizationId = ORGANIZATION_ID,
        defaultOrganizationEmail = EMAIL,
        trackingStrategy = InstanceConfigurationResponse.TrackingStrategy.LOGGING,
      )

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
    expectedResult: InstanceConfigurationResponse.TrackingStrategy?,
  ) {
    stubGetDefaultUser()
    stubGetDefaultOrganization()
    stubDefaultAuthConfigs()

    every { mWorkspacePersistence.getInitialSetupComplete() } returns true

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

    Assertions.assertEquals(expectedResult, result.trackingStrategy)
  }

  @Test
  fun testSetupInstanceConfigurationAlreadySetup() {
    stubGetDefaultOrganization()
    every { mOrganizationService.writeOrganization(any()) } just Runs
    every { mUserPersistence.getDefaultUser() } returns Optional.empty()

    // already setup, should trigger an error
    every { mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID) } returns
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withInitialSetupComplete(true)

    instanceConfigurationHandler = getInstanceConfigurationHandler()

    Assertions.assertThrows(IllegalStateException::class.java) {
      instanceConfigurationHandler.setupInstanceConfiguration(
        InstanceConfigurationSetupRequestBody(
          email = "test@mail.com",
          displaySetupWizard = false,
          initialSetupComplete = false,
          anonymousDataCollection = false,
        ),
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
    every { mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID) } returnsMany
      listOf(
        StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withInitialSetupComplete(false),
        StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withInitialSetupComplete(true),
      )
    every { mWorkspacePersistence.getInitialSetupComplete() } returns true
    every { mAuthConfigs.authMode } returns AuthMode.OIDC
    every { mAuthConfigs.keycloakConfig } returns keycloakConfiguration

    instanceConfigurationHandler = getInstanceConfigurationHandler()

    val expected =
      InstanceConfigurationResponse(
        edition = InstanceConfigurationResponse.Edition.COMMUNITY,
        version = "0.50.1",
        airbyteUrl = AIRBYTE_URL,
        auth =
          AuthConfiguration(
            mode = AuthConfiguration.Mode.OIDC,
            clientId = WEB_CLIENT_ID,
            defaultRealm = AIRBYTE_REALM,
            authorizationServerUrl = AUTHORIZATION_SERVER_URL,
          ),
        initialSetupComplete = true,
        defaultUserId = USER_ID,
        defaultOrganizationId = ORGANIZATION_ID,
        defaultOrganizationEmail = EMAIL,
        trackingStrategy = InstanceConfigurationResponse.TrackingStrategy.LOGGING,
      )

    val expectedUserName = if (userNamePresent) "test user" else DEFAULT_USER_NAME
    val expectedOrgName = if (orgNamePresent) "test org" else DEFAULT_ORG_NAME

    val requestBody =
      InstanceConfigurationSetupRequestBody(
        email = EMAIL,
        displaySetupWizard = true,
        anonymousDataCollection = true,
        initialSetupComplete = true,
        userName = if (userNamePresent) expectedUserName else null,
        organizationName = if (orgNamePresent) expectedOrgName else null,
      )

    val expectedEmailUpdate: String
    if (userWithEmailAlreadyExists) {
      expectedEmailUpdate = DEFAULT_USER_EMAIL // email should not be updated if it would conflict with an existing user
      every { mUserPersistence.getUserByEmail(EMAIL) } returns Optional.of(User().withEmail(EMAIL))
    } else {
      expectedEmailUpdate = EMAIL
      every { mUserPersistence.getUserByEmail(EMAIL) } returns Optional.empty()
    }
    every { mUserPersistence.writeAuthenticatedUser(any()) } just Runs
    every { mOrganizationService.writeOrganization(any()) } just Runs
    every { mWorkspacesHandler.updateWorkspace(any()) } returns mockk<WorkspaceRead>()

    val actual = instanceConfigurationHandler.setupInstanceConfiguration(requestBody)

    Assertions.assertEquals(expected, actual)

    // verify the user was updated with the expected email and name from the request
    verify {
      mUserPersistence.writeAuthenticatedUser(
        AuthenticatedUser()
          .withUserId(USER_ID)
          .withEmail(expectedEmailUpdate)
          .withName(expectedUserName),
      )
    }

    // verify the organization was updated with the name from the request
    verify {
      mOrganizationService.writeOrganization(
        Organization()
          .withOrganizationId(ORGANIZATION_ID)
          .withName(expectedOrgName)
          .withEmail(EMAIL)
          .withUserId(USER_ID),
      )
    }

    verify {
      mWorkspacesHandler.updateWorkspace(
        WorkspaceUpdate(
          workspaceId = WORKSPACE_ID,
          email = EMAIL,
          displaySetupWizard = true,
          anonymousDataCollection = true,
          initialSetupComplete = true,
        ),
      )
    }
  }

  private fun stubGetDefaultUser() {
    every { mUserPersistence.getDefaultUser() } returns
      Optional.of(
        AuthenticatedUser()
          .withUserId(USER_ID)
          .withName(DEFAULT_USER_NAME)
          .withEmail(DEFAULT_USER_EMAIL),
      )
  }

  private fun stubGetDefaultOrganization() {
    every { mOrganizationService.getDefaultOrganization() } returns
      Optional.of(
        Organization()
          .withOrganizationId(ORGANIZATION_ID)
          .withName(DEFAULT_ORG_NAME)
          .withUserId(USER_ID)
          .withEmail(EMAIL),
      )
  }

  private fun stubDefaultAuthConfigs() {
    every { mAuthConfigs.authMode } returns AuthMode.NONE
  }

  private fun stubOidcAuthConfigs() {
    every { mAuthConfigs.authMode } returns AuthMode.OIDC
    every { mAuthConfigs.keycloakConfig } returns keycloakConfiguration
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
