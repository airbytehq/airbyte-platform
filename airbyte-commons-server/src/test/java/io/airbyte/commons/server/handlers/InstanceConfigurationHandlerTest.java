/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.AuthConfiguration;
import io.airbyte.api.model.generated.AuthConfiguration.ModeEnum;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.TrackingStrategyEnum;
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody;
import io.airbyte.api.model.generated.LicenseStatus;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.auth.config.AuthConfigs;
import io.airbyte.commons.auth.config.AuthMode;
import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense.LicenseType;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.Configs.AirbyteEdition;
import io.airbyte.config.Organization;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.data.services.PermissionService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@ExtendWith(MockitoExtension.class)
class InstanceConfigurationHandlerTest {

  private static final String AIRBYTE_URL = "http://localhost:8000";
  private static final String AIRBYTE_REALM = "airbyte";
  private static final String WEB_CLIENT_ID = "airbyte-webapp";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID USER_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();
  private static final String EMAIL = "org@airbyte.io";
  private static final String DEFAULT_ORG_NAME = "Default Org Name";
  private static final String DEFAULT_USER_NAME = "Default User Name";
  private static final Integer MAX_NODES = 12;
  private static final Integer MAX_EDITORS = 50;
  private static final Date EXPIRATION_DATE = new Date(2025, 12, 3);

  @Mock
  private WorkspacePersistence mWorkspacePersistence;
  @Mock
  private UserPersistence mUserPersistence;
  @Mock
  private WorkspacesHandler mWorkspacesHandler;
  @Mock
  private OrganizationPersistence mOrganizationPersistence;
  @Mock
  private AuthConfigs mAuthConfigs;
  @Mock
  private PermissionService permissionService;

  private AirbyteKeycloakConfiguration keycloakConfiguration;
  private ActiveAirbyteLicense activeAirbyteLicense;
  private InstanceConfigurationHandler instanceConfigurationHandler;

  @BeforeEach
  void setup() throws IOException {
    keycloakConfiguration = new AirbyteKeycloakConfiguration();
    keycloakConfiguration.setAirbyteRealm(AIRBYTE_REALM);
    keycloakConfiguration.setWebClientId(WEB_CLIENT_ID);

    activeAirbyteLicense = new ActiveAirbyteLicense();
    activeAirbyteLicense
        .setLicense(new AirbyteLicense(LicenseType.PRO, Optional.of(EXPIRATION_DATE), Optional.of(MAX_NODES), Optional.of(MAX_EDITORS)));
  }

  @ParameterizedTest
  @CsvSource({
    "true, true",
    "true, false",
    "false, true",
    "false, false"
  })
  void testGetInstanceConfiguration(final boolean isEnterprise, final boolean isInitialSetupComplete) throws IOException {
    stubGetDefaultUser();
    stubGetDefaultOrganization();
    if (isEnterprise) {
      stubEnterpriseAuthConfigs();
    } else {
      stubDefaultAuthConfigs();
    }

    when(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(isInitialSetupComplete);
    instanceConfigurationHandler = getInstanceConfigurationHandler(isEnterprise);

    final InstanceConfigurationResponse expected = new InstanceConfigurationResponse()
        .edition(isEnterprise ? EditionEnum.PRO : EditionEnum.COMMUNITY)
        .version("0.50.1")
        .airbyteUrl(AIRBYTE_URL)
        .licenseStatus(isEnterprise ? LicenseStatus.EXCEEDED.PRO : null)
        .auth(isEnterprise ? new AuthConfiguration()
            .mode(ModeEnum.OIDC)
            .clientId(WEB_CLIENT_ID)
            .defaultRealm(AIRBYTE_REALM) : new AuthConfiguration().mode(ModeEnum.NONE))
        .initialSetupComplete(isInitialSetupComplete)
        .defaultUserId(USER_ID)
        .defaultOrganizationId(ORGANIZATION_ID)
        .defaultOrganizationEmail(EMAIL)
        .trackingStrategy(TrackingStrategyEnum.LOGGING)
        .licenseExpirationDate(isEnterprise ? EXPIRATION_DATE.toInstant().getEpochSecond() : null);

    final InstanceConfigurationResponse actual = instanceConfigurationHandler.getInstanceConfiguration();

    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @CsvSource({
    "logging, LOGGING", // lower case env works
    "LOGGING, LOGGING", // upper case env works
    "segment, SEGMENT", // lower case segment env works
    "SEGMENT, SEGMENT", // upper case segment env works
    "'' ,LOGGING", // empty env variable will become logging
    "unknownValue, LOGGING" // Unknown value will be treated as logging (since servers won't send segment events either)
  })
  void testGetInstanceConfigurationTrackingStrategy(final String envValue, final TrackingStrategyEnum expectedResult) throws IOException {
    stubGetDefaultUser();
    stubGetDefaultOrganization();
    stubDefaultAuthConfigs();

    when(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(true);

    final var handler = new InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        envValue,
        AirbyteEdition.COMMUNITY,
        new AirbyteVersion("0.50.1"),
        Optional.empty(),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionService);

    final var result = handler.getInstanceConfiguration();

    assertEquals(expectedResult, result.getTrackingStrategy());
  }

  @Test
  void testSetupInstanceConfigurationAlreadySetup() throws IOException {
    stubGetDefaultOrganization();

    when(mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID)).thenReturn(
        new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID)
            .withInitialSetupComplete(true)); // already setup, should trigger an error

    instanceConfigurationHandler = getInstanceConfigurationHandler(true);

    assertThrows(IllegalStateException.class, () -> instanceConfigurationHandler.setupInstanceConfiguration(
        new InstanceConfigurationSetupRequestBody()
            .email("test@mail.com")
            .displaySetupWizard(false)
            .initialSetupComplete(false)
            .anonymousDataCollection(false)));
  }

  @ParameterizedTest
  @CsvSource({
    "true, true",
    "true, false",
    "false, true",
    "false, false"
  })
  void testSetupInstanceConfiguration(final boolean userNamePresent, final boolean orgNamePresent)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    stubGetDefaultOrganization();
    stubGetDefaultUser();

    // first time default workspace is fetched, initial setup complete is false.
    // second time is after the workspace is updated, so initial setup complete is true.
    when(mWorkspacePersistence.getDefaultWorkspaceForOrganization(ORGANIZATION_ID))
        .thenReturn(
            new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withInitialSetupComplete(false))
        .thenReturn(
            new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withInitialSetupComplete(true));
    when(mWorkspacePersistence.getInitialSetupComplete()).thenReturn(true);
    when(mAuthConfigs.getAuthMode()).thenReturn(AuthMode.OIDC);
    when(mAuthConfigs.getKeycloakConfig()).thenReturn(keycloakConfiguration);

    instanceConfigurationHandler = getInstanceConfigurationHandler(true);

    final InstanceConfigurationResponse expected = new InstanceConfigurationResponse()
        .edition(EditionEnum.PRO)
        .version("0.50.1")
        .airbyteUrl(AIRBYTE_URL)
        .licenseStatus(LicenseStatus.PRO)
        .auth(new AuthConfiguration()
            .mode(ModeEnum.OIDC)
            .clientId(WEB_CLIENT_ID)
            .defaultRealm(AIRBYTE_REALM))
        .initialSetupComplete(true)
        .defaultUserId(USER_ID)
        .defaultOrganizationId(ORGANIZATION_ID)
        .defaultOrganizationEmail(EMAIL)
        .trackingStrategy(TrackingStrategyEnum.LOGGING)
        .licenseExpirationDate(EXPIRATION_DATE.toInstant().getEpochSecond());

    final InstanceConfigurationSetupRequestBody requestBody = new InstanceConfigurationSetupRequestBody()
        .email(EMAIL)
        .displaySetupWizard(true)
        .anonymousDataCollection(true)
        .initialSetupComplete(true);

    String expectedUserName = DEFAULT_USER_NAME;
    if (userNamePresent) {
      expectedUserName = "test user";
      requestBody.setUserName(expectedUserName);
    }

    String expectedOrgName = DEFAULT_ORG_NAME;
    if (orgNamePresent) {
      expectedOrgName = "test org";
      requestBody.setOrganizationName(expectedOrgName);
    }

    final InstanceConfigurationResponse actual = instanceConfigurationHandler.setupInstanceConfiguration(requestBody);

    assertEquals(expected, actual);

    // verify the user was updated with the email and name from the request
    verify(mUserPersistence).writeAuthenticatedUser(eq(new AuthenticatedUser()
        .withUserId(USER_ID)
        .withEmail(EMAIL)
        .withName(expectedUserName)));

    // verify the organization was updated with the name from the request
    verify(mOrganizationPersistence).updateOrganization(eq(new Organization()
        .withOrganizationId(ORGANIZATION_ID)
        .withName(expectedOrgName)
        .withEmail(EMAIL)
        .withUserId(USER_ID)));

    verify(mWorkspacesHandler).updateWorkspace(eq(new WorkspaceUpdate()
        .workspaceId(WORKSPACE_ID)
        .email(EMAIL)
        .displaySetupWizard(true)
        .anonymousDataCollection(true)
        .initialSetupComplete(true)));
  }

  @Test
  void testLicenseInfo() {
    final var handler = getInstanceConfigurationHandler(true);
    final var licenseInfoResponse = handler.licenseInfo();

    assertEquals(licenseInfoResponse.getExpirationDate(), EXPIRATION_DATE.toInstant().getEpochSecond());
    assertEquals(licenseInfoResponse.getMaxEditors(), MAX_EDITORS);
    assertEquals(licenseInfoResponse.getMaxNodes(), MAX_NODES);
  }

  private void stubGetDefaultUser() throws IOException {
    when(mUserPersistence.getDefaultUser()).thenReturn(
        Optional.of(new AuthenticatedUser()
            .withUserId(USER_ID)
            .withName(DEFAULT_USER_NAME)));
  }

  private void stubGetDefaultOrganization() throws IOException {
    when(mOrganizationPersistence.getDefaultOrganization()).thenReturn(
        Optional.of(new Organization()
            .withOrganizationId(ORGANIZATION_ID)
            .withName(DEFAULT_ORG_NAME)
            .withUserId(USER_ID)
            .withEmail(EMAIL)));
  }

  private void stubDefaultAuthConfigs() {
    when(mAuthConfigs.getAuthMode()).thenReturn(AuthMode.NONE);
  }

  private void stubEnterpriseAuthConfigs() {
    when(mAuthConfigs.getAuthMode()).thenReturn(AuthMode.OIDC);
    when(mAuthConfigs.getKeycloakConfig()).thenReturn(keycloakConfiguration);
  }

  private InstanceConfigurationHandler getInstanceConfigurationHandler(final boolean isEnterprise) {
    return new InstanceConfigurationHandler(
        Optional.of(AIRBYTE_URL),
        "logging",
        isEnterprise ? AirbyteEdition.PRO : AirbyteEdition.COMMUNITY,
        new AirbyteVersion("0.50.1"),
        isEnterprise ? Optional.of(activeAirbyteLicense) : Optional.empty(),
        mWorkspacePersistence,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence,
        mAuthConfigs,
        permissionService);
  }

}
