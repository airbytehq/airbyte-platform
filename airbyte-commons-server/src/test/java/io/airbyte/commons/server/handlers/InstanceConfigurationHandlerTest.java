/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.AuthConfiguration;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.LicenseTypeEnum;
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense.LicenseType;
import io.airbyte.config.Configs.AirbyteEdition;
import io.airbyte.config.Organization;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InstanceConfigurationHandlerTest {

  private static final String WEBAPP_URL = "http://localhost:8000";
  private static final String AIRBYTE_REALM = "airbyte";
  private static final String WEB_CLIENT_ID = "airbyte-webapp";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID USER_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();
  private static final String DEFAULT_ORG_NAME = "Default Org Name";
  private static final String DEFAULT_USER_NAME = "Default User Name";

  @Mock
  private ConfigRepository mConfigRepository;
  @Mock
  private UserPersistence mUserPersistence;
  @Mock
  private WorkspacesHandler mWorkspacesHandler;
  @Mock
  private OrganizationPersistence mOrganizationPersistence;

  private AirbyteKeycloakConfiguration keycloakConfiguration;
  private ActiveAirbyteLicense activeAirbyteLicense;
  private InstanceConfigurationHandler instanceConfigurationHandler;

  @BeforeEach
  void setup() throws IOException {
    keycloakConfiguration = new AirbyteKeycloakConfiguration();
    keycloakConfiguration.setAirbyteRealm(AIRBYTE_REALM);
    keycloakConfiguration.setWebClientId(WEB_CLIENT_ID);

    activeAirbyteLicense = new ActiveAirbyteLicense();
    activeAirbyteLicense.setLicense(new AirbyteLicense(LicenseType.PRO));

    when(mUserPersistence.getDefaultUser()).thenReturn(
        Optional.of(new User()
            .withUserId(USER_ID)
            .withName(DEFAULT_USER_NAME)));

    when(mOrganizationPersistence.getDefaultOrganization()).thenReturn(
        Optional.of(new Organization()
            .withOrganizationId(ORGANIZATION_ID)
            .withName(DEFAULT_ORG_NAME)
            .withUserId(USER_ID)));
  }

  @ParameterizedTest
  @CsvSource({
    "true, true",
    "true, false",
    "false, true",
    "false, false"
  })
  void testGetInstanceConfiguration(final boolean isPro, final boolean isInitialSetupComplete)
      throws IOException, ConfigNotFoundException {
    when(mConfigRepository.listStandardWorkspaces(true)).thenReturn(
        List.of(new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID)
            .withInitialSetupComplete(isInitialSetupComplete)));

    instanceConfigurationHandler = new InstanceConfigurationHandler(
        WEBAPP_URL,
        isPro ? AirbyteEdition.PRO : AirbyteEdition.COMMUNITY,
        isPro ? Optional.of(keycloakConfiguration) : Optional.empty(),
        isPro ? Optional.of(activeAirbyteLicense) : Optional.empty(),
        mConfigRepository,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence);

    final InstanceConfigurationResponse expected = new InstanceConfigurationResponse()
        .edition(isPro ? EditionEnum.PRO : EditionEnum.COMMUNITY)
        .webappUrl(WEBAPP_URL)
        .licenseType(isPro ? LicenseTypeEnum.PRO : null)
        .auth(isPro ? new AuthConfiguration()
            .clientId(WEB_CLIENT_ID)
            .defaultRealm(AIRBYTE_REALM) : null)
        .initialSetupComplete(isInitialSetupComplete)
        .defaultUserId(USER_ID)
        .defaultOrganizationId(ORGANIZATION_ID)
        .defaultWorkspaceId(WORKSPACE_ID);

    final InstanceConfigurationResponse actual = instanceConfigurationHandler.getInstanceConfiguration();

    assertEquals(expected, actual);
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
    when(mConfigRepository.listStandardWorkspaces(true))
        .thenReturn(List.of(new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID)
            .withInitialSetupComplete(true))); // after the handler's update, the workspace should have initialSetupComplete: true when retrieved

    instanceConfigurationHandler = new InstanceConfigurationHandler(
        WEBAPP_URL,
        AirbyteEdition.PRO,
        Optional.of(keycloakConfiguration),
        Optional.of(activeAirbyteLicense),
        mConfigRepository,
        mWorkspacesHandler,
        mUserPersistence,
        mOrganizationPersistence);

    final String userName = userNamePresent ? "test user" : DEFAULT_USER_NAME;
    final String orgName = orgNamePresent ? "test org" : DEFAULT_ORG_NAME;
    final String email = "test@airbyte.com";

    final InstanceConfigurationResponse expected = new InstanceConfigurationResponse()
        .edition(EditionEnum.PRO)
        .webappUrl(WEBAPP_URL)
        .licenseType(LicenseTypeEnum.PRO)
        .auth(new AuthConfiguration()
            .clientId(WEB_CLIENT_ID)
            .defaultRealm(AIRBYTE_REALM))
        .initialSetupComplete(true)
        .defaultUserId(USER_ID)
        .defaultOrganizationId(ORGANIZATION_ID)
        .defaultWorkspaceId(WORKSPACE_ID);

    final InstanceConfigurationResponse actual = instanceConfigurationHandler.setupInstanceConfiguration(
        new InstanceConfigurationSetupRequestBody()
            .workspaceId(WORKSPACE_ID)
            .email(email)
            .initialSetupComplete(true)
            .anonymousDataCollection(true)
            .displaySetupWizard(true)
            .userName(userName)
            .organizationName(orgName));

    assertEquals(expected, actual);

    // verify the user was updated with the email and name from the request
    verify(mUserPersistence).writeUser(eq(new User()
        .withUserId(USER_ID)
        .withEmail(email)
        .withName(userName)));

    // verify the organization was updated with the name from the request
    verify(mOrganizationPersistence).updateOrganization(eq(new Organization()
        .withOrganizationId(ORGANIZATION_ID)
        .withName(orgName)
        .withEmail(email)
        .withUserId(USER_ID)));

    verify(mWorkspacesHandler).updateWorkspace(eq(new WorkspaceUpdate()
        .workspaceId(WORKSPACE_ID)
        .email(email)
        .displaySetupWizard(true)
        .anonymousDataCollection(true)
        .initialSetupComplete(true)));
  }

}
