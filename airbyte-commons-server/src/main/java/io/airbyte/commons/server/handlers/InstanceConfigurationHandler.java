/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.AuthConfiguration;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.LicenseTypeEnum;
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.airbyte.config.Configs.AirbyteEdition;
import io.airbyte.config.Organization;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * InstanceConfigurationHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Slf4j
@Singleton
public class InstanceConfigurationHandler {

  private final String webappUrl;
  private final AirbyteEdition airbyteEdition;
  private final Optional<AirbyteKeycloakConfiguration> airbyteKeycloakConfiguration;
  private final Optional<ActiveAirbyteLicense> activeAirbyteLicense;
  private final WorkspacePersistence workspacePersistence;
  private final WorkspacesHandler workspacesHandler;
  private final UserPersistence userPersistence;
  private final OrganizationPersistence organizationPersistence;

  // the injected webapp-url value defaults to `null` to preserve backwards compatibility.
  // TODO remove the default value once configurations are standardized to always include a
  // webapp-url.
  public InstanceConfigurationHandler(@Value("${airbyte.webapp-url:null}") final String webappUrl,
                                      final AirbyteEdition airbyteEdition,
                                      final Optional<AirbyteKeycloakConfiguration> airbyteKeycloakConfiguration,
                                      final Optional<ActiveAirbyteLicense> activeAirbyteLicense,
                                      final WorkspacePersistence workspacePersistence,
                                      final WorkspacesHandler workspacesHandler,
                                      final UserPersistence userPersistence,
                                      final OrganizationPersistence organizationPersistence) {
    this.webappUrl = webappUrl;
    this.airbyteEdition = airbyteEdition;
    this.airbyteKeycloakConfiguration = airbyteKeycloakConfiguration;
    this.activeAirbyteLicense = activeAirbyteLicense;
    this.workspacePersistence = workspacePersistence;
    this.workspacesHandler = workspacesHandler;
    this.userPersistence = userPersistence;
    this.organizationPersistence = organizationPersistence;
  }

  public InstanceConfigurationResponse getInstanceConfiguration() throws IOException {
    final UUID defaultOrganizationId = getDefaultOrganizationId();
    final StandardWorkspace defaultWorkspace = getDefaultWorkspace(defaultOrganizationId);

    return new InstanceConfigurationResponse()
        .webappUrl(webappUrl)
        .edition(Enums.convertTo(airbyteEdition, EditionEnum.class))
        .licenseType(getLicenseType())
        .auth(getAuthConfiguration())
        .initialSetupComplete(defaultWorkspace.getInitialSetupComplete())
        .defaultUserId(getDefaultUserId())
        .defaultOrganizationId(defaultOrganizationId)
        .defaultWorkspaceId(defaultWorkspace.getWorkspaceId());
  }

  public InstanceConfigurationResponse setupInstanceConfiguration(final InstanceConfigurationSetupRequestBody requestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final UUID defaultOrganizationId = getDefaultOrganizationId();
    final StandardWorkspace defaultWorkspace = getDefaultWorkspace(defaultOrganizationId);

    // Update the default organization and user with the provided information
    updateDefaultOrganization(requestBody);
    updateDefaultUser(requestBody);

    // Update the underlying workspace to mark the initial setup as complete
    workspacesHandler.updateWorkspace(new WorkspaceUpdate()
        .workspaceId(defaultWorkspace.getWorkspaceId())
        .email(requestBody.getEmail())
        .displaySetupWizard(requestBody.getDisplaySetupWizard())
        .anonymousDataCollection(requestBody.getAnonymousDataCollection())
        .initialSetupComplete(requestBody.getInitialSetupComplete()));

    // Return the updated instance configuration
    return getInstanceConfiguration();
  }

  private LicenseTypeEnum getLicenseType() {
    if (airbyteEdition.equals(AirbyteEdition.PRO) && activeAirbyteLicense.isPresent()) {
      return Enums.convertTo(activeAirbyteLicense.get().getLicenseType(), LicenseTypeEnum.class);
    } else {
      return null;
    }
  }

  private AuthConfiguration getAuthConfiguration() {
    if (airbyteEdition.equals(AirbyteEdition.PRO) && airbyteKeycloakConfiguration.isPresent()) {
      return new AuthConfiguration()
          .clientId(airbyteKeycloakConfiguration.get().getWebClientId())
          .defaultRealm(airbyteKeycloakConfiguration.get().getAirbyteRealm());
    } else {
      return null;
    }
  }

  private UUID getDefaultUserId() throws IOException {
    return userPersistence.getDefaultUser().orElseThrow(() -> new IllegalStateException("Default user does not exist.")).getUserId();
  }

  private void updateDefaultUser(final InstanceConfigurationSetupRequestBody requestBody) throws IOException {
    final User defaultUser = userPersistence.getDefaultUser().orElseThrow(() -> new IllegalStateException("Default user does not exist."));
    // email is a required request property, so always set it.
    defaultUser.setEmail(requestBody.getEmail());

    // name is currently optional, so only set it if it is provided.
    if (requestBody.getUserName() != null) {
      defaultUser.setName(requestBody.getUserName());
    }

    userPersistence.writeUser(defaultUser);
  }

  private UUID getDefaultOrganizationId() throws IOException {
    return organizationPersistence.getDefaultOrganization()
        .orElseThrow(() -> new IllegalStateException("Default organization does not exist."))
        .getOrganizationId();
  }

  private void updateDefaultOrganization(final InstanceConfigurationSetupRequestBody requestBody) throws IOException {
    final Organization defaultOrganization =
        organizationPersistence.getDefaultOrganization().orElseThrow(() -> new IllegalStateException("Default organization does not exist."));

    // email is a required request property, so always set it.
    defaultOrganization.setEmail(requestBody.getEmail());

    // name is currently optional, so only set it if it is provided.
    if (requestBody.getOrganizationName() != null) {
      defaultOrganization.setName(requestBody.getOrganizationName());
    }

    organizationPersistence.updateOrganization(defaultOrganization);
  }

  // Historically, instance setup for an OSS installation of Airbyte was stored on the one and only
  // workspace that was created for the instance. Now that OSS supports multiple workspaces, we
  // use the default Organization ID to select a workspace to use for instance setup. This is a hack.
  // TODO persist instance configuration to a separate resource, rather than using a workspace.
  private StandardWorkspace getDefaultWorkspace(final UUID organizationId) throws IOException {
    return workspacePersistence.getDefaultWorkspaceForOrganization(organizationId);
  }

}
