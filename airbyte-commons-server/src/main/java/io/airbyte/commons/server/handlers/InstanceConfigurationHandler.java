/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AuthConfiguration;
import io.airbyte.api.model.generated.AuthConfiguration.ModeEnum;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.TrackingStrategyEnum;
import io.airbyte.api.model.generated.InstanceConfigurationSetupRequestBody;
import io.airbyte.api.model.generated.LicenseInfoResponse;
import io.airbyte.api.model.generated.LicenseStatus;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.commons.auth.config.AuthConfigs;
import io.airbyte.commons.auth.config.AuthMode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.Configs.AirbyteEdition;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.data.services.PermissionService;
import io.airbyte.validation.json.JsonValidationException;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * InstanceConfigurationHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Slf4j
@Singleton
public class InstanceConfigurationHandler {

  public static final Set<Permission.PermissionType> EDITOR_ROLES =
      Set.of(Permission.PermissionType.ORGANIZATION_EDITOR, Permission.PermissionType.ORGANIZATION_ADMIN,
          Permission.PermissionType.WORKSPACE_EDITOR, Permission.PermissionType.WORKSPACE_OWNER, Permission.PermissionType.WORKSPACE_ADMIN);
  private final Optional<String> airbyteUrl;
  private final AirbyteEdition airbyteEdition;
  private final AirbyteVersion airbyteVersion;
  private final Optional<ActiveAirbyteLicense> activeAirbyteLicense;
  private final WorkspacePersistence workspacePersistence;
  private final WorkspacesHandler workspacesHandler;
  private final UserPersistence userPersistence;
  private final OrganizationPersistence organizationPersistence;
  private final String trackingStrategy;
  private final AuthConfigs authConfigs;
  private final PermissionService permissionService;
  private final Clock clock;
  private final Optional<KubernetesClient> kubernetesClient;

  public InstanceConfigurationHandler(@Named("airbyteUrl") final Optional<String> airbyteUrl,
                                      @Value("${airbyte.tracking.strategy:}") final String trackingStrategy,
                                      final AirbyteEdition airbyteEdition,
                                      final AirbyteVersion airbyteVersion,
                                      final Optional<ActiveAirbyteLicense> activeAirbyteLicense,
                                      final WorkspacePersistence workspacePersistence,
                                      final WorkspacesHandler workspacesHandler,
                                      final UserPersistence userPersistence,
                                      final OrganizationPersistence organizationPersistence,
                                      final AuthConfigs authConfigs,
                                      final PermissionService permissionService,
                                      final Optional<Clock> clock,
                                      final Optional<KubernetesClient> kubernetesClient) {
    this.airbyteUrl = airbyteUrl;
    this.trackingStrategy = trackingStrategy;
    this.airbyteEdition = airbyteEdition;
    this.airbyteVersion = airbyteVersion;
    this.activeAirbyteLicense = activeAirbyteLicense;
    this.workspacePersistence = workspacePersistence;
    this.workspacesHandler = workspacesHandler;
    this.userPersistence = userPersistence;
    this.organizationPersistence = organizationPersistence;
    this.authConfigs = authConfigs;
    this.permissionService = permissionService;
    this.clock = clock.orElse(Clock.systemUTC());
    this.kubernetesClient = kubernetesClient;
  }

  public InstanceConfigurationResponse getInstanceConfiguration() throws IOException {
    final Organization defaultOrganization = getDefaultOrganization();
    final Boolean initialSetupComplete = workspacePersistence.getInitialSetupComplete();

    return new InstanceConfigurationResponse()
        .airbyteUrl(airbyteUrl.orElse("airbyte-url-not-configured"))
        .edition(Enums.convertTo(airbyteEdition, EditionEnum.class))
        .version(airbyteVersion.serialize())
        .licenseStatus(getLicenseStatus())
        .licenseExpirationDate(licenseExpirationDate())
        .auth(getAuthConfiguration())
        .initialSetupComplete(initialSetupComplete)
        .defaultUserId(getDefaultUserId())
        .defaultOrganizationId(defaultOrganization.getOrganizationId())
        .defaultOrganizationEmail(defaultOrganization.getEmail())
        .trackingStrategy("segment".equalsIgnoreCase(trackingStrategy) ? TrackingStrategyEnum.SEGMENT : TrackingStrategyEnum.LOGGING);
  }

  public InstanceConfigurationResponse setupInstanceConfiguration(final InstanceConfigurationSetupRequestBody requestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final Organization defaultOrganization = getDefaultOrganization();
    final StandardWorkspace defaultWorkspace = getDefaultWorkspace(defaultOrganization.getOrganizationId());

    // Update the default organization and user with the provided information.
    // note that this is important especially for Community edition w/ Auth enabled,
    // because the login email must match the default organization's saved email in
    // order to login successfully.
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

  private LicenseStatus getLicenseStatus() {
    if (airbyteEdition.equals(AirbyteEdition.PRO) && activeAirbyteLicense.isPresent()) {
      return Enums.convertTo(activeAirbyteLicense.get().getLicenseType(), LicenseStatus.class);
    } else {
      return null;
    }
  }

  private AuthConfiguration getAuthConfiguration() {
    final AuthConfiguration authConfig = new AuthConfiguration().mode(Enums.convertTo(authConfigs.getAuthMode(), ModeEnum.class));

    // if Enterprise configurations are present, set OIDC-specific configs
    if (authConfigs.getAuthMode().equals(AuthMode.OIDC)) {
      // OIDC depends on Keycloak configuration being present
      if (authConfigs.getKeycloakConfig() == null) {
        throw new IllegalStateException("Keycloak configuration is required for OIDC mode.");
      }
      authConfig.setClientId(authConfigs.getKeycloakConfig().getWebClientId());
      authConfig.setDefaultRealm(authConfigs.getKeycloakConfig().getAirbyteRealm());
    }

    return authConfig;
  }

  private UUID getDefaultUserId() throws IOException {
    return userPersistence.getDefaultUser().orElseThrow(() -> new IllegalStateException("Default user does not exist.")).getUserId();
  }

  private void updateDefaultUser(final InstanceConfigurationSetupRequestBody requestBody) throws IOException {
    final AuthenticatedUser defaultUser =
        userPersistence.getDefaultUser().orElseThrow(() -> new IllegalStateException("Default user does not exist."));
    // email is a required request property, so always set it.
    defaultUser.setEmail(requestBody.getEmail());

    // name is currently optional, so only set it if it is provided.
    if (requestBody.getUserName() != null) {
      defaultUser.setName(requestBody.getUserName());
    }

    userPersistence.writeAuthenticatedUser(defaultUser);
  }

  private Organization getDefaultOrganization() throws IOException {
    return organizationPersistence.getDefaultOrganization()
        .orElseThrow(() -> new IllegalStateException("Default organization does not exist."));
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

  public LicenseInfoResponse licenseInfo() {
    final Optional<AirbyteLicense> licenseMaybe = activeAirbyteLicense.flatMap(ActiveAirbyteLicense::getLicense);
    if (licenseMaybe.isPresent()) {
      final AirbyteLicense license = licenseMaybe.get();
      return new LicenseInfoResponse()
          .edition(license.type().toString())
          .expirationDate(licenseExpirationDate())
          .usedEditors(editorsUsage())
          .usedNodes(0)
          .maxEditors(license.maxEditors().orElse(null))
          .maxNodes(license.maxNodes().orElse(null))
          .usedNodes(nodesUsage())
          .licenseStatus(getLicenseStatus());
    }
    return null;
  }

  private Long licenseExpirationDate() {
    final Optional<AirbyteLicense> licenseMaybe = activeAirbyteLicense.flatMap(ActiveAirbyteLicense::getLicense);

    if (licenseMaybe.isPresent()) {
      final AirbyteLicense license = licenseMaybe.get();
      return license.expirationDate().map(d -> d.toInstant().toEpochMilli() / 1000).orElse(null);
    }
    return null;
  }

  private Integer editorsUsage() {

    final var editors = permissionService.listPermissions().stream()
        .filter(p -> EDITOR_ROLES.contains(p.getPermissionType()))
        .map(Permission::getUserId).collect(Collectors.toSet());
    return editors.size();
  }

  @VisibleForTesting
  LicenseStatus currentLicenseStatus() {
    if (activeAirbyteLicense.isEmpty()
        || activeAirbyteLicense.get().getLicense().isEmpty()
        || activeAirbyteLicense.get().getLicense().get().type() == AirbyteLicense.LicenseType.INVALID) {
      return LicenseStatus.INVALID;
    }
    final AirbyteLicense actualLicense = activeAirbyteLicense.get().getLicense().get();
    if (actualLicense.expirationDate().map(exp -> exp.toInstant().isBefore(clock.instant())).orElse(false)) {
      return LicenseStatus.EXPIRED;
    }
    if (actualLicense.maxEditors().map(m -> editorsUsage() > m).orElse(false)) {
      return LicenseStatus.EXCEEDED;
    }
    return LicenseStatus.PRO;
  }

  private Integer nodesUsage() {
    return kubernetesClient.map(client -> client.nodes().list().getItems().size()).orElse(null);
  }

}
