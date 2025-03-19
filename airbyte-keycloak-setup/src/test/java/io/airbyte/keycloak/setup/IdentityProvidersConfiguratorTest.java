/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static io.airbyte.keycloak.setup.IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_KEY;
import static io.airbyte.keycloak.setup.IdentityProvidersConfigurator.AIRBYTE_MANAGED_IDP_VALUE;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.OidcConfig;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.admin.client.resource.IdentityProviderResource;
import org.keycloak.admin.client.resource.IdentityProvidersResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.IdentityProviderRepresentation;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@ExtendWith(MockitoExtension.class)
class IdentityProvidersConfiguratorTest {

  @Mock
  private RealmResource realmResource;
  @Mock
  private ConfigurationMapService configurationMapService;
  @Mock
  private OidcConfig oidcConfig;
  @Mock
  private IdentityProvidersResource identityProvidersResource;
  @Mock
  private IdentityProviderResource identityProviderResource;
  @Mock
  private IdentityProviderRepresentation identityProviderRepresentation;
  @InjectMocks
  private IdentityProvidersConfigurator identityProvidersConfigurator;

  @BeforeEach
  void setUp() {
    identityProvidersConfigurator = new IdentityProvidersConfigurator(configurationMapService, Optional.of(oidcConfig));
  }

  @Nested
  class ConfigureIdp {

    @Test
    void testNoOidcConfig() {
      identityProvidersConfigurator = new IdentityProvidersConfigurator(configurationMapService, Optional.empty());

      identityProvidersConfigurator.configureIdp(realmResource);

      verifyNoInteractions(realmResource);
    }

    @Test
    void testNoExistingIdp() {
      when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
      when(identityProvidersResource.findAll()).thenReturn(Collections.emptyList());

      final Response response = mock(Response.class);
      when(response.getStatus()).thenReturn(201);
      when(identityProvidersResource.create(any(IdentityProviderRepresentation.class))).thenReturn(response);

      final Map<String, String> importedMap = mock(HashMap.class);
      final Map<String, String> configMap = mock(HashMap.class);
      when(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
          .thenReturn(importedMap);
      when(configurationMapService.setupProviderConfig(oidcConfig, importedMap))
          .thenReturn(configMap);

      identityProvidersConfigurator.configureIdp(realmResource);

      // verify the idp is created with the correct config
      verify(identityProvidersResource, times(1)).create(argThat(idp -> idp.getConfig().equals(configMap)));
      // verify that the idp is marked as managed by Airbyte
      verify(configMap, times(1)).put(AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE);
    }

    @Test
    void testOneExistingIdpNotMarked() {
      when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
      when(identityProvidersResource.findAll()).thenReturn(List.of(identityProviderRepresentation));
      when(identityProviderRepresentation.getInternalId()).thenReturn("some-internal-id");
      when(identityProviderRepresentation.getAlias()).thenReturn("some-alias");
      when(identityProvidersResource.get("some-alias")).thenReturn(identityProviderResource);

      final Map<String, String> importedMap = mock(HashMap.class);
      final Map<String, String> configMap = mock(HashMap.class);
      when(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
          .thenReturn(importedMap);
      when(configurationMapService.setupProviderConfig(oidcConfig, importedMap))
          .thenReturn(configMap);

      identityProvidersConfigurator.configureIdp(realmResource);

      // verify the existing idp (based on internal id) is updated with new config
      verify(identityProviderResource, times(1)).update(
          argThat(idp -> idp.getConfig().equals(configMap) && ("some-internal-id").equals(idp.getInternalId())));
      // verify that the idp is marked as managed by Airbyte
      verify(configMap, times(1)).put(AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE);
    }

    @Test
    void testMultipleExistingIdpOnlyOneMarked() {
      final IdentityProviderRepresentation unmarkedIdp = mock(IdentityProviderRepresentation.class);
      when(unmarkedIdp.getConfig()).thenReturn(Map.of()); // does not contain marked key
      when(identityProviderRepresentation.getConfig()).thenReturn(Map.of(AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE));

      when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
      when(identityProvidersResource.findAll()).thenReturn(List.of(unmarkedIdp, identityProviderRepresentation));
      when(identityProviderRepresentation.getInternalId()).thenReturn("some-internal-id");
      when(identityProviderRepresentation.getAlias()).thenReturn("some-alias");
      when(identityProvidersResource.get("some-alias")).thenReturn(identityProviderResource);

      final Map<String, String> importedMap = mock(HashMap.class);
      final Map<String, String> configMap = mock(HashMap.class);
      when(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
          .thenReturn(importedMap);
      when(configurationMapService.setupProviderConfig(oidcConfig, importedMap))
          .thenReturn(configMap);

      identityProvidersConfigurator.configureIdp(realmResource);

      // verify the marked idp is updated with new config
      verify(identityProviderResource, times(1)).update(
          argThat(idp -> idp.getConfig().equals(configMap) && ("some-internal-id").equals(idp.getInternalId())));
      // verify the unmarkedIdp was examined, but not touched
      verify(unmarkedIdp, times(1)).getConfig();
      verifyNoMoreInteractions(unmarkedIdp);
    }

    @Test
    void testMultipleExistingIdpsMultipleMarked() {
      final IdentityProviderRepresentation otherMarkedIdp = mock(IdentityProviderRepresentation.class);
      when(otherMarkedIdp.getConfig()).thenReturn(Map.of(AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE));
      when(identityProviderRepresentation.getConfig()).thenReturn(Map.of(AIRBYTE_MANAGED_IDP_KEY, AIRBYTE_MANAGED_IDP_VALUE));

      when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
      when(identityProvidersResource.findAll()).thenReturn(List.of(otherMarkedIdp, identityProviderRepresentation));

      identityProvidersConfigurator.configureIdp(realmResource);

      // verify the no creates or updates took place, because multiple idps were marked and could not be
      // distinguished
      verify(identityProviderResource, never()).update(any());
      verify(identityProviderResource, never()).update(any());
    }

    @Test
    void testMultipleExistingIdpsNoneMarked() {
      final IdentityProviderRepresentation otherUnmarkedIdp = mock(IdentityProviderRepresentation.class);
      when(otherUnmarkedIdp.getConfig()).thenReturn(Map.of());
      when(identityProviderRepresentation.getConfig()).thenReturn(Map.of());

      when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
      when(identityProvidersResource.findAll()).thenReturn(List.of(otherUnmarkedIdp, identityProviderRepresentation));

      identityProvidersConfigurator.configureIdp(realmResource);

      // verify the no creates or updates took place, because multiple idps could not be distinguished and
      // none were marked
      verify(identityProviderResource, never()).update(any());
      verify(identityProviderResource, never()).update(any());
    }

    @Test
    void testCreateFailureThrows() {
      when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
      when(identityProvidersResource.findAll()).thenReturn(Collections.emptyList());
      when(identityProvidersResource.create(any(IdentityProviderRepresentation.class)))
          .thenReturn(Response.status(Response.Status.BAD_REQUEST).build());

      final Map<String, String> configMap = new HashMap<>();
      when(configurationMapService.importProviderFrom(realmResource, oidcConfig, "oidc"))
          .thenReturn(configMap);
      when(configurationMapService.setupProviderConfig(oidcConfig, configMap))
          .thenReturn(configMap);

      assertThrows(RuntimeException.class, () -> {
        identityProvidersConfigurator.configureIdp(realmResource);
      });
    }

  }

}
