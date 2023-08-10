/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.IdentityProviderConfiguration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.admin.client.resource.IdentityProviderResource;
import org.keycloak.admin.client.resource.IdentityProvidersResource;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.IdentityProviderRepresentation;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IdentityProvidersCreatorTest {

  @Mock
  private RealmResource realmResource;
  @Mock
  private Response response;
  @Mock
  private ConfigurationMapService configurationMapService;
  @Mock
  private IdentityProviderConfiguration identityProviderConfiguration;
  @Mock
  private IdentityProvidersResource identityProvidersResource;
  @Mock
  private IdentityProviderRepresentation idpRepresentation;
  @InjectMocks
  private IdentityProvidersCreator identityProvidersCreator;

  @BeforeEach
  void setUp() {
    identityProvidersCreator = new IdentityProvidersCreator(Collections.singletonList(identityProviderConfiguration),
        configurationMapService);
  }

  @Test
  void testCreateIdps() {
    when(realmResource.identityProviders()).thenReturn(identityProvidersResource);

    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(201);
    when(identityProvidersResource.create(any(IdentityProviderRepresentation.class))).thenReturn(response);

    identityProvidersCreator.createIdps(realmResource);

    verify(identityProvidersResource).create(any(IdentityProviderRepresentation.class));
  }

  @Test
  void testCreateIdps_Success() {
    when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
    when(identityProvidersResource.create(any(IdentityProviderRepresentation.class)))
        .thenReturn(Response.status(Response.Status.CREATED).build());
    when(identityProviderConfiguration.getType()).thenReturn(IdentityProviderConfiguration.ProviderType.OKTA);

    Map<String, String> configMap = new HashMap<>();
    when(configurationMapService.importProviderFrom(realmResource, identityProviderConfiguration, "keycloak-oidc"))
        .thenReturn(configMap);
    when(configurationMapService.setupProviderConfig(identityProviderConfiguration, configMap))
        .thenReturn(configMap);

    identityProvidersCreator.createIdps(realmResource);

    verify(realmResource, times(1)).identityProviders();
    verify(identityProvidersResource, times(1)).create(any(IdentityProviderRepresentation.class));
  }

  @Test
  void testCreateIdps_Failure() {
    when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
    when(identityProvidersResource.create(any(IdentityProviderRepresentation.class)))
        .thenReturn(Response.status(Response.Status.BAD_REQUEST).build());
    when(identityProviderConfiguration.getType()).thenReturn(IdentityProviderConfiguration.ProviderType.OKTA);

    Map<String, String> configMap = new HashMap<>();
    when(configurationMapService.importProviderFrom(realmResource, identityProviderConfiguration, "keycloak-oidc"))
        .thenReturn(configMap);
    when(configurationMapService.setupProviderConfig(identityProviderConfiguration, configMap))
        .thenReturn(configMap);

    assertThrows(RuntimeException.class, () -> {
      identityProvidersCreator.createIdps(realmResource);
    });
  }

  @Test
  void testResetIdentityProviders() {
    IdentityProviderRepresentation identityProviderRepresentation = mock(IdentityProviderRepresentation.class);
    IdentityProviderResource identityProvider = mock(IdentityProviderResource.class);
    Response response = mock(Response.class);

    when(realmResource.identityProviders()).thenReturn(identityProvidersResource);
    when(identityProvidersResource.findAll()).thenReturn(Arrays.asList(identityProviderRepresentation));
    when(identityProvidersResource.get(identityProviderRepresentation.getInternalId())).thenReturn(identityProvider);
    when(identityProvidersResource.create(any(IdentityProviderRepresentation.class))).thenReturn(response);
    when(response.getStatus()).thenReturn(201);

    identityProvidersCreator.resetIdentityProviders(realmResource);

    verify(identityProvidersResource).findAll();
    verify(identityProvidersResource).get(identityProviderRepresentation.getInternalId());
    verify(identityProvider).remove();
  }

}
