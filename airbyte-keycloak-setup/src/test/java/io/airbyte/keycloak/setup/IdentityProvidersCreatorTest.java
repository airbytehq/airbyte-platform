/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.config.IdentityProviderConfiguration;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
  private ConfigurationMapService configurationMapService;
  @Mock
  private IdentityProviderConfiguration identityProviderConfiguration;
  @Mock
  private IdentityProvidersResource identityProvidersResource;
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
    when(identityProviderConfiguration.getType()).thenReturn(IdentityProviderConfiguration.ProviderType.OIDC);

    Map<String, String> configMap = new HashMap<>();
    when(configurationMapService.importProviderFrom(realmResource, identityProviderConfiguration, "oidc"))
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
    when(identityProviderConfiguration.getType()).thenReturn(IdentityProviderConfiguration.ProviderType.OIDC);

    Map<String, String> configMap = new HashMap<>();
    when(configurationMapService.importProviderFrom(realmResource, identityProviderConfiguration, "oidc"))
        .thenReturn(configMap);
    when(configurationMapService.setupProviderConfig(identityProviderConfiguration, configMap))
        .thenReturn(configMap);

    assertThrows(RuntimeException.class, () -> {
      identityProvidersCreator.createIdps(realmResource);
    });
  }

}
