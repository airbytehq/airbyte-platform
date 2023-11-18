/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.oauth.MoreOAuthParameters;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.AdvancedAuth.AuthFlowType;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OAuthConfigSupplierTest {

  static final String API_CLIENT = "api_client";
  static final String CREDENTIALS = "credentials";
  static final String PROPERTIES = "properties";

  private static final String AUTH_TYPE = "auth_type";
  private static final String OAUTH = "oauth";
  private static final String API_SECRET = "api_secret";

  // Existing field k/v used to test that we don't overwrite/lose unrelated data during injection
  private static final String EXISTING_FIELD_NAME = "fieldName";
  private static final String EXISTING_FIELD_VALUE = "fieldValue";

  private static final String SECRET_ONE = "mysecret";
  private static final String SECRET_TWO = "123";

  private ConfigRepository configRepository;
  private TrackingClient trackingClient;
  private OAuthConfigSupplier oAuthConfigSupplier;
  private UUID sourceDefinitionId;
  private StandardSourceDefinition testSourceDefinition;
  private ActorDefinitionVersion testSourceVersion;
  private ConnectorSpecification testConnectorSpecification;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  @BeforeEach
  void setup() throws JsonValidationException, ConfigNotFoundException, IOException {
    configRepository = mock(ConfigRepository.class);
    trackingClient = mock(TrackingClient.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    oAuthConfigSupplier = new OAuthConfigSupplier(configRepository, trackingClient, actorDefinitionVersionHelper);
    sourceDefinitionId = UUID.randomUUID();
    testSourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefinitionId)
        .withName("test");
    testSourceVersion = new ActorDefinitionVersion()
        .withDockerRepository("test/test")
        .withDockerImageTag("dev")
        .withSpec(null);

    final AdvancedAuth advancedAuth = createAdvancedAuth()
        .withPredicateKey(List.of(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue(OAUTH);

    testConnectorSpecification = createConnectorSpecification(advancedAuth);

    setupStandardDefinitionMock(advancedAuth);
  }

  @Test
  void testNoOAuthInjectionBecauseEmptyParams() throws IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    assertEquals(config, actualConfig);
    assertNoTracking();
  }

  @Test
  void testNoAuthMaskingBecauseEmptyParams() throws IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final JsonNode actualConfig =
        oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), testConnectorSpecification);
    assertEquals(config, actualConfig);
  }

  @Test
  void testNoOAuthInjectionBecauseMissingPredicateKey() throws IOException, JsonValidationException, ConfigNotFoundException {
    setupStandardDefinitionMock(createAdvancedAuth()
        .withPredicateKey(List.of("some_random_fields", AUTH_TYPE))
        .withPredicateValue(OAUTH));
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    setupOAuthParamMocks(generateOAuthParameters());
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    assertEquals(config, actualConfig);
    assertNoTracking();
  }

  @Test
  void testNoOAuthInjectionBecauseWrongPredicateValue() throws IOException, JsonValidationException, ConfigNotFoundException {
    setupStandardDefinitionMock(createAdvancedAuth()
        .withPredicateKey(List.of(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue("wrong_auth_type"));
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    setupOAuthParamMocks(generateOAuthParameters());
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    assertEquals(config, actualConfig);
    assertNoTracking();
  }

  @Test
  void testNoOAuthMaskingBecauseWrongPredicateValue() throws IOException, JsonValidationException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final ConnectorSpecification spec = createConnectorSpecification(createAdvancedAuth()
        .withPredicateKey(List.of(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue("wrong_auth_type"));
    setupOAuthParamMocks(generateOAuthParameters());
    final JsonNode actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), spec);
    assertEquals(config, actualConfig);
  }

  @Test
  void testOAuthInjection() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = getExpectedNode((String) oauthParameters.get(API_CLIENT));
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthMasking() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig =
        oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), testConnectorSpecification);
    final JsonNode expectedConfig = getExpectedNode(MoreOAuthParameters.SECRET_MASK);
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void testOAuthInjectionWithoutPredicate() throws JsonValidationException, IOException, ConfigNotFoundException {
    setupStandardDefinitionMock(createAdvancedAuth()
        .withPredicateKey(null)
        .withPredicateValue(null));
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = getExpectedNode((String) oauthParameters.get(API_CLIENT));
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthMaskingWithoutPredicate() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final ConnectorSpecification spec = createConnectorSpecification(createAdvancedAuth()
        .withPredicateKey(null)
        .withPredicateValue(null));
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), spec);
    final JsonNode expectedConfig = getExpectedNode(MoreOAuthParameters.SECRET_MASK);
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void testOAuthInjectionWithoutPredicateValue() throws JsonValidationException, IOException, ConfigNotFoundException {
    setupStandardDefinitionMock(createAdvancedAuth()
        .withPredicateKey(List.of(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue(""));
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = getExpectedNode((String) oauthParameters.get(API_CLIENT));
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthMaskingWithoutPredicateValue() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final ConnectorSpecification spec = createConnectorSpecification(createAdvancedAuth()
        .withPredicateKey(List.of(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue(""));
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), spec);
    final JsonNode expectedConfig = getExpectedNode(MoreOAuthParameters.SECRET_MASK);
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void testOAuthFullInjectionBecauseNoOAuthSpec() throws JsonValidationException, IOException, ConfigNotFoundException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    when(actorDefinitionVersionHelper.getSourceVersion(any(), eq(workspaceId), eq(sourceId))).thenReturn(testSourceVersion.withSpec(null));
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final ObjectNode expectedConfig = ((ObjectNode) Jsons.clone(config));
    for (final String key : oauthParameters.keySet()) {
      expectedConfig.set(key, Jsons.jsonNode(oauthParameters.get(key)));
    }
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthNoMaskingBecauseNoOAuthSpec() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), null);
    assertEquals(config, actualConfig);
  }

  @Test
  void testOAuthInjectionScopedToWorkspace() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateOAuthParameters();
    when(configRepository.getSourceOAuthParameterOptional(any(), any())).thenReturn(Optional.of(
        new SourceOAuthParameter()
            .withOauthParameterId(UUID.randomUUID())
            .withSourceDefinitionId(sourceDefinitionId)
            .withWorkspaceId(workspaceId)
            .withConfiguration(Jsons.jsonNode(oauthParameters))));
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = getExpectedNode((String) oauthParameters.get(API_CLIENT));
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthFullInjectionBecauseNoOAuthSpecNestedParameters() throws JsonValidationException, IOException {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateNestedOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = Jsons.jsonNode(Map.of(
        EXISTING_FIELD_NAME, EXISTING_FIELD_VALUE,
        CREDENTIALS, Map.of(
            API_SECRET, SECRET_TWO,
            AUTH_TYPE, OAUTH,
            API_CLIENT, ((Map<String, String>) oauthParameters.get(CREDENTIALS)).get(API_CLIENT))));
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthInjectionNestedParameters() throws JsonValidationException, IOException {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateNestedOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = getExpectedNode((String) ((Map<String, Object>) oauthParameters.get(CREDENTIALS)).get(API_CLIENT));
    assertEquals(expectedConfig, actualConfig);
    assertTracking(workspaceId);
  }

  @Test
  void testOAuthMaskingNestedParameters() throws JsonValidationException, IOException {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = generateNestedOAuthParameters();
    setupOAuthParamMocks(oauthParameters);
    final JsonNode actualConfig =
        oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, Jsons.clone(config), testConnectorSpecification);
    final JsonNode expectedConfig = getExpectedNode(MoreOAuthParameters.SECRET_MASK);
    assertEquals(expectedConfig, actualConfig);
  }

  @Test
  void testOAuthInjectingNestedSecrets() throws JsonValidationException, IOException {
    final JsonNode config = generateJsonConfig();
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final Map<String, Object> oauthParameters = Map.of(CREDENTIALS, generateSecretOAuthParameters());
    setupOAuthParamMocks(oauthParameters);

    final JsonNode actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, Jsons.clone(config));
    final JsonNode expectedConfig = getExpectedNode(secretCoordinateMap());
    assertEquals(expectedConfig, actualConfig);
  }

  private static ConnectorSpecification createConnectorSpecification(final AdvancedAuth advancedAuth) {
    return new ConnectorSpecification().withAdvancedAuth(advancedAuth);
  }

  private static AdvancedAuth createAdvancedAuth() {
    return new AdvancedAuth()
        .withAuthFlowType(AuthFlowType.OAUTH_2_0)
        .withOauthConfigSpecification(new OAuthConfigSpecification()
            .withCompleteOauthServerOutputSpecification(Jsons.jsonNode(Map.of(PROPERTIES,
                Map.of(API_CLIENT, Map.of(
                    "type", "string",
                    OAuthConfigSupplier.PATH_IN_CONNECTOR_CONFIG, List.of(CREDENTIALS, API_CLIENT)))))));
  }

  private void setupStandardDefinitionMock(final AdvancedAuth advancedAuth) throws JsonValidationException, ConfigNotFoundException, IOException {
    when(configRepository.getStandardSourceDefinition(any())).thenReturn(testSourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(any(), any(), any())).thenReturn(testSourceVersion
        .withSpec(createConnectorSpecification(advancedAuth)));
  }

  private void setupOAuthParamMocks(final Map<String, Object> oauthParameters) throws JsonValidationException, IOException {
    when(configRepository.getSourceOAuthParameterOptional(any(), any())).thenReturn(Optional.of(
        new SourceOAuthParameter()
            .withOauthParameterId(UUID.randomUUID())
            .withSourceDefinitionId(sourceDefinitionId)
            .withWorkspaceId(null)
            .withConfiguration(Jsons.jsonNode(oauthParameters))));
  }

  private static ObjectNode generateJsonConfig() {
    return (ObjectNode) Jsons.jsonNode(
        Map.of(
            EXISTING_FIELD_NAME, EXISTING_FIELD_VALUE,
            CREDENTIALS, Map.of(
                API_SECRET, SECRET_TWO,
                AUTH_TYPE, OAUTH)));
  }

  private static Map<String, Object> generateOAuthParameters() {
    return Map.of(
        API_SECRET, SECRET_ONE,
        API_CLIENT, UUID.randomUUID().toString());
  }

  private static Map<String, Object> generateSecretOAuthParameters() {
    return Map.of(
        API_SECRET, SECRET_ONE,
        API_CLIENT, secretCoordinateMap());
  }

  private static Map<String, Object> secretCoordinateMap() {
    return Map.of("_secret", "secret_coordinate");
  }

  private static Map<String, Object> generateNestedOAuthParameters() {
    return Map.of(CREDENTIALS, generateOAuthParameters());
  }

  private static JsonNode getExpectedNode(final String apiClient) {
    return Jsons.jsonNode(
        Map.of(
            EXISTING_FIELD_NAME, EXISTING_FIELD_VALUE,
            CREDENTIALS, Map.of(
                API_SECRET, SECRET_TWO,
                AUTH_TYPE, OAUTH,
                API_CLIENT, apiClient)));
  }

  private static JsonNode getExpectedNode(final Map<String, Object> apiClient) {
    return Jsons.jsonNode(
        Map.of(
            EXISTING_FIELD_NAME, EXISTING_FIELD_VALUE,
            CREDENTIALS, Map.of(
                API_SECRET, SECRET_TWO,
                AUTH_TYPE, OAUTH,
                API_CLIENT, apiClient)));
  }

  private void assertNoTracking() {
    verify(trackingClient, times(0)).track(any(), anyString(), anyMap());
  }

  private void assertTracking(final UUID workspaceId) {
    verify(trackingClient, times(1)).track(workspaceId, "OAuth Injection - Backend", Map.of(
        "connector_source", "test",
        "connector_source_definition_id", sourceDefinitionId,
        "connector_source_docker_repository", "test/test",
        "connector_source_version", "dev"));
  }

}
