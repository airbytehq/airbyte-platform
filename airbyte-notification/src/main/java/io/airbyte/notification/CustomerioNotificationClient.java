/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.common.StreamDescriptorUtils;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.StreamAttributeTransform;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SyncSummary;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notification client that uses customer.io API send emails.
 *
 * These notifications rely on `TRANSACTION_MESSAGE_ID`, which are basically templates you create
 * through customer.io. These IDs are specific to a user's account on customer.io, so they will be
 * different for every user. For now they are stored as variables here, but in the future they may
 * be stored in as a notification config in the database.
 *
 * For Airbyte Cloud, Airbyte engineers may use `DEFAULT_TRANSACTION_MESSAGE_ID = "6"` as a generic
 * template for notifications.
 */
@SuppressWarnings("PMD.ConfusingArgumentToVarargsMethod")
public class CustomerioNotificationClient extends NotificationClient {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomerioNotificationClient.class);

  // Email templates created in Customer.io
  private static final String AUTO_DISABLE_TRANSACTION_MESSAGE_ID = "29";
  private static final String AUTO_DISABLE_WARNING_TRANSACTION_MESSAGE_ID = "30";
  private static final String BREAKING_CHANGE_WARNING_BROADCAST_ID = "32";
  private static final String BREAKING_CHANGE_SYNCS_DISABLED_BROADCAST_ID = "33";
  private static final String BREAKING_CHANGE_SYNCS_UPCOMING_UPGRADE_BROADCAST_ID = "48";
  private static final String BREAKING_CHANGE_SYNCS_UPGRADED_BROADCAST_ID = "47";
  private static final String SCHEMA_CHANGE_TRANSACTION_ID = "25";
  private static final String SCHEMA_BREAKING_CHANGE_TRANSACTION_ID = "24";
  private static final String SCHEMA_CHANGE_DETECTED_TRANSACTION_ID = "31";

  private static final String SYNC_SUCCEED_MESSAGE_ID = "27";
  private static final String SYNC_FAILURE_MESSAGE_ID = "26";

  private static final String CUSTOMERIO_BASE_URL = "https://api.customer.io/";
  private static final String CUSTOMERIO_EMAIL_API_ENDPOINT = "v1/send/email";
  private static final String CAMPAIGNS_PATH_SEGMENT = "campaigns";
  private static final String CUSTOMERIO_BROADCAST_API_ENDPOINT_TEMPLATE = "v1/" + CAMPAIGNS_PATH_SEGMENT + "/%s/triggers";

  private static final String CUSTOMERIO_TYPE = "customerio";
  public static final String CONNECTOR_NAME = "connector_name";
  public static final String CONNECTOR_TYPE = "connector_type";
  public static final String CONNECTOR_VERSION_NEW = "connector_version_new";
  public static final String CONNECTOR_VERSION_CHANGE_DESCRIPTION = "connector_version_change_description";
  public static final String CONNECTOR_VERSION_MIGRATION_URL = "connector_version_migration_url";
  public static final String CONNECTOR_VERSION_UPGRADE_DEADLINE = "connector_version_upgrade_deadline";

  private final String baseUrl;
  private final OkHttpClient okHttpClient;
  private final String apiToken;

  static {
    final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    MAPPER.setDateFormat(dateFormat);
    MAPPER.registerModule(new JavaTimeModule());
  }

  public CustomerioNotificationClient() {
    this.apiToken = System.getenv(EnvVar.CUSTOMERIO_API_KEY.name());
    this.baseUrl = CUSTOMERIO_BASE_URL;
    this.okHttpClient = new OkHttpClient.Builder()
        .addInterceptor(new CampaignsRateLimitInterceptor())
        .build();
  }

  @VisibleForTesting
  public CustomerioNotificationClient(final String apiToken,
                                      final String baseUrl) {
    this.apiToken = apiToken;
    this.baseUrl = baseUrl;
    this.okHttpClient = new OkHttpClient.Builder()
        .addInterceptor(new CampaignsRateLimitInterceptor())
        .build();
  }

  /**
   * Customer.io has a rate limit of 10 requests per second for broadcasts. This interceptor will
   * sleep for 10 seconds if a broadcast request fails with a 429 error.
   */
  static class CampaignsRateLimitInterceptor implements Interceptor {

    @NotNull
    @Override
    public Response intercept(@NotNull final Chain chain) throws IOException {
      final Request request = chain.request();
      Response response = chain.proceed(request);

      final int maxRetries = 5;
      int retryCount = 0;
      while (retryCount < maxRetries && !response.isSuccessful() && response.code() == 429
          && request.url().pathSegments().contains(CAMPAIGNS_PATH_SEGMENT)) {
        LOGGER.info("sleeping for 10s due to rate limit hit when sending broadcast...");
        Exceptions.swallow(() -> Thread.sleep(10000));
        response = chain.proceed(request);
        retryCount++;
      }

      return response;
    }

  }

  @Override
  public boolean notifyJobFailure(final SyncSummary summary,
                                  final String receiverEmail) {
    final ObjectNode node = buildSyncCompletedJson(summary, receiverEmail, SYNC_FAILURE_MESSAGE_ID);
    final String payload = Jsons.serialize(node);
    try {
      return notifyByEmail(payload);
    } catch (final IOException e) {
      return false;
    }
  }

  @Override
  public boolean notifyJobSuccess(final SyncSummary summary,
                                  final String receiverEmail) {
    final ObjectNode node = buildSyncCompletedJson(summary, receiverEmail, SYNC_SUCCEED_MESSAGE_ID);
    final String payload = Jsons.serialize(node);
    try {
      return notifyByEmail(payload);
    } catch (final IOException e) {
      return false;
    }
  }

  // Once the configs are editable through the UI, the reciever email should be stored in
  // airbyte-config/models/src/main/resources/types/CustomerioNotificationConfiguration.yaml
  // instead of being passed in
  @Override
  public boolean notifyConnectionDisabled(final SyncSummary summary,
                                          final String receiverEmail) {
    final ObjectNode node = buildSyncCompletedJson(summary, receiverEmail, AUTO_DISABLE_TRANSACTION_MESSAGE_ID);
    final String payload = Jsons.serialize(node);
    try {
      return notifyByEmail(payload);
    } catch (final IOException e) {
      return false;
    }
  }

  @Override
  public boolean notifyConnectionDisableWarning(final SyncSummary summary,
                                                final String receiverEmail) {
    final ObjectNode node = buildSyncCompletedJson(summary, receiverEmail, AUTO_DISABLE_WARNING_TRANSACTION_MESSAGE_ID);
    final String payload = Jsons.serialize(node);
    try {
      return notifyByEmail(payload);
    } catch (final IOException e) {
      return false;
    }
  }

  @Override
  public boolean notifyBreakingChangeWarning(final List<String> receiverEmails,
                                             final String connectorName,
                                             final ActorType actorType,
                                             final ActorDefinitionBreakingChange breakingChange) {
    try {
      return notifyByEmailBroadcast(BREAKING_CHANGE_WARNING_BROADCAST_ID, receiverEmails, Map.of(
          CONNECTOR_NAME, connectorName,
          CONNECTOR_TYPE, actorType.value(),
          CONNECTOR_VERSION_NEW, breakingChange.getVersion().serialize(),
          CONNECTOR_VERSION_UPGRADE_DEADLINE, formatDate(breakingChange.getUpgradeDeadline()),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION, convertMarkdownToHtml(breakingChange.getMessage()),
          CONNECTOR_VERSION_MIGRATION_URL, breakingChange.getMigrationDocumentationUrl()));
    } catch (final IOException e) {
      LOGGER.error("Failed to dispatch breaking change - sync warning notifications", e);
      return false;
    }
  }

  @Override
  public boolean notifyBreakingChangeSyncsDisabled(final List<String> receiverEmails,
                                                   final String connectorName,
                                                   final ActorType actorType,
                                                   final ActorDefinitionBreakingChange breakingChange) {
    try {
      return notifyByEmailBroadcast(BREAKING_CHANGE_SYNCS_DISABLED_BROADCAST_ID, receiverEmails, Map.of(
          CONNECTOR_NAME, connectorName,
          CONNECTOR_TYPE, actorType.value(),
          CONNECTOR_VERSION_NEW, breakingChange.getVersion().serialize(),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION, convertMarkdownToHtml(breakingChange.getMessage()),
          CONNECTOR_VERSION_MIGRATION_URL, breakingChange.getMigrationDocumentationUrl()));
    } catch (final IOException e) {
      LOGGER.error("Failed to dispatch breaking change - sync disabled notifications", e);
      return false;
    }
  }

  @Override
  public boolean notifyBreakingUpcomingAutoUpgrade(final List<String> receiverEmails,
                                                   final String connectorName,
                                                   final ActorType actorType,
                                                   final ActorDefinitionBreakingChange breakingChange) {
    try {
      return notifyByEmailBroadcast(BREAKING_CHANGE_SYNCS_UPCOMING_UPGRADE_BROADCAST_ID, receiverEmails, Map.of(
          CONNECTOR_NAME, connectorName,
          CONNECTOR_TYPE, actorType.value(),
          CONNECTOR_VERSION_NEW, breakingChange.getVersion().serialize(),
          CONNECTOR_VERSION_UPGRADE_DEADLINE, formatDate(breakingChange.getUpgradeDeadline()),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION, convertMarkdownToHtml(breakingChange.getMessage()),
          CONNECTOR_VERSION_MIGRATION_URL, breakingChange.getMigrationDocumentationUrl()));
    } catch (final IOException e) {
      LOGGER.error("Failed to dispatch breaking change - sync upcoming auto-upgrade notifications", e);
      return false;
    }

  }

  @Override
  public boolean notifyBreakingChangeSyncsUpgraded(final List<String> receiverEmails,
                                                   final String connectorName,
                                                   final ActorType actorType,
                                                   final ActorDefinitionBreakingChange breakingChange) {
    try {
      return notifyByEmailBroadcast(BREAKING_CHANGE_SYNCS_UPGRADED_BROADCAST_ID, receiverEmails, Map.of(
          CONNECTOR_NAME, connectorName,
          CONNECTOR_TYPE, actorType.value(),
          CONNECTOR_VERSION_NEW, breakingChange.getVersion().serialize(),
          CONNECTOR_VERSION_CHANGE_DESCRIPTION, convertMarkdownToHtml(breakingChange.getMessage()),
          CONNECTOR_VERSION_MIGRATION_URL, breakingChange.getMigrationDocumentationUrl()));
    } catch (final IOException e) {
      LOGGER.error("Failed to dispatch breaking change - sync auto upgraded notifications", e);
      return false;
    }
  }

  @Override
  public boolean notifySchemaPropagated(final SchemaUpdateNotification notification,
                                        final String recipient) {
    final String transactionalMessageId = notification.isBreakingChange() ? SCHEMA_BREAKING_CHANGE_TRANSACTION_ID : SCHEMA_CHANGE_TRANSACTION_ID;

    final ObjectNode node =
        buildSchemaChangeJson(notification, recipient, transactionalMessageId);

    final String payload = Jsons.serialize(node);
    try {
      return notifyByEmail(payload);
    } catch (final IOException e) {
      return false;
    }
  }

  @Override
  public boolean notifySchemaDiffToApply(final SchemaUpdateNotification notification, final String recipient) {
    final ObjectNode node =
        buildSchemaChangeJson(notification, recipient, SCHEMA_CHANGE_DETECTED_TRANSACTION_ID);
    final String payload = Jsons.serialize(node);
    try {
      return notifyByEmail(payload);
    } catch (final IOException e) {
      return false;
    }
  }

  static ObjectNode buildSyncCompletedJson(final SyncSummary syncSummary,
                                           final String recipient,
                                           final String transactionMessageId) {
    final ObjectNode node = MAPPER.createObjectNode();
    node.put("transactional_message_id", transactionMessageId);
    node.put("to", recipient);

    final ObjectNode identifiersNode = MAPPER.createObjectNode();
    identifiersNode.put("email", recipient);
    node.set("identifiers", identifiersNode);

    node.set("message_data", MAPPER.valueToTree(syncSummary));
    node.put("disable_message_retention", false);
    node.put("send_to_unsubscribed", true);
    node.put("tracked", true);
    node.put("queue_draft", false);
    node.put("disable_css_preprocessing", true);

    return node;
  }

  @NotNull
  @VisibleForTesting
  static ObjectNode buildSchemaChangeJson(final SchemaUpdateNotification notification,
                                          final String recipient,
                                          final String transactionalMessageId) {
    final ObjectNode node = MAPPER.createObjectNode();
    node.put("transactional_message_id", transactionalMessageId);
    node.put("to", recipient);

    final ObjectNode identifiersNode = MAPPER.createObjectNode();
    identifiersNode.put("email", recipient);
    node.set("identifiers", identifiersNode);

    final ObjectNode messageDataNode = MAPPER.createObjectNode();
    messageDataNode.put("connection_name", notification.getConnectionInfo().getName());
    messageDataNode.put("connection_id", notification.getConnectionInfo().getId().toString());
    messageDataNode.put("workspace_id", notification.getWorkspace().getId().toString());
    messageDataNode.put("workspace_name", notification.getWorkspace().getName());

    final ObjectNode changesNode = MAPPER.createObjectNode();
    messageDataNode.set("changes", changesNode);

    final CatalogDiff diff = notification.getCatalogDiff();

    final List<StreamTransform> newStreams =
        diff.getTransforms().stream().filter((t) -> t.getTransformType() == StreamTransform.TransformTypeEnum.ADD_STREAM).toList();
    LOGGER.info("Notify schema changes on new streams: {}", newStreams);
    final ArrayNode newStreamsNodes = MAPPER.createArrayNode();
    changesNode.set("new_streams", newStreamsNodes);
    for (final StreamTransform stream : newStreams) {
      newStreamsNodes.add(StreamDescriptorUtils.buildFullyQualifiedName(stream.getStreamDescriptor()));
    }

    final List<StreamTransform> deletedStreams =
        diff.getTransforms().stream().filter((t) -> t.getTransformType() == StreamTransform.TransformTypeEnum.REMOVE_STREAM).toList();
    LOGGER.info("Notify schema changes on deleted streams: {}", deletedStreams);
    final ArrayNode deletedStreamsNodes = MAPPER.createArrayNode();
    changesNode.set("deleted_streams", deletedStreamsNodes);
    for (final StreamTransform stream : deletedStreams) {
      deletedStreamsNodes.add(StreamDescriptorUtils.buildFullyQualifiedName(stream.getStreamDescriptor()));
    }

    final List<StreamTransform> alteredStreams =
        diff.getTransforms().stream().filter((t) -> t.getTransformType() == StreamTransform.TransformTypeEnum.UPDATE_STREAM).toList();
    LOGGER.info("Notify schema changes on altered streams: {}", alteredStreams);
    final ObjectNode modifiedStreamsNodes = MAPPER.createObjectNode();
    changesNode.set("modified_streams", modifiedStreamsNodes);

    for (final StreamTransform stream : alteredStreams) {

      final ObjectNode streamNode = MAPPER.createObjectNode();
      modifiedStreamsNodes.set(StreamDescriptorUtils.buildFullyQualifiedName(stream.getStreamDescriptor()), streamNode);
      final ArrayNode newFields = MAPPER.createArrayNode();
      final ArrayNode deletedFields = MAPPER.createArrayNode();
      final ArrayNode modifiedFields = MAPPER.createArrayNode();
      final ArrayNode updatedPrimaryKeyInfo = MAPPER.createArrayNode();

      streamNode.set("new", newFields);
      streamNode.set("deleted", deletedFields);
      streamNode.set("altered", modifiedFields);
      streamNode.set("updated_primary_key", updatedPrimaryKeyInfo);

      final Optional<StreamAttributeTransform> primaryKeyChangeOptional = stream.getUpdateStream().getStreamAttributeTransforms().stream()
          .filter(t -> t.getTransformType().equals(StreamAttributeTransform.TransformTypeEnum.UPDATE_PRIMARY_KEY))
          .findFirst();

      if (primaryKeyChangeOptional.isPresent()) {
        final StreamAttributeTransform primaryKeyChange = primaryKeyChangeOptional.get();
        final List<List<String>> oldPrimaryKey = primaryKeyChange.getUpdatePrimaryKey().getOldPrimaryKey();
        final String oldPrimaryKeyString = formatPrimaryKeyString(oldPrimaryKey);

        final List<List<String>> newPrimaryKey = primaryKeyChange.getUpdatePrimaryKey().getNewPrimaryKey();
        final String newPrimaryKeyString = formatPrimaryKeyString(newPrimaryKey);

        if (!oldPrimaryKeyString.isEmpty() && newPrimaryKeyString.isEmpty()) {
          updatedPrimaryKeyInfo.add(String.format("%s removed as primary key", oldPrimaryKeyString));
        } else if (oldPrimaryKeyString.isEmpty() && !newPrimaryKeyString.isEmpty()) {
          updatedPrimaryKeyInfo.add(String.format("%s added as primary key", newPrimaryKeyString));
        } else if (!oldPrimaryKeyString.isEmpty()) {
          updatedPrimaryKeyInfo.add(String.format("Primary key changed (%s -> %s)", oldPrimaryKeyString, newPrimaryKeyString));
        }
      }

      for (final var fieldChange : stream.getUpdateStream().getFieldTransforms()) {
        final String fieldName = StreamDescriptorUtils.buildFieldName(fieldChange.getFieldName());
        switch (fieldChange.getTransformType()) {
          case ADD_FIELD -> newFields.add(fieldName);
          case REMOVE_FIELD -> deletedFields.add(fieldName);
          case UPDATE_FIELD_SCHEMA -> modifiedFields.add(fieldName);
          default -> LOGGER.warn("Unknown TransformType: '{}'", fieldChange.getTransformType());
        }
      }
    }
    messageDataNode.put("source", notification.getSourceInfo().getName());
    messageDataNode.put("source_name", notification.getSourceInfo().getName());
    messageDataNode.put("source_id", notification.getSourceInfo().getId().toString());

    node.set("message_data", messageDataNode);

    node.put("disable_message_retention", false);
    node.put("send_to_unsubscribed", true);
    node.put("tracked", true);
    node.put("queue_draft", false);
    node.put("disable_css_preprocessing", true);
    return node;
  }

  @Override
  public String getNotificationClientType() {
    return CUSTOMERIO_TYPE;
  }

  private boolean notifyByEmail(final String requestPayload) throws IOException {
    return sendNotifyRequest(CUSTOMERIO_EMAIL_API_ENDPOINT, requestPayload);
  }

  @VisibleForTesting
  boolean notifyByEmailBroadcast(final String broadcastId, final List<String> emails, final Map<String, String> data) throws IOException {
    if (emails.isEmpty()) {
      LOGGER.info("No emails to notify. Skipping email notification.");
      return false;
    }

    final String broadcastTriggerUrl = String.format(CUSTOMERIO_BROADCAST_API_ENDPOINT_TEMPLATE, broadcastId);

    final String payload = Jsons.serialize(Map.of(
        "emails", emails,
        "data", data,
        "email_add_duplicates", true,
        "email_ignore_missing", true,
        "id_ignore_missing", true));

    return sendNotifyRequest(broadcastTriggerUrl, payload);
  }

  @VisibleForTesting
  boolean sendNotifyRequest(final String urlEndpoint, final String payload) throws IOException {
    if (StringUtils.isEmpty(apiToken)) {
      LOGGER.info("Customer.io API token is empty. Skipping email notification.");
      return false;
    }

    final String url = baseUrl + urlEndpoint;
    final RequestBody requestBody = RequestBody.create(payload, JSON);

    final okhttp3.Request request = new Request.Builder()
        .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", apiToken))
        .url(url)
        .post(requestBody)
        .build();

    try (final Response response = okHttpClient.newCall(request).execute()) {
      if (response.isSuccessful()) {
        LOGGER.info("Successful notification ({}): {}", response.code(), response.body());
        return true;
      } else {
        final String body = response.body() != null ? response.body().string() : "";
        final String errorMessage = String.format("Failed to deliver notification (%s): %s", response.code(), body);
        throw new IOException(errorMessage);
      }
    }
  }

  private String convertMarkdownToHtml(final String message) {
    final Parser markdownParser = Parser.builder().build();
    final Node document = markdownParser.parse(message);
    final HtmlRenderer renderer = HtmlRenderer.builder().build();
    return renderer.render(document);
  }

  private String formatDate(final String dateString) {
    final LocalDate date = LocalDate.parse(dateString);
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMMM d, yyyy");
    return date.format(formatter);
  }

}
