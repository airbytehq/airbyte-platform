package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.NotificationItem
import io.airbyte.api.model.generated.NotificationSettings
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.server.converters.NotificationConverter
import io.airbyte.commons.server.converters.NotificationSettingsConverter
import io.airbyte.commons.server.converters.WorkspaceWebhookConfigsConverter
import io.airbyte.config.Geography
import io.airbyte.config.Organization
import io.airbyte.config.StandardWorkspace
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

// These helpers exist so that we can get some of the utility of working with workspaces but without needing to inject WorkspacesHandler

fun buildStandardWorkspace(
  workspaceCreateWithId: WorkspaceCreateWithId,
  organization: Organization,
  uuidSupplier: Supplier<UUID>,
): StandardWorkspace {
  val email = workspaceCreateWithId.email
  val anonymousDataCollection = workspaceCreateWithId.anonymousDataCollection
  val news = workspaceCreateWithId.news
  val securityUpdates = workspaceCreateWithId.securityUpdates
  val displaySetupWizard = workspaceCreateWithId.displaySetupWizard

  // if not set on the workspaceCreate, set the defaultGeography to AUTO
  val defaultGeography =
    if (workspaceCreateWithId.defaultGeography != null) {
      Enums.convertTo(
        workspaceCreateWithId.defaultGeography,
        Geography::class.java,
      )
    } else {
      Geography.AUTO
    }

  // NotificationSettings from input will be patched with default values.
  val notificationSettings: NotificationSettings = patchNotificationSettingsWithDefaultValue(workspaceCreateWithId)

  return StandardWorkspace().apply {
    this.workspaceId = workspaceCreateWithId.id ?: uuidSupplier.get()
    this.customerId = uuidSupplier.get() // "customer_id" should be deprecated
    this.name = workspaceCreateWithId.name
    this.slug = uuidSupplier.get().toString()
    this.initialSetupComplete = false
    this.anonymousDataCollection = anonymousDataCollection ?: false
    this.news = news ?: false
    this.securityUpdates = securityUpdates ?: false
    this.displaySetupWizard = displaySetupWizard ?: false
    this.tombstone = false
    this.notifications = NotificationConverter.toConfigList(workspaceCreateWithId.notifications)
    this.notificationSettings = NotificationSettingsConverter.toConfig(notificationSettings)
    this.defaultGeography = defaultGeography
    this.webhookOperationConfigs = WorkspaceWebhookConfigsConverter.toPersistenceWrite(workspaceCreateWithId.webhookConfigs, uuidSupplier)
    this.organizationId = organization.organizationId
    this.email = email
  }
}

private fun patchNotificationSettingsWithDefaultValue(workspaceCreateWithId: WorkspaceCreateWithId): NotificationSettings {
  val defaultNotificationType = NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO)
  return NotificationSettings().apply {
    this.sendOnSuccess = workspaceCreateWithId.notificationSettings?.sendOnSuccess ?: NotificationItem().notificationType(emptyList())
    this.sendOnFailure = workspaceCreateWithId.notificationSettings?.sendOnFailure ?: defaultNotificationType
    this.sendOnConnectionUpdate = workspaceCreateWithId.notificationSettings?.sendOnConnectionUpdate ?: defaultNotificationType

    this.sendOnConnectionUpdateActionRequired =
      workspaceCreateWithId.notificationSettings?.sendOnConnectionUpdateActionRequired ?: defaultNotificationType

    this.sendOnSyncDisabled = workspaceCreateWithId.notificationSettings?.sendOnSyncDisabled ?: defaultNotificationType
    this.sendOnSyncDisabledWarning = workspaceCreateWithId.notificationSettings?.sendOnSyncDisabledWarning ?: defaultNotificationType
    this.sendOnBreakingChangeWarning = workspaceCreateWithId.notificationSettings?.sendOnBreakingChangeWarning ?: defaultNotificationType

    this.sendOnBreakingChangeSyncsDisabled =
      workspaceCreateWithId.notificationSettings?.sendOnBreakingChangeSyncsDisabled ?: defaultNotificationType
  }
}

fun getDefaultWorkspaceName(
  organization: Optional<Organization>,
  companyName: String?,
  email: String,
): String {
  var defaultWorkspaceName = ""
  if (organization.isPresent) {
    // use organization name as default workspace name
    defaultWorkspaceName = organization.get().name.trim()
  }
  // if organization name is not available or empty, use user's company name (note: this is an
  // optional field)
  if (defaultWorkspaceName.isEmpty() && companyName != null) {
    defaultWorkspaceName = companyName.trim()
  }
  // if company name is still empty, use user's email (note: this is a required field)
  if (defaultWorkspaceName.isEmpty()) {
    defaultWorkspaceName = email
  }
  return defaultWorkspaceName
}
