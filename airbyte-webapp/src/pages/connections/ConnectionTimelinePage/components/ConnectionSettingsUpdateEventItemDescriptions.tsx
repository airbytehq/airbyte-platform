import capitalize from "lodash/capitalize";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { FormattedScheduleDataMessage } from "components/connection/ConnectionHeaderControls/FormattedScheduleDataMessage";
import { Text } from "components/ui/Text";

import { ConnectionScheduleData } from "core/api/types/AirbyteClient";

import { TimelineEventUser } from "./TimelineEventUser";
import { patchFields, generalEventSchema, scheduleDataSchema } from "../types";

const patchedFieldToI8n: Record<string, string> = {
  scheduleType: "form.scheduleType",
  name: "form.connectionName",
  namespaceDefinition: "connectionForm.namespaceDefinition.title",
  namespaceFormat: "connectionForm.namespaceFormat.title",
  prefix: "form.prefix",
  geography: "connection.geographyTitle",
  notifySchemaChanges: "connection.schemaUpdateNotifications.title",
  nonBreakingChangesPreference: "connectionForm.nonBreakingChangesPreference.autopropagation.label",
  backfillPreference: "connectionForm.backfillColumns.title",
  scheduleData: "frequency.syncSchedule",
};

const translateScheduleData = (scheduleData?: ConnectionScheduleData) => {
  if (!scheduleData) {
    return null;
  }

  return (
    <FormattedScheduleDataMessage
      scheduleData={scheduleData}
      scheduleType={scheduleData.basicSchedule ? "basic" : scheduleData.cron ? "cron" : "manual"}
    />
  );
};

function translateFieldValues(
  field: string,
  from: string | boolean | InferType<typeof scheduleDataSchema> | undefined,
  to: string | boolean | InferType<typeof scheduleDataSchema> | undefined,
  formatMessage: ReturnType<typeof useIntl>["formatMessage"]
) {
  switch (field) {
    case "scheduleType":
      return {
        from: formatMessage({ id: `frequency.${from}` }),
        to: formatMessage({ id: `frequency.${to}` }),
      };
    case "scheduleData":
      return {
        from: translateScheduleData(from as ConnectionScheduleData),
        to: translateScheduleData(to as ConnectionScheduleData),
      };
    case "namespaceDefinition":
      return {
        from: formatMessage({ id: `connectionForm.modal.destinationNamespace.option.${from}` }),
        to: formatMessage({ id: `connectionForm.modal.destinationNamespace.option.${to}` }),
      };
    case "geography":
      return {
        from: formatMessage({ id: `connection.geography.${from}` }),
        to: formatMessage({ id: `connection.geography.${to}` }),
      };
    case "nonBreakingChangesPreference":
      return {
        from: formatMessage({ id: `connectionForm.nonBreakingChangesPreference.autopropagation.${from}` }),
        to: formatMessage({ id: `connectionForm.nonBreakingChangesPreference.autopropagation.${to}` }),
      };
    default:
      return { from: from?.toString(), to: to?.toString() };
  }
}

interface ConnectionSettingsUpdateEventItemDescriptionProps {
  user: InferType<typeof generalEventSchema>["user"];
  field: (typeof patchFields)[number];
  to?: string | InferType<typeof scheduleDataSchema> | boolean;
  from?: string | InferType<typeof scheduleDataSchema> | boolean;
}

export const ConnectionSettingsUpdateEventItemDescription: React.FC<
  ConnectionSettingsUpdateEventItemDescriptionProps
> = ({ user, field, to, from }) => {
  const { formatMessage } = useIntl();

  const translated = translateFieldValues(field, from, to, formatMessage);
  const id =
    !!translated.from && !!translated.to
      ? "connection.timeline.connection_settings_update.descriptionWithUser"
      : !!translated.from
      ? "connection.timeline.connection_settings_remove.descriptionWithUser"
      : "connection.timeline.connection_settings_add.descriptionWithUser";

  return (
    <Text as="span" size="sm" color="grey400">
      <FormattedMessage
        id={id}
        values={{
          user: <TimelineEventUser user={user} />,
          field: formatMessage({ id: patchedFieldToI8n[field] }).toLowerCase(),
          from: translated.from,
          to: translated.to,
        }}
      />
    </Text>
  );
};

interface MultiConnectionSettingsUpdateEventItemDescriptionProps
  extends ConnectionSettingsUpdateEventItemDescriptionProps {
  totalChanges?: number;
}

export const MultiConnectionSettingsUpdateEventItemDescription: React.FC<
  MultiConnectionSettingsUpdateEventItemDescriptionProps
> = ({ user, field, to, from, totalChanges }) => {
  const { formatMessage } = useIntl();

  const translated = translateFieldValues(field, from, to, formatMessage);
  const id =
    !!translated.from && !!translated.to
      ? "connection.timeline.connection_settings_update.descriptionWithUserMulti"
      : !!translated.from
      ? "connection.timeline.connection_settings_remove.descriptionWithUserMulti"
      : "connection.timeline.connection_settings_add.descriptionWithUserMulti";
  return (
    <Text as="span" size="sm" color="grey400">
      <FormattedMessage
        id={id}
        values={{
          user: <TimelineEventUser user={user} />,
          field: formatMessage({ id: patchedFieldToI8n[field] }).toLowerCase(),
          from: translated.from,
          to: translated.to,
          otherChanges: (totalChanges ?? 1) - 1,
        }}
      />
    </Text>
  );
};

type ShortConnectionSettingsUpdateEventItemDescriptionProps = Pick<
  ConnectionSettingsUpdateEventItemDescriptionProps,
  "field" | "from" | "to"
>;

export const ShortConnectionSettingsUpdateEventItemDescription: React.FC<
  ShortConnectionSettingsUpdateEventItemDescriptionProps
> = ({ field, to, from }) => {
  const { formatMessage } = useIntl();

  const translated = translateFieldValues(field, from, to, formatMessage);
  const id =
    !!translated.from && !!translated.to
      ? "connection.timeline.connection_settings_update.descriptionWithoutUser"
      : !!translated.from
      ? "connection.timeline.connection_settings_remove.descriptionWithoutUser"
      : "connection.timeline.connection_settings_add.descriptionWithoutUser";
  return (
    <Text as="span" size="sm" color="grey400">
      <FormattedMessage
        id={id}
        values={{
          field: capitalize(formatMessage({ id: patchedFieldToI8n[field] }).toLowerCase()),
          from: translated.from,
          to: translated.to,
        }}
      />
    </Text>
  );
};
