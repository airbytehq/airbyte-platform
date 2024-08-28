import capitalize from "lodash/capitalize";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Text } from "components/ui/Text";

import { TimelineEventUser } from "./TimelineEventUser";
import { patchFields, generalEventSchema } from "../types";

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
};

function translateFieldValues(
  field: string,
  from: string | boolean | undefined,
  to: string | boolean | undefined,
  formatMessage: ReturnType<typeof useIntl>["formatMessage"]
) {
  switch (field) {
    case "scheduleType":
      return {
        from: formatMessage({ id: `frequency.${from}` }),
        to: formatMessage({ id: `frequency.${to}` }),
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
  to?: string | boolean;
  from?: string | boolean;
}

export const ConnectionSettingsUpdateEventItemDescription: React.FC<
  ConnectionSettingsUpdateEventItemDescriptionProps
> = ({ user, field, to, from }) => {
  const { formatMessage } = useIntl();

  const translated = translateFieldValues(field, from, to, formatMessage);

  return (
    <Text as="span" size="sm" color="grey400">
      <FormattedMessage
        id="connection.timeline.connection_settings_update.descriptionWithUser"
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

  return (
    <Text as="span" size="sm" color="grey400">
      <FormattedMessage
        id="connection.timeline.connection_settings_update.descriptionWithUserMulti"
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

  return (
    <Text as="span" size="sm" color="grey400">
      <FormattedMessage
        id="connection.timeline.connection_settings_update.descriptionWithoutUser"
        values={{
          field: capitalize(formatMessage({ id: patchedFieldToI8n[field] }).toLowerCase()),
          from: translated.from,
          to: translated.to,
        }}
      />
    </Text>
  );
};
