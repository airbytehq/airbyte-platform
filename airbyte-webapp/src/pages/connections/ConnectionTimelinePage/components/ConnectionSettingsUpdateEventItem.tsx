import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useModalService } from "hooks/services/Modal";

import {
  ConnectionSettingsUpdateEventItemDescription,
  MultiConnectionSettingsUpdateEventItemDescription,
  ShortConnectionSettingsUpdateEventItemDescription,
} from "./ConnectionSettingsUpdateEventItemDescriptions";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { connectionSettingsUpdateEventSchema, patchFields } from "../types";
import { titleIdMap } from "../utils";

interface ConnectionSettingsUpdateEventItemProps {
  event: InferType<typeof connectionSettingsUpdateEventSchema>;
}

export const ConnectionSettingsUpdateEventItem: React.FC<ConnectionSettingsUpdateEventItemProps> = ({ event }) => {
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const titleId = titleIdMap[event.eventType];
  const patchedFields = patchFields.filter((field) => event.summary.patches.hasOwnProperty(field));
  if (patchedFields.length === 0) {
    return null;
  }

  const firstPatchedField = patchedFields.at(0);

  const triggerModal = () => {
    openModal({
      size: "md",
      title: formatMessage({ id: "connection.timeline.connection_settings_update" }),
      content: () => (
        <Box p="lg">
          <FlexContainer direction="column">
            {patchedFields.map((field) => (
              <ShortConnectionSettingsUpdateEventItemDescription
                key={field}
                field={field}
                to={event.summary.patches[field].to}
                from={event.summary.patches[field].from}
              />
            ))}
          </FlexContainer>
        </Box>
      ),
    });
  };

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="connection" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id={titleId} />
          </Text>
          {firstPatchedField && (
            <span>
              {patchedFields.length === 1 ? (
                <ConnectionSettingsUpdateEventItemDescription
                  field={firstPatchedField}
                  user={event.user}
                  to={event.summary.patches[firstPatchedField].to}
                  from={event.summary.patches[firstPatchedField].from}
                />
              ) : (
                <MultiConnectionSettingsUpdateEventItemDescription
                  field={firstPatchedField}
                  user={event.user}
                  to={event.summary.patches[firstPatchedField].to}
                  from={event.summary.patches[firstPatchedField].from}
                  totalChanges={patchedFields.length}
                />
              )}{" "}
              {patchedFields.length > 1 && (
                <Button variant="link" onClick={triggerModal}>
                  <Text size="xs" color="grey">
                    <FormattedMessage id="connection.timeline.connection_settings_update.View details" />
                  </Text>
                </Button>
              )}
            </span>
          )}
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
