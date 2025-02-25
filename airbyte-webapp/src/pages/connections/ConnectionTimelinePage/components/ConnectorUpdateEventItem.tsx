import { FormattedMessage } from "react-intl";
import { z } from "zod";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectorType } from "core/api/types/AirbyteClient";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { connectorUpdateEventSchema } from "../types";
import { isSemanticVersionTags, isVersionUpgraded } from "../utils";

interface ConnectorUpdateEventItemProps {
  event: z.infer<typeof connectorUpdateEventSchema>;
}

export const ConnectorUpdateEventItem: React.FC<ConnectorUpdateEventItemProps> = ({ event }) => {
  const {
    user,
    summary: {
      connectorName,
      connectorType,
      toVersion,
      fromVersion,
      changeReason = "USER", // set default value to "USER" since we don't get this value from the API
    },
  } = event;
  const connectorTypeLowerCase = connectorType.toLowerCase() as ConnectorType;
  const description = `connection.timeline.connector_update.version.description.reason`;

  const messageId = isSemanticVersionTags(toVersion, fromVersion)
    ? isVersionUpgraded(toVersion, fromVersion)
      ? `${description}.upgraded.${changeReason}`
      : `${description}.downgraded.${changeReason}`
    : `${description}.updated.${changeReason}`;

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon={connectorTypeLowerCase} />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage
              id="connection.timeline.connector_update.version"
              values={{
                connectorType: connectorTypeLowerCase,
              }}
            />
          </Text>
          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id={messageId}
              values={{
                user: <TimelineEventUser user={user} />,
                connectorName,
                fromVersion,
                toVersion,
              }}
            />
          </Text>
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
