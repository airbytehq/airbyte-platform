import { FormattedMessage } from "react-intl";
import { InferType } from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { sourceConnectorUpdateEventSchema } from "../types";
import { isSemanticVersionTags, isVersionUpgraded } from "../utils";

interface ConnectionEnabledEventItemProps {
  event: InferType<typeof sourceConnectorUpdateEventSchema>;
}

export const SourceConnectorUpdateEventItem: React.FC<ConnectionEnabledEventItemProps> = ({ event }) => {
  const description = "connection.timeline.connector_update.source.version.description.reason";

  const messageId = isSemanticVersionTags(event.summary.newDockerImageTag, event.summary.oldDockerImageTag)
    ? isVersionUpgraded(event.summary.newDockerImageTag, event.summary.oldDockerImageTag)
      ? `${description}.upgraded.${event.summary.changeReason}`
      : `${description}.downgraded.${event.summary.changeReason}`
    : `${description}.updated.${event.summary.changeReason}`;

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="source" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id="connection.timeline.connector_update.source.version" />
          </Text>
          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id={messageId}
              values={{
                user: <TimelineEventUser user={event.user} />,
                source: event.summary.name,
                from: event.summary.oldDockerImageTag,
                to: event.summary.newDockerImageTag,
              }}
            />
          </Text>
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
