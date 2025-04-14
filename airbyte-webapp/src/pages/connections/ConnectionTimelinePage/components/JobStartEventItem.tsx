import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { jobStartedEventSchema } from "../types";
import { titleIdMap } from "../utils";
interface JobStartEventItemProps {
  event: z.infer<typeof jobStartedEventSchema>;
}

export const JobStartEventItem: React.FC<JobStartEventItemProps> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];

  const { descriptionId, icon, streamsCount } = useMemo<{
    descriptionId: string;
    icon: IconType;
    streamsCount: number | undefined;
  }>(() => {
    if (event.eventType === "CLEAR_STARTED") {
      return {
        descriptionId: "connection.timeline.clear_started.description",
        icon: "cross",
        streamsCount: event.summary.streams?.length ?? 0,
      };
    } else if (event.eventType === "REFRESH_STARTED") {
      return {
        descriptionId: "connection.timeline.refresh_started.description",
        icon: "rotate",
        streamsCount: event.summary.streams?.length ?? 0,
      };
    }
    return { descriptionId: "connection.timeline.sync_started.description", icon: "sync", streamsCount: undefined };
  }, [event.eventType, event.summary.streams]);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon={icon} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <Box pt="xs">
          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id={descriptionId}
              values={{
                user: <TimelineEventUser user={event.user} />,
                ...(streamsCount !== undefined && { value: streamsCount }),
              }}
            />
          </Text>
        </Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} eventId={event.id} />
    </ConnectionTimelineEventItem>
  );
};
