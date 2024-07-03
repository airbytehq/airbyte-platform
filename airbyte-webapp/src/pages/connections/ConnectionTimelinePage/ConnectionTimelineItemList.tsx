import { ConnectionTimelineEventItem } from "./ConnectionTimelineEventItem";
import { ConnectionTimelineEvent } from "./utils";

export const ConnectionTimelineItemList: React.FC<{ timelineItems: ConnectionTimelineEvent[] }> = ({
  timelineItems,
}) => {
  return (
    <>
      {timelineItems.map((item, idx) => {
        return <ConnectionTimelineEventItem event={item} key={item.id} isLast={idx === timelineItems.length - 1} />;
      })}
    </>
  );
};
