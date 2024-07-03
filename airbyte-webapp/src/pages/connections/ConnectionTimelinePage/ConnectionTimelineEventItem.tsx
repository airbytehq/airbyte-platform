import { FormattedDate, FormattedTime } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionTimelineEventBody } from "./ConnectionTimelineEventBody";
import { ConnectionTimelineEventIcon } from "./ConnectionTimelineEventIcon";
import styles from "./ConnectionTimelineEventItem.module.scss";
import { ConnectionTimelineEvent } from "./utils";
export const ConnectionTimelineEventItem: React.FC<{ event: ConnectionTimelineEvent; isLast: boolean }> = ({
  event,
  isLast,
}) => {
  return (
    <FlexContainer
      direction="row"
      gap="lg"
      alignItems="center"
      className={styles.connectionTimelineEventItem__container}
    >
      <ConnectionTimelineEventIcon isLast={isLast} eventType={event.eventType} />
      <FlexItem grow>
        <ConnectionTimelineEventBody eventSummary={event.eventSummary} eventType={event.eventType} />
      </FlexItem>
      <FlexContainer alignItems="center" direction="row" className={styles.connectionTimelineEventItem__endContent}>
        <Text color="grey400" size="lg">
          <FormattedDate value={event.timestamp * 1000} month="short" day="numeric" year="numeric" />
          {"  "}
          <FormattedTime value={event.timestamp * 1000} />
        </Text>
        <div>
          <Button variant="clear" icon="options" />
        </div>
      </FlexContainer>
    </FlexContainer>
  );
};
