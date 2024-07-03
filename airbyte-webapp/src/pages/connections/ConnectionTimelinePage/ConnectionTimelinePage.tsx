import { FormattedMessage } from "react-intl";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import { ConnectionTimelineItemList } from "./ConnectionTimelineItemList";
import { mockConnectionTimelineEventList } from "./mocks";

export const ConnectionTimelinePage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TIMELINE);
  const { events } = mockConnectionTimelineEventList;

  return (
    <PageContainer centered>
      <ConnectionSyncContextProvider>
        <Card noPadding>
          <Box p="xl">
            <FlexContainer direction="column">
              <FlexContainer justifyContent="space-between" alignItems="center">
                <Heading as="h5" size="sm">
                  <FormattedMessage id="connection.timeline" />
                </Heading>
              </FlexContainer>
              <Box p="lg">
                <FlexContainer alignItems="center">filters go here</FlexContainer>
              </Box>
            </FlexContainer>
          </Box>
          <ConnectionTimelineItemList timelineItems={events} />
        </Card>
      </ConnectionSyncContextProvider>
    </PageContainer>
  );
};
