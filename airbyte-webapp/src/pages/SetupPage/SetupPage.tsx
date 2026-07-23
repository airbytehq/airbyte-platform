import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { HeadTitle } from "components/ui/HeadTitle";
import { PageViewContainer } from "components/ui/PageViewContainer";

import { SetupForm } from "area/settings/components/SetupForm";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

export const SetupPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.PREFERENCES);

  return (
    <PageViewContainer>
      <HeadTitle titles={[{ id: "preferences.headTitle" }]} />
      <FlexContainer justifyContent="center">
        <Heading as="h1" size="lg">
          <FormattedMessage id="preferences.title" />
        </Heading>
      </FlexContainer>
      <Box mt="2xl">
        <SetupForm />
      </Box>
    </PageViewContainer>
  );
};
