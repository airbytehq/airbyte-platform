import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/HeadTitle";
import { PageViewContainer } from "components/PageViewContainer";
import { SetupForm } from "components/settings/SetupForm";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

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
