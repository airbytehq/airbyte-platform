import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { PageViewContainer } from "components/common/PageViewContainer";
import { SetupForm } from "components/settings/SetupForm";
import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import useWorkspace from "hooks/services/useWorkspace";

export const SetupPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.PREFERENCES);

  const { setInitialSetupConfig } = useWorkspace();

  return (
    <PageViewContainer>
      <HeadTitle titles={[{ id: "preferences.headTitle" }]} />
      <Heading as="h1" size="lg" centered>
        <FormattedMessage id="preferences.title" />
      </Heading>
      <Box mt="2xl">
        <SetupForm onSubmit={setInitialSetupConfig} />
      </Box>
    </PageViewContainer>
  );
};
