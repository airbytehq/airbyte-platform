import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";

import AccountForm from "./components/AccountForm";

/**
 * OSS Account Settings section
 */
export const AccountPage: React.FC = () => (
  <>
    <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.account" }]} />
    <Card title={<FormattedMessage id="settings.accountSettings" />}>
      <Box p="xl">
        <AccountForm />
      </Box>
    </Card>
  </>
);
