import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";

import useWorkspace from "hooks/services/useWorkspace";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import AccountForm from "./components/AccountForm";
import { Content, SettingsCard } from "../SettingsComponents";

const AccountPage: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const { updatePreferences } = useWorkspace();

  const onSubmit = async (data: { email: string }) => {
    await updatePreferences({
      ...workspace,
      ...data,
      news: !!workspace.news,
      anonymousDataCollection: !!workspace.anonymousDataCollection,
      securityUpdates: !!workspace.securityUpdates,
    });
  };

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.account" }]} />
      <SettingsCard title={<FormattedMessage id="settings.accountSettings" />}>
        <Content>
          <AccountForm email={workspace.email ?? ""} onSubmit={onSubmit} />
        </Content>
      </SettingsCard>
    </>
  );
};

export default AccountPage;
