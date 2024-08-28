import { useIntl } from "react-intl";

import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { Breadcrumbs } from "components/ui/Breadcrumbs";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";

import styles from "./SettingsLayout.module.scss";

export const SettingsLayoutContent: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <FlexItem grow className={styles.settings__content}>
      {children}
    </FlexItem>
  );
};

export const SettingsLayout: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { name: workspaceName } = useCurrentWorkspace();
  const organization = useCurrentOrganizationInfo();
  const multiWorkspaceUi = useFeature(FeatureItem.MultiWorkspaceUI);

  const breadcrumbs = [
    { label: formatMessage({ id: "sidebar.settings" }) },
    ...(multiWorkspaceUi ? [{ label: organization.organizationName }, { label: workspaceName }] : []),
  ];

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }]} />
      <FlexContainer direction="column" gap="none" className={styles.settings}>
        <Box px="xl" className={styles.settings__breadcrumbs}>
          <Breadcrumbs data={breadcrumbs} />
        </Box>
        <main className={styles.settings__main}>{children} </main>
      </FlexContainer>
    </>
  );
};
