import React, { Suspense } from "react";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";
import { HeadTitle } from "components/HeadTitle";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import styles from "./OrganizationSettingsLayout.module.scss";

export const OrganizationSettingsLayout: React.FC = () => {
  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }]} />
      <FlexContainer direction="column" gap="none" className={styles.settings}>
        <main className={styles.settings__main}>
          <FlexItem grow className={styles.settings__content}>
            <Suspense fallback={<LoadingPage />}>
              <Outlet />
            </Suspense>
          </FlexItem>
        </main>
      </FlexContainer>
    </>
  );
};

export default OrganizationSettingsLayout;
