import { FlexContainer, FlexItem } from "components/ui/Flex";
import { HeadTitle } from "components/ui/HeadTitle";

import styles from "./SettingsLayout.module.scss";

export const SettingsLayoutContent: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <FlexItem grow className={styles.settings__content}>
      {children}
    </FlexItem>
  );
};

export const SettingsLayout: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }]} />
      <FlexContainer direction="column" gap="none" className={styles.settings}>
        <main className={styles.settings__main}>{children} </main>
      </FlexContainer>
    </>
  );
};
