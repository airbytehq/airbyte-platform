import { FlexContainer, FlexItem } from "components/ui/Flex";
import { HeadTitle } from "components/ui/HeadTitle";

import styles from "./SettingsLayout.module.scss";

interface SettingsLayoutProps extends React.PropsWithChildren {
  titleId?: string;
}

export const SettingsLayoutContent: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <FlexItem grow className={styles.settings__content}>
      {children}
    </FlexItem>
  );
};

export const SettingsLayout: React.FC<SettingsLayoutProps> = ({ children, titleId = "sidebar.settings" }) => {
  return (
    <>
      <HeadTitle titles={[{ id: titleId }]} />
      <FlexContainer direction="column" gap="none" className={styles.settings}>
        <main className={styles.settings__main}>{children} </main>
      </FlexContainer>
    </>
  );
};
