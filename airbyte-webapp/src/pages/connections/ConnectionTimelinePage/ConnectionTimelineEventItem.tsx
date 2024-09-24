import { PropsWithChildren } from "react";

import { FlexContainer } from "components/ui/Flex";

import styles from "./ConnectionTimelineEventItem.module.scss";

export const ConnectionTimelineEventItem: React.FC<PropsWithChildren> = ({ children }) => {
  return (
    <FlexContainer direction="row" gap="lg" className={styles.connectionTimelineEventItem__container}>
      {children}
    </FlexContainer>
  );
};
