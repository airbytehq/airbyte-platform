import { PropsWithChildren } from "react";

import { FlexContainer } from "components/ui/Flex";

import styles from "./ConnectionTimelineEventItem.module.scss";
export const ConnectionTimelineEventItem: React.FC<PropsWithChildren<{ centered?: boolean }>> = ({
  centered,
  children,
}) => {
  return (
    <FlexContainer
      direction="row"
      gap="lg"
      className={styles.connectionTimelineEventItem__container}
      alignItems={centered ? "center" : undefined}
    >
      {children}
    </FlexContainer>
  );
};
