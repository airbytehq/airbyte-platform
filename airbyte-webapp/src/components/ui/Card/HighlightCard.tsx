import { PropsWithChildren } from "react";

import { Card } from "./Card";
import styles from "./HighlightCard.module.scss";

export const HighlightCard: React.FC<PropsWithChildren> = ({ children, ...restProps }) => {
  return (
    <Card className={styles.highlightCard__card} bodyClassName={styles.highlightCard__cardBody} {...restProps}>
      {children}
    </Card>
  );
};
