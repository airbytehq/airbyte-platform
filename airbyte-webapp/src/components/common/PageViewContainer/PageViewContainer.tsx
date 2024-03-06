import React from "react";

import { Card } from "components/ui/Card";

import { BaseClearView } from "./BaseClearView";
import styles from "./PageViewContainer.module.scss";

export const PageViewContainer: React.FC<React.PropsWithChildren<unknown>> = (props) => (
  <BaseClearView>
    <Card className={styles.card}>{props.children}</Card>
  </BaseClearView>
);
