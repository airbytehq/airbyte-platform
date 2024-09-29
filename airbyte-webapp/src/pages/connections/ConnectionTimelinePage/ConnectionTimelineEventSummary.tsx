import { PropsWithChildren } from "react";

import styles from "./ConnectionTimelineEventSummary.module.scss";

export const ConnectionTimelineEventSummary: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.eventSummary}>{children}</div>;
};
