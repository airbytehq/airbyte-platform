import React, { ReactNode } from "react";

import styles from "./Details.module.scss";
import { Collapsible } from "../../Collapsible";

export const Details = ({ children }: { children: ReactNode; className?: string }) => {
  // Filter out whitespace-only strings so the <details> element's <summary> recognition
  // doesn't break based on html formatting
  const detailsChildren = React.Children.toArray(children).filter(
    (child) => typeof child !== "string" || !/^\s*$/.test(child)
  );
  const [firstChild, ...restChildren] = detailsChildren;

  let collapsibleChildren = detailsChildren;
  let summaryText = "";

  if (React.isValidElement(firstChild) && firstChild.type === "summary") {
    summaryText = firstChild.props.children;
    collapsibleChildren = restChildren;
  }

  return (
    <Collapsible className={styles.details} buttonClassName={styles.detailsButton} label={summaryText}>
      {collapsibleChildren}
    </Collapsible>
  );
};
