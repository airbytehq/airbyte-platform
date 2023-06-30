import classnames from "classnames";
import React from "react";

import { ArrowRightIcon } from "components/icons/ArrowRightIcon";
import { Heading } from "components/ui/Heading";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./StreamsConfigTableConnectorHeader.module.scss";
import { ConnectorHeader } from "../ConnectorHeader";

export const StreamsConfigTableConnectorHeader: React.FC = () => {
  const {
    connection: { source, destination },
  } = useConnectionFormService();
  const sourceStyles = classnames(styles.connector, styles.source);

  return (
    <div className={classnames(styles.container)}>
      <div className={sourceStyles}>
        <Heading as="h5" size="sm">
          <ConnectorHeader type="source" icon={source.icon} />
        </Heading>
      </div>
      <div className={styles.destination}>
        <div className={styles.arrowContainer}>
          <ArrowRightIcon />
        </div>
        <div className={styles.connector}>
          <Heading as="h5" size="sm">
            <ConnectorHeader type="destination" icon={destination.icon} />
          </Heading>
        </div>
      </div>
    </div>
  );
};
