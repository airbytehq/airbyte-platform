import React from "react";
import { FormattedMessage } from "react-intl";

import styles from "./NoJobsPlaceholder.module.scss";
import octaviaWorker from "../JobListItem/octavia-worker.png";

/**
 * there are no jobs from dbt airbyte
 */
export const NoJobsPlaceholder: React.FC = () => (
  <>
    <img src={octaviaWorker} alt="" className={styles.emptyListImage} />
    <FormattedMessage id="connection.dbtCloudJobs.noJobs" />
  </>
);
