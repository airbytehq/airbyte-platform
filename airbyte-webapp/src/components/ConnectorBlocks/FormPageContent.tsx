import classNames from "classnames";
import { PropsWithChildren } from "react";

import { useExperiment } from "hooks/services/Experiment";
import { isCloudApp } from "utils/app";

import styles from "./FormPageContent.module.scss";

const FormPageContent: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const isNewFlowActive = useExperiment("connection.updatedConnectionFlow.selectConnectors", false);

  return (
    <div
      className={classNames(styles.container, {
        [styles.cloud]: isCloudApp(),
        [styles.wide]: isNewFlowActive,
      })}
    >
      {children}
    </div>
  );
};

export default FormPageContent;
