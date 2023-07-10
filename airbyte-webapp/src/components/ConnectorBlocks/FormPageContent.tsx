import classNames from "classnames";
import { PropsWithChildren } from "react";

import { isCloudApp } from "core/utils/app";

import styles from "./FormPageContent.module.scss";

const FormPageContent: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <div
      className={classNames(styles.container, {
        [styles.cloud]: isCloudApp(),
      })}
    >
      {children}
    </div>
  );
};

export default FormPageContent;
