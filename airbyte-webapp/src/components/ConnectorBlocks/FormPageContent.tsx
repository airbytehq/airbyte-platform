import { PropsWithChildren } from "react";

import styles from "./FormPageContent.module.scss";

const FormPageContent: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  return <div className={styles.container}>{children}</div>;
};

export default FormPageContent;
