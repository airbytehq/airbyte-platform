import { useIntl } from "react-intl";

import styles from "./ConnectorImage.module.scss";

export const ConnectorImage = () => {
  const { formatMessage } = useIntl();

  /* TODO: replace with uploaded img when that functionality is added */
  return (
    <img
      className={styles.connectorImg}
      src="/logo.png"
      alt={formatMessage({ id: "connectorBuilder.connectorImgAlt" })}
    />
  );
};
