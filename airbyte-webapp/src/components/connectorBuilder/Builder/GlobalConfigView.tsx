import { useIntl } from "react-intl";

import { useConnectorBuilderPermission } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import styles from "./GlobalConfigView.module.scss";
export const GlobalConfigView: React.FC = () => {
  const { formatMessage } = useIntl();
  const permission = useConnectorBuilderPermission();

  // TODO(lmossman): implement global view in follow-up PR
  return (
    <fieldset className={styles.fieldset} disabled={permission === "readOnly"}>
      <BuilderConfigView heading={formatMessage({ id: "connectorBuilder.globalConfiguration" })} />
    </fieldset>
  );
};
