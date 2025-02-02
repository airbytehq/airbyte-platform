import React from "react";
import { useIntl } from "react-intl";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import styles from "./ComponentsView.module.scss";
import { CustomComponentsEditor } from "../CustomComponentsEditor/CustomComponentsEditor";

export const ComponentsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const { permission } = useConnectorBuilderFormState();

  return (
    <fieldset className={styles.fieldset} disabled={permission === "readOnly"}>
      <BuilderConfigView
        heading={formatMessage({ id: "connectorBuilder.customComponents" })}
        className={styles.fullHeight}
      >
        <CustomComponentsEditor />
      </BuilderConfigView>
    </fieldset>
  );
};
