import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ExternalLink } from "components/ui/Link";
import { InfoTooltip } from "components/ui/Tooltip";

import { links } from "core/utils/links";
import { useConnectorBuilderPermission } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import styles from "./ComponentsView.module.scss";
import { CustomComponentsEditor } from "../CustomComponentsEditor/CustomComponentsEditor";

export const ComponentsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const permission = useConnectorBuilderPermission();

  return (
    <fieldset className={styles.fieldset} disabled={permission === "readOnly"}>
      <BuilderConfigView
        heading={
          <>
            {formatMessage({ id: "connectorBuilder.customComponents" })}
            <InfoTooltip placement="top">
              <FormattedMessage
                id="connectorBuilder.customComponents.tooltip"
                values={{
                  lnk: (...lnk: React.ReactNode[]) => (
                    <ExternalLink href={links.connectorBuilderCustomComponents}>{lnk}</ExternalLink>
                  ),
                }}
              />
            </InfoTooltip>
          </>
        }
        className={styles.fullHeight}
      >
        <CustomComponentsEditor />
      </BuilderConfigView>
    </fieldset>
  );
};
