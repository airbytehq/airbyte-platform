import { useIntl } from "react-intl";

import { AssistButton } from "components/connectorBuilder/Builder/Assist/AssistButton";

import { Namespace, Action, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { AuthenticationSection } from "./AuthenticationSection";
import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
import styles from "./GlobalConfigView.module.scss";

export const GlobalConfigView: React.FC = () => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const { permission } = useConnectorBuilderFormState();

  return (
    <fieldset className={styles.fieldset} disabled={permission === "readOnly"}>
      <BuilderConfigView heading={formatMessage({ id: "connectorBuilder.globalConfiguration" })}>
        <BuilderCard>
          <BuilderField
            type="jinja"
            manifestPath="HttpRequester.properties.url_base"
            path="formValues.global.urlBase"
            labelAction={<AssistButton assistKey="urlbase" />}
            onBlur={(value: string) => {
              if (value) {
                analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.API_URL_CREATE, {
                  actionDescription: "Base API URL filled in",
                  api_url: value,
                });
              }
            }}
          />
        </BuilderCard>
        <AuthenticationSection />
      </BuilderConfigView>
    </fieldset>
  );
};
