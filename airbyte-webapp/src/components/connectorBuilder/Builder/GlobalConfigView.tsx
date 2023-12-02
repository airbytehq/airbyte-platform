import { useIntl } from "react-intl";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

import { AuthenticationSection } from "./AuthenticationSection";
import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderTitle } from "./BuilderTitle";
import styles from "./GlobalConfigView.module.scss";

export const GlobalConfigView: React.FC = () => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();

  return (
    <BuilderConfigView heading={formatMessage({ id: "connectorBuilder.globalConfiguration" })}>
      {/* Not using intl for the labels and tooltips in this component in order to keep maintainence simple */}
      <BuilderTitle
        path="name"
        label={formatMessage({ id: "connectorBuilder.globalConfiguration.connectorName" })}
        size="lg"
      />
      <BuilderCard className={styles.content}>
        <BuilderFieldWithInputs
          type="string"
          manifestPath="HttpRequester.properties.url_base"
          path="formValues.global.urlBase"
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
  );
};
