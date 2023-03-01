/* eslint-disable css-modules/no-unused-class */
import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { isSourceDefinition } from "core/domain/connector/source";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import styles from "./ResourceNotAvailable.module.scss";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
console.log(styles);

interface ResourceNotAvailableProps {
  activeTab: "erd" | "schema";
  isRequested: boolean;
  setRequested: (val: boolean) => void;
}
export const ResourceNotAvailable: React.FC<React.PropsWithChildren<ResourceNotAvailableProps>> = ({
  activeTab,
  setRequested,
  isRequested,
}) => {
  const { selectedConnectorDefinition } = useDocumentationPanelContext();
  const { trackRequest } = useAnalyticsTrackFunctions();

  return (
    <FlexContainer
      direction="column"
      alignItems="center"
      justifyContent="center"
      gap="lg"
      className={classNames(styles.requestContainer, styles.requestContainer__previewImage, {
        [styles["requestContainer__previewImage--erd"]]: activeTab === "erd",
        [styles["requestContainer__previewImage--schema"]]: activeTab === "schema",
      })}
    >
      {isRequested ? (
        <Text size="lg">
          <FormattedMessage id="sources.request.thankYou" />
        </Text>
      ) : (
        <FlexContainer
          direction="column"
          alignItems="center"
          justifyContent="center"
          className={styles.requestContainer__cta}
        >
          <Text size="lg">
            <FormattedMessage id={`sources.request.prioritize.${activeTab}`} />
          </Text>

          <Button
            variant="primary"
            onClick={() => {
              if (selectedConnectorDefinition && isSourceDefinition(selectedConnectorDefinition)) {
                trackRequest({
                  sourceDefinitionId: selectedConnectorDefinition.sourceDefinitionId,
                  connectorName: selectedConnectorDefinition.name,
                  requestType: activeTab,
                });
              }
              setRequested(true);
            }}
          >
            <FormattedMessage id={`sources.request.button.${activeTab}`} />
          </Button>
        </FlexContainer>
      )}
    </FlexContainer>
  );
};
