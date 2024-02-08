import classNames from "classnames";
import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { SyncCatalogCard } from "components/connection/ConnectionForm/SyncCatalogCard";
import { SOURCE_ID_PARAM } from "components/connection/CreateConnection/SelectSource";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";

import { useGetSourceFromSearchParams } from "area/connector/utils";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./SimplifiedConnectionConfiguration.module.scss";
import { SimplifiedDestinationNamespaceFormField } from "./SimplifiedDestinationNamespaceFormField";
import { SimplifiedSchemaQuestionnaire } from "./SimplifiedSchemaQuestionnaire";
import { SimplifiedSyncModeCard } from "./SimplifiedSyncModeCard";

export const SimplifiedConnectionConfiguration: React.FC = () => {
  const [currentStep] = useState<"schema" | "connection">("schema");
  const createLink = useCurrentWorkspaceLink();
  const source = useGetSourceFromSearchParams();

  return (
    <>
      {currentStep === "schema" ? (
        <>
          <SimplifiedConnectionCreationSchemaConfig />
          <SimplifiedConnectionCreationSchemaStreams />
        </>
      ) : (
        <SimplifiedConnectionCreationConfigureConnection />
      )}
      <FlexContainer>
        <Link
          to={{
            pathname: createLink(`/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`),
            search: `${SOURCE_ID_PARAM}=${source.sourceId}`,
          }}
          className={classNames(styles.button, styles.typeSecondary, styles.sizeS, styles.linkText)}
        >
          <FormattedMessage id="connectionForm.backToDefineDestination" />
        </Link>
      </FlexContainer>
    </>
  );
};

const SimplifiedConnectionCreationSchemaConfig: React.FC = () => {
  return (
    <>
      <Card>
        <Box m="xl">
          <SimplifiedSchemaQuestionnaire />
        </Box>
      </Card>
      <Card>
        <SyncCatalogCard />
      </Card>
    </>
  );
};

const SimplifiedConnectionCreationSchemaStreams: React.FC = () => {
  return null;
};

const SimplifiedConnectionCreationConfigureConnection: React.FC = () => {
  return (
    <Card>
      <SimplifiedDestinationNamespaceFormField />
      <SimplifiedSyncModeCard />
    </Card>
  );
};
