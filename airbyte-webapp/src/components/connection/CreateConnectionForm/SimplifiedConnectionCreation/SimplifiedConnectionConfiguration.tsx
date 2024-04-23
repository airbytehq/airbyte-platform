import classNames from "classnames";
import React from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { Route, Routes } from "react-router-dom";
import { useEffectOnce, useMount } from "react-use";

import { CreateConnectionFormControls } from "components/connection/ConnectionForm/CreateConnectionFormControls";
import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { SyncCatalogCard } from "components/connection/ConnectionForm/SyncCatalogCard";
import { DESTINATION_ID_PARAM } from "components/connection/CreateConnection/SelectDestination";
import { SOURCE_ID_PARAM } from "components/connection/CreateConnection/SelectSource";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./SimplifiedConnectionConfiguration.module.scss";
import { SimplifiedConnectionsSettingsCard } from "./SimplifiedConnectionSettingsCard";
import { SimplifiedSchemaQuestionnaire } from "./SimplifiedSchemaQuestionnaire";
import { CREATE_CONNECTION_FORM_ID } from "../CreateConnectionForm";

export const SimplifiedConnectionConfiguration: React.FC = () => {
  return (
    <Routes>
      <Route
        index
        element={
          <>
            <SimplifiedConnectionCreationReplication />
            <FirstNav />
          </>
        }
      />
      <Route
        path={ConnectionRoutePaths.ConfigureContinued}
        element={
          <>
            <SimplifiedConnectionCreationConfigureConnection />
            <SecondNav />
          </>
        }
      />
    </Routes>
  );
};

const SimplifiedConnectionCreationReplication: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_SELECT_STREAMS);
  const { formatMessage } = useIntl();
  const { isDirty } = useFormState<FormConnectionFormValues>();
  const { trackFormChange } = useFormChangeTrackerService();

  // if the user is navigating back from the second step the form may be dirty
  useMount(() => {
    trackFormChange(CREATE_CONNECTION_FORM_ID, isDirty);
  });

  return (
    <>
      <Card
        title={formatMessage({ id: "connectionForm.selectSyncMode" })}
        helpText={formatMessage({ id: "connectionForm.selectSyncModeDescription" })}
      >
        <SimplifiedSchemaQuestionnaire />
      </Card>
      <SyncCatalogCard />
    </>
  );
};

const SimplifiedConnectionCreationConfigureConnection: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_CONFIGURE_CONNECTION);
  const { formatMessage } = useIntl();
  const { isDirty } = useFormState<FormConnectionFormValues>();
  const { trackFormChange } = useFormChangeTrackerService();

  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();

  // if the user is navigating from the first step the form may be dirty
  useMount(() => {
    trackFormChange(CREATE_CONNECTION_FORM_ID, isDirty);
  });

  return (
    <SimplifiedConnectionsSettingsCard
      title={formatMessage({ id: "connectionForm.configureConnection" })}
      source={source}
      destination={destination}
      isCreating
    />
  );
};

const FirstNav: React.FC = () => {
  const createLink = useCurrentWorkspaceLink();
  const destination = useGetDestinationFromSearchParams();
  const source = useGetSourceFromSearchParams();

  const { isValid, errors } = useFormState<FormConnectionFormValues>();
  const { trigger } = useFormContext<FormConnectionFormValues>();
  const { getErrorMessage } = useConnectionFormService();
  const errorMessage = getErrorMessage(isValid, errors);

  const { clearFormChange } = useFormChangeTrackerService();

  // it's possible to navigate to the settings step, cause a form error, and navigate back to this step
  // we want to prevent moving to the next step when there is a streams error, not other parts of the form
  const canProceed = !Boolean(errors?.syncCatalog?.streams);

  // If the source doesn't select any streams by default, the initial untouched state
  // won't validate that at least one is selected. In this case, a user could submit the form
  // without selecting any streams, which would trigger an error and cause a lousy UX.
  useEffectOnce(() => {
    trigger("syncCatalog.streams");
  });

  return (
    <FlexContainer justifyContent="space-between">
      <Link
        to={{
          pathname: createLink(`/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`),
          search: `?${SOURCE_ID_PARAM}=${source.sourceId}`,
        }}
        className={classNames(styles.linkText)}
      >
        <FormattedMessage id="connectionForm.backToDefineDestination" />
      </Link>
      <div>
        {errorMessage && !canProceed /* if the error message applies to this view */ && (
          <Box as="span" mr="lg">
            <Text color="red" size="lg" as="span">
              {errorMessage}
            </Text>
          </Box>
        )}
        {canProceed ? (
          <Link
            to={{
              pathname: createLink(
                `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}/${ConnectionRoutePaths.ConfigureContinued}`
              ),
              search: `?${SOURCE_ID_PARAM}=${source.sourceId}&${DESTINATION_ID_PARAM}=${destination.destinationId}`,
            }}
            className={classNames(styles.nextLink)}
            onClick={() => {
              // we're navigating to the next step which retains the creation form's state
              clearFormChange(CREATE_CONNECTION_FORM_ID);
            }}
          >
            <FormattedMessage id="connectionForm.nextButton" />
          </Link>
        ) : (
          <Button disabled>
            <FormattedMessage id="connectionForm.nextButton" />
          </Button>
        )}
      </div>
    </FlexContainer>
  );
};

const SecondNav: React.FC = () => {
  const createLink = useCurrentWorkspaceLink();
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();
  const { clearFormChange } = useFormChangeTrackerService();

  return (
    <FlexContainer justifyContent="space-between">
      <Link
        to={{
          pathname: createLink(
            `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}`
          ),
          search: `?${SOURCE_ID_PARAM}=${source.sourceId}&${DESTINATION_ID_PARAM}=${destination.destinationId}`,
        }}
        className={classNames(styles.linkText)}
        onClick={() => {
          // we're navigating to the previous step which retains the creation form's state
          clearFormChange(CREATE_CONNECTION_FORM_ID);
        }}
      >
        <FormattedMessage id="connectionForm.backToSetupSchema" />
      </Link>
      <CreateConnectionFormControls />
    </FlexContainer>
  );
};
