import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate, useSearchParams } from "react-router-dom";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FormDevTools } from "components/ui/forms/FormDevTools";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { BASIC_FREQUENCY_DEFAULT_VALUE } from "area/connection/components/ConnectionForm/ScheduleFormField/useBasicFrequencyDropdownData";
import { CreateConnectionFlowLayout } from "area/connection/components/CreateConnectionFlowLayout";
import { SimplifiedConnectionsSettingsCard } from "area/connection/components/CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionSettingsCard";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { createSyncCatalogFromFormValues } from "area/dataActivation/utils/createSyncCatalogFromFormValues";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { CreateConnectionProps, useCreateConnection, useDiscoverDestination, useDiscoverSchemaQuery } from "core/api";
import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { useNotificationService } from "core/services/Notification";
import { FormModeProvider } from "core/services/ui/FormModeContext";
import { trackError } from "core/utils/datadog";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConfigureConnectionRoute.module.scss";
import { useStreamMappings } from "../CreateDataActivationConnectionRoutes";

export const ConfigureDataActivationConnectionPage: React.FC = () => {
  const { streamMappings } = useStreamMappings();
  const createLink = useCurrentWorkspaceLink();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const { mutateAsync: webBackendCreateConnection, isLoading } = useCreateConnection();
  const { registerNotification } = useNotificationService();
  const source = useGetSourceFromSearchParams();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source);
  const destination = useGetDestinationFromSearchParams();
  const { data: discoveredDestination } = useDiscoverDestination(destination.destinationId);

  const createConnection = async (formValues: FormConnectionFormValues) => {
    if (!sourceSchema) {
      throw new Error("Source schema missing when trying to create connection");
    }
    if (!sourceSchema.catalog) {
      throw new Error("Source schema catalog missing when trying to create connection");
    }
    if (!streamMappings) {
      throw new Error("Stream mappings missing when trying to create connection");
    }

    const webBackendConnectionCreate: CreateConnectionProps = {
      values: {
        ...formValues,
        syncCatalog: createSyncCatalogFromFormValues(streamMappings, sourceSchema.catalog),
      },
      source,
      destination,
      sourceDefinition: {
        sourceDefinitionId: source.sourceDefinitionId,
      },
      destinationDefinition: {
        name: destination.name,
        destinationDefinitionId: destination.destinationDefinitionId,
      },
      sourceCatalogId: sourceSchema.catalogId,
      destinationCatalogId: discoveredDestination?.catalogId,
      isDataActivationConnection: true,
    };

    try {
      const res = await webBackendCreateConnection(webBackendConnectionCreate);
      navigate(createLink(`/${RoutePaths.Connections}/${res.connectionId}`));
    } catch (e) {
      trackError(e);
      registerNotification({
        id: "connection.createConnectionError",
        text: formatMessage({ id: "connection.create.error" }, { message: e.message }),
        type: "error",
      });
    }
  };

  const methods = useForm<FormConnectionFormValues>({
    defaultValues: {
      // Sync catalog is intentionally empty here. We will construct it on save based on the mapped streams configured
      // in the previous step.
      syncCatalog: {
        streams: [],
      },
      name: `${source.name} → ${destination.name}`,
      scheduleType: ConnectionScheduleType.basic,
      scheduleData: {
        basicSchedule: BASIC_FREQUENCY_DEFAULT_VALUE,
      },
    },
    resolver: zodResolver(zodSchema),
    mode: "onChange",
  });

  // Dummy partialConnection so we can use the ConnectionFormServiceProvider. We only need that context to use the same
  // error message logic as the ConnectionForm with getErrorMessage()
  const partialConnection = {
    source,
    destination,
    syncCatalog: { streams: [] },
  };

  return (
    <FormModeProvider mode="create">
      <FormProvider {...methods}>
        <ConnectionFormServiceProvider connection={partialConnection} refreshSchema={() => Promise.resolve()}>
          <form onSubmit={methods.handleSubmit(createConnection)} className={styles.form}>
            <CreateConnectionFlowLayout.Main>
              <Box p="xl">
                <SimplifiedConnectionsSettingsCard
                  title={formatMessage({ id: "connectionForm.configureConnection" })}
                  source={source}
                  destination={destination}
                  isCreating
                />
              </Box>
              <FormDevTools />
            </CreateConnectionFlowLayout.Main>
            <CreateConnectionFlowLayout.Footer>
              <Footer isLoading={isLoading} />
            </CreateConnectionFlowLayout.Footer>
          </form>
        </ConnectionFormServiceProvider>
      </FormProvider>
    </FormModeProvider>
  );
};

const zodSchema = z.object({
  name: z.string().trim().nonempty({ message: "form.empty.error" }),
});

const Footer = ({ isLoading }: { isLoading: boolean }) => {
  const [searchParams] = useSearchParams();

  const createLink = useCurrentWorkspaceLink();
  const linkBackToMapStreams = createLink(
    `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${
      ConnectionRoutePaths.ConfigureDataActivation
    }?${searchParams.toString()}`
  );
  const { isSubmitting, isValid } = useFormState<FormConnectionFormValues>();

  return (
    <>
      <Link variant="button" to={linkBackToMapStreams}>
        <FormattedMessage id="connection.create.backToMappings" />
      </Link>
      <FormErrors />
      <Button type="submit" isLoading={isLoading} disabled={isSubmitting || !isValid}>
        <FormattedMessage id="connection.save" />
      </Button>
    </>
  );
};

const FormErrors = () => {
  const { isDirty, isValid, errors } = useFormState<FormConnectionFormValues>();
  const { getErrorMessage } = useConnectionFormService();
  const errorMessage = getErrorMessage(isValid, errors);

  return (
    <Text color="red" size="lg">
      {isDirty && errorMessage}
    </Text>
  );
};
