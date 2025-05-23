import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { z } from "zod";

import { FormDevTools } from "components/forms/FormDevTools";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { CreateConnectionProps, useCreateConnection, useDiscoverSchemaQuery } from "core/api";
import { ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { FormModeProvider } from "core/services/ui/FormModeContext";
import { trackError } from "core/utils/datadog";
import {
  ConnectionFormServiceProvider,
  useConnectionFormService,
} from "hooks/services/ConnectionForm/ConnectionFormService";
import { useNotificationService } from "hooks/services/Notification";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConfigureConnectionRoute.module.scss";
import { createSyncCatalogFromMappedStreams } from "./createSyncCatalogFromMappedStreams";
import { StreamMappingsFormValuesSchema } from "./StreamMappings";
import { FormConnectionFormValues } from "../ConnectionForm/formConfig";
import { BASIC_FREQUENCY_DEFAULT_VALUE } from "../ConnectionForm/ScheduleFormField/useBasicFrequencyDropdownData";
import { CreateConnectionFlowLayout } from "../CreateConnectionFlowLayout";
import { SimplifiedConnectionsSettingsCard } from "../CreateConnectionForm/SimplifiedConnectionCreation/SimplifiedConnectionSettingsCard";

export const ConfigureConnectionRoute = () => {
  const location = useLocation();
  const createLink = useCurrentWorkspaceLink();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();
  const { mutateAsync: webBackendCreateConnection, isLoading } = useCreateConnection();
  const { registerNotification } = useNotificationService();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source.sourceId);

  const createConnection = async (formValues: FormConnectionFormValues) => {
    if (!sourceSchema) {
      throw new Error("Source schema missing when trying to create connection");
    }

    const mappedStreams = StreamMappingsFormValuesSchema.parse({ streams: location.state.streams });

    const webBackendConnectionCreate: CreateConnectionProps = {
      values: {
        ...formValues,
        syncCatalog: createSyncCatalogFromMappedStreams(mappedStreams, sourceSchema),
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
  const location = useLocation();
  const { isSubmitting, isValid } = useFormState<FormConnectionFormValues>();
  return (
    <>
      <Link
        to={{
          pathname: createLink(
            `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.ConfigureDataActivation}`
          ),
          search: searchParams.toString(),
        }}
        state={location.state}
        variant="button"
      >
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
