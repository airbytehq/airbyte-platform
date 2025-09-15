import { zodResolver } from "@hookform/resolvers/zod";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate } from "react-router-dom";

import { FormChangeTracker } from "components/forms/FormChangeTracker";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { ScrollParent } from "components/ui/ScrollParent";
import { Text } from "components/ui/Text";

import { useIsDataActivationConnection } from "area/connection/utils/useIsDataActivationConnection";
import { StreamMappings } from "area/dataActivation/components/ConnectionForm/StreamMappings";
import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import {
  DataActivationConnectionFormOutput,
  DataActivationConnectionFormSchema,
  createFormDefaultValues,
} from "area/dataActivation/utils";
import { createSyncCatalogFromFormValues } from "area/dataActivation/utils/createSyncCatalogFromFormValues";
import { useDestinationCatalogByConnectionId, useCurrentConnection, useUpdateConnection } from "core/api";
import { links } from "core/utils/links";
import { useNotificationService } from "hooks/services/Notification";
import { ConnectionRoutePaths } from "pages/routePaths";

export const EditDataActivationMappingsPageWrapper = () => {
  const isDataActivationConnection = useIsDataActivationConnection();

  // Should only happen if someone tries to access the /data-activation-mappings URL directly for a non data activation connection
  if (!isDataActivationConnection) {
    return <Navigate to={`../${ConnectionRoutePaths.Mappings}`} replace />;
  }

  return <EditDataActivationMappingsPage />;
};

export const EditDataActivationMappingsPage = () => {
  const connection = useCurrentConnection();
  const methods = useForm<DataActivationConnectionFormValues, unknown, DataActivationConnectionFormOutput>({
    defaultValues: createFormDefaultValues(connection.syncCatalog),
    mode: "onBlur",
    resolver: zodResolver(DataActivationConnectionFormSchema),
  });
  const { mutateAsync: updateConnection } = useUpdateConnection();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const destinationCatalog = useDestinationCatalogByConnectionId(connection.connectionId);

  const onSubmit = async (values: DataActivationConnectionFormOutput) => {
    const mappedStreams = DataActivationConnectionFormSchema.parse({ streams: values.streams });
    const updatedCatalog = createSyncCatalogFromFormValues(mappedStreams, connection.syncCatalog);
    try {
      await updateConnection({
        connectionId: connection.connectionId,
        syncCatalog: updatedCatalog,
        // Refreshes or clears do not make sense in the context of data activation, so we set skipReset to true
        skipReset: true,
      });
      registerNotification({
        id: "data_activation_connection_update_success",
        text: formatMessage({ id: "form.changesSaved" }),
        type: "success",
      });
      methods.reset(mappedStreams);
    } catch (e) {
      registerNotification({
        id: "data_activation_connection_update_success",
        text: formatMessage({ id: "connection.updateFailed" }),
        type: "error",
      });
    }
  };

  return (
    <ScrollParent>
      <FormProvider {...methods}>
        <FormChangeTracker formId="edit-data-activation-mappings" changed={methods.formState.isDirty} />
        <form onSubmit={methods.handleSubmit(onSubmit)}>
          <FlexContainer direction="column" gap="lg">
            <FlexContainer justifyContent="space-between" alignItems="flex-start">
              <FlexContainer direction="column" gap="lg">
                <Heading as="h1">
                  <FormattedMessage id="connections.mappings.title" />
                </Heading>
                <Text>
                  <FormattedMessage
                    id="connection.dataActivationDescription"
                    values={{
                      destinationName: connection.destination.name,
                      sourceName: connection.source.name,
                      bold: (children) => (
                        <Text as="span" bold>
                          {children}
                        </Text>
                      ),
                    }}
                  />
                </Text>
              </FlexContainer>
              <FlexItem noShrink>
                <FlexContainer alignItems="center" gap="xl">
                  <Text>
                    <ExternalLink href={links.dataActivationDocs}>
                      <FormattedMessage id="connections.mappings.docsLink" /> <Icon type="share" size="xs" />
                    </ExternalLink>
                  </Text>

                  <FormSubmissionButtons
                    submitKey="connection.dataActivation.save"
                    cancelKey="form.reset"
                    allowInvalidSubmit
                  />
                </FlexContainer>
              </FlexItem>
            </FlexContainer>
            <StreamMappings
              destination={connection.destination}
              destinationCatalog={destinationCatalog.catalog}
              source={connection.source}
              sourceCatalog={connection.syncCatalog}
            />
          </FlexContainer>
        </form>
      </FormProvider>
    </ScrollParent>
  );
};
