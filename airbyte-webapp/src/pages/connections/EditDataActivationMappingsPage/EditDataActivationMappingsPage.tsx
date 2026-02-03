import { zodResolver } from "@hookform/resolvers/zod";
import isEqual from "lodash/isEqual";
import { useCallback, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { FormChangeTracker } from "components/ui/forms/FormChangeTracker";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { ScrollParent } from "components/ui/ScrollParent";
import { Text } from "components/ui/Text";

import { useIsDataActivationConnection } from "area/connection/utils/useIsDataActivationConnection";
import { StreamMappings } from "area/dataActivation/components/ConnectionForm/StreamMappings";
import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import {
  DataActivationConnectionFormOutput,
  DataActivationConnectionFormSchema,
  EMPTY_STREAM,
  createFormDefaultValues,
} from "area/dataActivation/utils";
import { createSyncCatalogFromFormValues } from "area/dataActivation/utils/createSyncCatalogFromFormValues";
import { getDestinationOperationFields } from "area/dataActivation/utils/getDestinationOperationFields";
import {
  useDestinationCatalogByConnectionId,
  useCurrentConnection,
  useUpdateConnection,
  useDiscoverSourceSchemaMutation,
  useDiscoverDestinationSchemaMutation,
} from "core/api";
import { AirbyteCatalog, DestinationCatalog } from "core/api/types/AirbyteClient";
import { ModalContentProps, useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { links } from "core/utils/links";
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
    mode: "onChange",
    resolver: zodResolver(DataActivationConnectionFormSchema),
  });
  const { mutateAsync: updateConnection } = useUpdateConnection();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const savedDestinationDiscover = useDestinationCatalogByConnectionId(connection.connectionId);
  const { openModal } = useModalService();

  const { mutateAsync: refreshDestinationCatalog, isLoading: isRefreshingDestinationCatalog } =
    useDiscoverDestinationSchemaMutation();
  const [refreshedDestinationCatalogId, setRefreshedDestinationCatalogId] = useState<string | undefined>();
  const [refreshedDestinationCatalog, setRefreshedDestinationCatalog] = useState<DestinationCatalog | undefined>();
  const destinationCatalog = refreshedDestinationCatalog ?? savedDestinationDiscover.catalog;

  const onRefreshDestinationCatalog = useCallback(async () => {
    if (methods.formState.isDirty) {
      const result = await openModal({
        title: formatMessage({ id: "connection.dataActivation.refreshCatalogDirtyFormChangesTitle" }),
        content: RefreshCatalogConfirmationModal,
      });
      if (result.type === "canceled") {
        return;
      }
      methods.reset();
    }

    const currentValues = methods.getValues();
    let newDestinationCatalog: DestinationCatalog | undefined;
    let newDestinationCatalogId: string | undefined;

    try {
      const { catalog, catalogId } = await refreshDestinationCatalog({
        destinationId: connection.destination.destinationId,
      });
      newDestinationCatalog = catalog;
      newDestinationCatalogId = catalogId;
      registerNotification({
        id: "data_activation_destination_refresh_success",
        text: formatMessage({ id: "connection.dataActivation.destinationSchemaRefreshSuccess" }),
        type: "success",
      });
    } catch (e) {
      registerNotification({
        id: "data_activation_destination_refresh_error",
        text: formatMessage({ id: "connection.dataActivation.destinationSchemaRefreshError" }),
        type: "error",
      });
      return;
    }

    setRefreshedDestinationCatalog(newDestinationCatalog);
    setRefreshedDestinationCatalogId(newDestinationCatalogId);

    currentValues.streams.forEach((stream, streamIndex) => {
      const destinationOperation = newDestinationCatalog?.operations.find(
        (operation) =>
          operation.objectName === stream.destinationObjectName && operation.syncMode === stream.destinationSyncMode
      );
      if (!destinationOperation) {
        // This is a quick and safe way to reset the form in case the destination object name or sync mode changes.
        // This will remove all mapped fields, which may be kind of annoying. A potentially better UX would be to
        // allow the user to pick one of the available operations in the refreshed catalog and attempt to remap fields
        // that are still available. Punting on this for now since destination object renames should be pretty rare.
        methods.reset({ streams: [EMPTY_STREAM] });
        return;
      }

      const availableDestinationFields = getDestinationOperationFields(destinationOperation).map(([key]) => key);
      stream.fields.forEach((field, fieldIndex) => {
        if (!availableDestinationFields.includes(field.destinationFieldName)) {
          methods.setValue(`streams.${streamIndex}.fields.${fieldIndex}.destinationFieldName`, "", {
            shouldValidate: true,
          });
        }
      });

      // In this case, thew new catalog does not have any matching keys for this operation, so we should null out the
      // value
      if (!destinationOperation.matchingKeys || destinationOperation.matchingKeys.length === 0) {
        methods.setValue(`streams.${streamIndex}.matchingKeys`, null, { shouldValidate: true });
      }

      // If the selected matching key has been removed from the options, clear the value but do not set it to null,
      // prompting the user to select a new one
      if (
        stream.matchingKeys &&
        !destinationOperation.matchingKeys?.some((matchingKeys) => isEqual(matchingKeys, stream.matchingKeys))
      ) {
        methods.setValue(`streams.${streamIndex}.matchingKeys`, [], { shouldValidate: true });
      }
    });
    // Trigger validation in case the schema refresh caused some form values to become invalid
    methods.trigger();
  }, [
    connection.destination.destinationId,
    formatMessage,
    methods,
    openModal,
    refreshDestinationCatalog,
    registerNotification,
  ]);

  const { mutateAsync: refreshSourceSchema, isLoading: isRefreshingSourceSchema } = useDiscoverSourceSchemaMutation(
    connection.source
  );
  const [refreshedSourceCatalogId, setRefreshedSourceCatalogId] = useState<string | undefined>();
  const [refreshedSourceCatalog, setRefreshedSourceCatalog] = useState<AirbyteCatalog | undefined>();
  const sourceCatalog = refreshedSourceCatalog ?? connection.syncCatalog;
  const onRefreshSourceSchema = useCallback(async () => {
    if (methods.formState.isDirty) {
      const result = await openModal({
        title: formatMessage({ id: "connection.dataActivation.refreshCatalogDirtyFormChangesTitle" }),
        content: RefreshCatalogConfirmationModal,
      });
      if (result.type === "canceled") {
        return;
      }
      methods.reset();
    }

    const currentValues = methods.getValues();
    let newSourceCatalog: AirbyteCatalog | undefined;
    let newSourceCatalogId: string | undefined;

    try {
      const { catalog, catalogId } = await refreshSourceSchema();
      newSourceCatalog = catalog;
      newSourceCatalogId = catalogId;
      registerNotification({
        id: "data_activation_source_refresh_success",
        text: formatMessage({ id: "connection.dataActivation.sourceSchemaRefreshSuccess" }),
        type: "success",
      });
    } catch (e) {
      registerNotification({
        id: "data_activation_source_refresh_error",
        text: formatMessage({ id: "connection.dataActivation.sourceSchemaRefreshError" }),
        type: "error",
      });
      return;
    }

    setRefreshedSourceCatalog(newSourceCatalog);
    setRefreshedSourceCatalogId(newSourceCatalogId);

    // Validate the new catalog against the current form values and make any adjustments to keep the form valid.
    currentValues.streams.forEach((stream, streamIndex) => {
      const streamInRefreshedCatalog = newSourceCatalog?.streams.find(
        (s) =>
          s.stream?.name === stream.sourceStreamDescriptor.name &&
          s.stream?.namespace === stream.sourceStreamDescriptor.namespace
      );

      if (!streamInRefreshedCatalog) {
        // This is a quick and safe way to reset the form in case the source stream name changes. This will remove all
        // mapped fields, which may be kind of annoying. A potentially better UX would be to allow the user to pick one
        // of the available streams in the refreshed catalog and attempt to remap fields that are still available.
        // Punting on this for now since source stream renames should be pretty rare.
        methods.reset({ streams: [EMPTY_STREAM] });
        return;
      }

      // We need to check that all the source field names being used still exist in the refreshed catalog. If they don't
      // we reset them to an empty string, which will force the user to select a new source field name.
      const availableFields = Object.keys(streamInRefreshedCatalog?.stream?.jsonSchema?.properties ?? {});
      if (stream.cursorField && !availableFields.includes(stream.cursorField)) {
        methods.setValue(`streams.${streamIndex}.cursorField`, "");
      }

      const newFields = stream.fields.map((field) => {
        if (!availableFields.includes(field.sourceFieldName)) {
          return {
            ...field,
            sourceFieldName: "",
          };
        }
        return field;
      });

      methods.setValue(`streams.${streamIndex}.fields`, newFields);
    });
    // Trigger validation in case the schema refresh caused some form values to become invalid
    methods.trigger();
  }, [formatMessage, methods, openModal, refreshSourceSchema, registerNotification]);

  const onSubmit = async (values: DataActivationConnectionFormOutput) => {
    const mappedStreams = DataActivationConnectionFormSchema.parse({ streams: values.streams });
    const updatedCatalog = createSyncCatalogFromFormValues(mappedStreams, sourceCatalog);

    await updateConnection({
      connectionId: connection.connectionId,
      ...(refreshedSourceCatalogId ? { sourceCatalogId: refreshedSourceCatalogId } : {}),
      ...(refreshedDestinationCatalogId ? { destinationCatalogId: refreshedDestinationCatalogId } : {}),
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
              destinationCatalog={destinationCatalog}
              source={connection.source}
              sourceCatalog={sourceCatalog}
              onRefreshSourceSchema={onRefreshSourceSchema}
              onRefreshDestinationCatalog={onRefreshDestinationCatalog}
              disabled={isRefreshingSourceSchema || isRefreshingDestinationCatalog}
            />
          </FlexContainer>
        </form>
      </FormProvider>
    </ScrollParent>
  );
};

const RefreshCatalogConfirmationModal: React.FC<ModalContentProps<unknown>> = ({ onCancel, onComplete }) => (
  <>
    <ModalBody>
      <Text>
        <FormattedMessage id="connection.dataActivation.refreshCatalogDirtyFormChanges" />
      </Text>
    </ModalBody>
    <ModalFooter>
      <Button variant="secondary" type="button" onClick={onCancel}>
        <FormattedMessage id="form.cancel" />
      </Button>
      <Button type="button" onClick={() => onComplete(undefined)}>
        <FormattedMessage id="form.discardChanges" />
      </Button>
    </ModalFooter>
  </>
);
