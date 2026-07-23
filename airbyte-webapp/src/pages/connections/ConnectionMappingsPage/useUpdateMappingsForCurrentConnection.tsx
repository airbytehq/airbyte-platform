import isBoolean from "lodash/isBoolean";
import { useIntl } from "react-intl";

import { useConnectionEditService } from "area/connection/utils/ConnectionEdit/ConnectionEditService";
import { useDestinationDefinitionVersion, useGetConnection, useGetStateTypeQuery } from "core/api";
import { AirbyteStreamAndConfiguration, ConfiguredStreamMapper } from "core/api/types/AirbyteClient";
import { ModalResult, useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { trackError } from "core/utils/datadog";

import { getKeyForStream } from "./MappingContext";
import { ClearDataWarningModal } from "../ConnectionReplicationPage/ClearDataWarningModal";
import { RecommendRefreshModal } from "../ConnectionReplicationPage/RecommendRefreshModal";

export const useUpdateMappingsForCurrentConnection = (connectionId: string) => {
  const connection = useGetConnection(connectionId);
  const { supportsRefreshes: destinationSupportsRefreshes } = useDestinationDefinitionVersion(
    connection.destination.destinationId
  );
  const { registerNotification } = useNotificationService();
  const getStateType = useGetStateTypeQuery();
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();
  const { updateConnection } = useConnectionEditService();

  const updateMappings = async (updatedMappings: Record<string, ConfiguredStreamMapper[]>) => {
    const updatedCatalog: AirbyteStreamAndConfiguration[] = connection.syncCatalog.streams.map(
      (streamWithConfig: AirbyteStreamAndConfiguration) => {
        if (streamWithConfig.stream && streamWithConfig.config) {
          const streamDescriptorKey = getKeyForStream(streamWithConfig.stream);
          return {
            ...streamWithConfig,
            config: {
              ...streamWithConfig.config,
              mappers:
                updatedMappings[streamDescriptorKey] && updatedMappings[streamDescriptorKey].length > 0
                  ? updatedMappings[streamDescriptorKey]
                  : undefined,
              // We should explicitly remove hashedFields, since the mappers field is now the source of truth. The
              // backend may re-populate hashedFields in the response, but we don't want to send conflicting information.
              hashedFields: undefined,
            },
          };
        }

        return streamWithConfig;
      }
    );

    async function handleModalResult(result: ModalResult<boolean>) {
      if (result.type === "completed" && isBoolean(result.reason)) {
        await updateConnection({
          connectionId: connection.connectionId,
          syncCatalog: {
            streams: updatedCatalog,
          },
          skipReset: !result.reason,
        })
          .then(() => {
            registerNotification({
              id: "connection_settings_change_success",
              text: formatMessage({ id: "form.changesSaved" }),
              type: "success",
            });
          })
          .catch((e: Error) => {
            trackError(e, { connectionName: connection.name });
            registerNotification({
              id: "connection_settings_change_error",
              text: formatMessage({ id: "connection.updateFailed" }),
              type: "error",
            });
          });
        return { success: true, skipped: false };
      }
      return { success: false, skipped: true };
    }

    try {
      if (!destinationSupportsRefreshes) {
        const stateType = await getStateType(connection.connectionId);
        const result = await openModal<boolean>({
          title: formatMessage({ id: "connection.clearDataRecommended" }),
          size: "md",
          content: (props) => <ClearDataWarningModal {...props} stateType={stateType} />,
        });
        return await handleModalResult(result);
      }
      const result = await openModal<boolean>({
        title: formatMessage({ id: "connection.refreshDataRecommended" }),
        size: "md",
        content: ({ onCancel, onComplete }) => <RecommendRefreshModal onCancel={onCancel} onComplete={onComplete} />,
      });
      return await handleModalResult(result);
    } catch (e) {
      throw new Error(e);
    }
  };

  return { updateMappings };
};
