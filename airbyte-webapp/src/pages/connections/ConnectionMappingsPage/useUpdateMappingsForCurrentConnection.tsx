import isBoolean from "lodash/isBoolean";
import { useIntl } from "react-intl";

import { useDestinationDefinitionVersion, useGetConnection, useGetStateTypeQuery } from "core/api";
import { AirbyteStreamAndConfiguration, ConfiguredStreamMapper } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ModalResult, useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

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
              mappers: updatedMappings[streamDescriptorKey],
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
      } else {
        return Promise.reject();
      }
      return Promise.resolve();
    }

    try {
      if (!destinationSupportsRefreshes) {
        const stateType = await getStateType(connection.connectionId);
        const result = await openModal<boolean>({
          title: formatMessage({ id: "connection.clearDataRecommended" }),
          size: "md",
          content: (props) => <ClearDataWarningModal {...props} stateType={stateType} />,
        });
        await handleModalResult(result);
      } else {
        const result = await openModal<boolean>({
          title: formatMessage({ id: "connection.refreshDataRecommended" }),
          size: "md",
          content: ({ onCancel, onComplete }) => <RecommendRefreshModal onCancel={onCancel} onComplete={onComplete} />,
        });
        await handleModalResult(result);
      }
    } catch (e) {
      throw new Error(e);
    }
  };

  return { updateMappings };
};
