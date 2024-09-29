import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Icon } from "components/ui/Icon";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateDestinationDefinition, useCreateSourceDefinition } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";
import { ConnectorBuilderRoutePaths } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { DestinationPaths, RoutePaths, SourcePaths } from "pages/routePaths";

import { AddCustomDockerImageConnectorModal } from "./AddCustomDockerImageConnectorModal";

interface AddNewConnectorButtonProps {
  type: "sources" | "destinations";
}

interface ConnectorDefinitionProps {
  name: string;
  documentationUrl: string;
  dockerImageTag: string;
  dockerRepository: string;
}

export const AddNewConnectorButton: React.FC<AddNewConnectorButtonProps> = ({ type }) => {
  const { formatMessage } = useIntl();
  const hasUploadCustomConnectorPermissions = useGeneratedIntent(Intent.UploadCustomConnector);
  const allowUploadCustomDockerImage =
    useFeature(FeatureItem.AllowUploadCustomImage) && hasUploadCustomConnectorPermissions;
  const navigate = useNavigate();
  const workspaceId = useCurrentWorkspaceId();
  const { openModal } = useModalService();

  const { mutateAsync: createSourceDefinition } = useCreateSourceDefinition();
  const { mutateAsync: createDestinationDefinition } = useCreateDestinationDefinition();

  const onSubmitSource = async (sourceDefinition: ConnectorDefinitionProps) => {
    const result = await createSourceDefinition(sourceDefinition);

    navigate({
      pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${SourcePaths.SelectSourceNew}/${result.sourceDefinitionId}`,
    });
  };

  const onSubmitDestination = async (destinationDefinition: ConnectorDefinitionProps) => {
    const result = await createDestinationDefinition(destinationDefinition);

    navigate({
      pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${DestinationPaths.SelectDestinationNew}/${result.destinationDefinitionId}`,
    });
  };

  const onSubmit = (values: ConnectorDefinitionProps) =>
    type === "sources" ? onSubmitSource(values) : onSubmitDestination(values);

  const openAddCustomDockerImageConnectorModal = () =>
    openModal<void>({
      title: formatMessage({ id: "admin.addNewConnector" }),
      content: ({ onComplete, onCancel }) => (
        <AddCustomDockerImageConnectorModal
          onCancel={onCancel}
          onSubmit={async (values: ConnectorDefinitionProps) => {
            await onSubmit(values);
            onComplete();
          }}
        />
      ),
    });

  if (type === "destinations" && !allowUploadCustomDockerImage) {
    return null;
  }

  return (
    <>
      {type === "destinations" && allowUploadCustomDockerImage ? (
        <Button size="xs" icon="plus" onClick={openAddCustomDockerImageConnectorModal}>
          <FormattedMessage id="admin.newConnector" />
        </Button>
      ) : (
        <DropdownMenu
          placement="bottom-end"
          options={[
            {
              as: "a",
              href: `../../${RoutePaths.ConnectorBuilder}/${ConnectorBuilderRoutePaths.Create}`,
              icon: <Icon type="wrench" />,
              displayName: formatMessage({ id: "admin.newConnector.build" }),
              internal: true,
            },
            ...(allowUploadCustomDockerImage
              ? [
                  {
                    as: "button" as const,
                    icon: <Icon type="docker" />,
                    value: "docker",
                    displayName: formatMessage({ id: "admin.newConnector.docker" }),
                  },
                ]
              : []),
          ]}
          onChange={(data: DropdownMenuOptionType) =>
            data.value === "docker" && openAddCustomDockerImageConnectorModal()
          }
        >
          {() => (
            <Button size="xs" icon="plus">
              <FormattedMessage id="admin.newConnector" />
            </Button>
          )}
        </DropdownMenu>
      )}
    </>
  );
};
