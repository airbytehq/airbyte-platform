import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Icon } from "components/ui/Icon";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreateDestinationDefinition, useCreateSourceDefinition } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectorBuilderRoutePaths } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { DestinationPaths, RoutePaths, SourcePaths } from "pages/routePaths";

import CreateConnectorModal from "./CreateConnectorModal";

interface IProps {
  type: "sources" | "destinations";
}

interface ICreateProps {
  name: string;
  documentationUrl: string;
  dockerImageTag: string;
  dockerRepository: string;
}

const CreateConnector: React.FC<IProps> = ({ type }) => {
  const navigate = useNavigate();
  const workspaceId = useCurrentWorkspaceId();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const onChangeModalState = () => {
    setIsModalOpen(!isModalOpen);
  };
  const allowUploadCustomImage = useFeature(FeatureItem.AllowUploadCustomImage);

  const { formatMessage } = useIntl();

  const { mutateAsync: createSourceDefinition } = useCreateSourceDefinition();

  const { mutateAsync: createDestinationDefinition } = useCreateDestinationDefinition();

  const onSubmitSource = async (sourceDefinition: ICreateProps) => {
    const result = await createSourceDefinition(sourceDefinition);

    navigate({
      pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${SourcePaths.SelectSourceNew}/${result.sourceDefinitionId}`,
    });
  };

  const onSubmitDestination = async (destinationDefinition: ICreateProps) => {
    const result = await createDestinationDefinition(destinationDefinition);

    navigate({
      pathname: `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${DestinationPaths.SelectDestinationNew}/${result.destinationDefinitionId}`,
    });
  };

  const onSubmit = (values: ICreateProps) =>
    type === "sources" ? onSubmitSource(values) : onSubmitDestination(values);

  if (type === "destinations" && !allowUploadCustomImage) {
    return null;
  }

  return (
    <>
      {type === "destinations" && allowUploadCustomImage ? (
        <NewConnectorButton onClick={onChangeModalState} />
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
            ...(allowUploadCustomImage
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
          onChange={(data: DropdownMenuOptionType) => data.value === "docker" && onChangeModalState()}
        >
          {() => <NewConnectorButton />}
        </DropdownMenu>
      )}

      {isModalOpen && <CreateConnectorModal onClose={onChangeModalState} onSubmit={onSubmit} />}
    </>
  );
};

interface NewConnectorButtonProps {
  onClick?: () => void;
}

const NewConnectorButton = React.forwardRef<HTMLButtonElement, NewConnectorButtonProps>(({ onClick }, ref) => {
  return (
    <Button size="xs" icon={<Icon type="plus" />} onClick={onClick} ref={ref}>
      <FormattedMessage id="admin.newConnector" />
    </Button>
  );
});
NewConnectorButton.displayName = "NewConnectorButton";

export default CreateConnector;
