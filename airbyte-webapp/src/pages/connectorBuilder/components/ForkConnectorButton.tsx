import { useCallback } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Link } from "components/ui/Link";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceLink, useWorkspaceLink } from "area/workspace/utils";
import { useCreateSourceDefForkedBuilderProject, useGetBuilderProjectIdByDefinitionId } from "core/api";
import { ConnectorBuilderProjectIdWithWorkspaceId, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { RoutePaths } from "pages/routePaths";

import styles from "./ForkInBuilderButton.module.scss";
import { isBuilderCompatible } from "./useBuilderCompatibleSourceDefinitions";
import { getEditPath } from "../ConnectorBuilderRoutes";

const EditBuilderProjectButton = ({
  builderProjectId,
  builderProjectWorkspaceId,
  sourceDefinition,
}: {
  builderProjectId: string;
  builderProjectWorkspaceId: string;
  sourceDefinition: SourceDefinitionRead;
}) => {
  // Check if the user has permission to edit the project in the builder workspace,
  // custom connectors are scoped to the Organization, but Builder projects are scoped to the Workspace.
  const canEditInBuilder = useGeneratedIntent(Intent.CreateOrEditConnectorBuilder, {
    workspaceId: builderProjectWorkspaceId,
  });
  const createWorkspaceLink = useWorkspaceLink(builderProjectWorkspaceId);
  const createProjectEditLink = useCallback(
    (projectId: string) => createWorkspaceLink(`/${RoutePaths.ConnectorBuilder}/${getEditPath(projectId)}`),
    [createWorkspaceLink]
  );

  return canEditInBuilder ? (
    <Tooltip
      control={
        // Explicitly render a Button instead of using Link variant="button" to ensure that the styles stay
        // consistent with the non-Link buttons below
        <Link to={createProjectEditLink(builderProjectId)} opensInNewTab>
          <InnerButton label="edit" />
        </Link>
      }
      placement="bottom"
    >
      <FormattedMessage id="onboarding.sourceSetUp.editInBuilder.tooltip" values={{ name: sourceDefinition.name }} />
    </Tooltip>
  ) : (
    <Tooltip control={<InnerButton label="edit" disabled />} placement="bottom">
      <FormattedMessage
        id="onboarding.sourceSetUp.editInBuilder.tooltip.forbidden"
        values={{ workspace: builderProjectWorkspaceId }}
      />
    </Tooltip>
  );
};

export const ForkConnectorButton = ({ sourceDefinition }: { sourceDefinition: SourceDefinitionRead }) => {
  const createLink = useCurrentWorkspaceLink();
  const { data: builderProjectIdResponse } = useGetBuilderProjectIdByDefinitionId(sourceDefinition?.sourceDefinitionId);
  const builderProjectId = builderProjectIdResponse?.builderProjectId;
  const builderProjectWorkspaceId = builderProjectIdResponse?.workspaceId;

  const createProjectEditLink = useCallback(
    (projectId: string) => createLink(`/${RoutePaths.ConnectorBuilder}/${getEditPath(projectId)}`),
    [createLink]
  );

  const { mutateAsync: createForkedProject, isLoading: isForking } = useCreateSourceDefForkedBuilderProject();
  const forkAndOpenInBuilder = useCallback(() => {
    createForkedProject(sourceDefinition.sourceDefinitionId).then(
      (result: ConnectorBuilderProjectIdWithWorkspaceId) => {
        window.open(createProjectEditLink(result.builderProjectId), "_blank");
      }
    );
  }, [createForkedProject, createProjectEditLink, sourceDefinition]);

  if (!sourceDefinition) {
    return null;
  }

  if (builderProjectId && builderProjectWorkspaceId) {
    return (
      <EditBuilderProjectButton
        builderProjectId={builderProjectId}
        builderProjectWorkspaceId={builderProjectWorkspaceId}
        sourceDefinition={sourceDefinition}
      />
    );
  }

  if (isBuilderCompatible(sourceDefinition)) {
    return (
      <Tooltip
        control={<InnerButton label="fork" onClick={forkAndOpenInBuilder} isLoading={isForking} />}
        placement="bottom"
      >
        <FormattedMessage
          id="onboarding.sourceSetUp.forkInBuilder.tooltip.compatible"
          values={{ name: sourceDefinition.name }}
        />
      </Tooltip>
    );
  }

  return (
    <Tooltip control={<InnerButton label="fork" disabled />} placement="bottom">
      <FormattedMessage
        id="onboarding.sourceSetUp.forkInBuilder.tooltip.incompatible"
        values={{ name: sourceDefinition.name }}
      />
    </Tooltip>
  );
};

const InnerButton = ({
  label,
  onClick,
  isLoading,
  disabled,
}: {
  label: "fork" | "edit";
  onClick?: () => void;
  isLoading?: boolean;
  disabled?: boolean;
}) => (
  <Button
    className={styles.button}
    variant="secondary"
    icon="export"
    iconSize="sm"
    onClick={onClick}
    isLoading={isLoading}
    disabled={disabled || isLoading}
    type="button"
  >
    <FormattedMessage
      id={
        label === "fork" ? "onboarding.sourceSetUp.forkInBuilder.button" : "onboarding.sourceSetUp.editInBuilder.button"
      }
    />
  </Button>
);
