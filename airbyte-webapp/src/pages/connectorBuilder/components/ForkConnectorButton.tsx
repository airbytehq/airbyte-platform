import classNames from "classnames";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

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

const BUTTON_CONTENT = {
  fork: {
    iconOnlyIcon: "fork",
    label: "onboarding.sourceSetUp.forkInBuilder.button",
    iconOnlyLabel: "onboarding.sourceSetUp.forkInBuilder.iconButton",
    tooltip: "onboarding.sourceSetUp.forkInBuilder.tooltip.compatible",
  },
  edit: {
    iconOnlyIcon: "pencil",
    label: "onboarding.sourceSetUp.editInBuilder.button",
    iconOnlyLabel: "onboarding.sourceSetUp.editInBuilder.button",
    tooltip: "onboarding.sourceSetUp.editInBuilder.tooltip",
  },
} as const;

type ButtonAction = keyof typeof BUTTON_CONTENT;

const ActionTooltipContent = ({ action, iconOnly, name }: { action: ButtonAction; iconOnly: boolean; name: string }) =>
  iconOnly ? (
    <FormattedMessage id={BUTTON_CONTENT[action].iconOnlyLabel} />
  ) : (
    <FormattedMessage id={BUTTON_CONTENT[action].tooltip} values={{ name }} />
  );

const EditBuilderProjectButton = ({
  builderProjectId,
  builderProjectWorkspaceId,
  sourceDefinition,
  iconOnly,
}: {
  builderProjectId: string;
  builderProjectWorkspaceId: string;
  sourceDefinition: SourceDefinitionRead;
  iconOnly: boolean;
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
          <InnerButton action="edit" iconOnly={iconOnly} />
        </Link>
      }
      placement="bottom"
    >
      <ActionTooltipContent action="edit" iconOnly={iconOnly} name={sourceDefinition.name} />
    </Tooltip>
  ) : (
    <Tooltip
      control={<InnerButton action="edit" iconOnly={iconOnly} disabled />}
      placement="bottom"
      cursor="not-allowed"
    >
      <FormattedMessage
        id="onboarding.sourceSetUp.editInBuilder.tooltip.forbidden"
        values={{ workspace: builderProjectWorkspaceId }}
      />
    </Tooltip>
  );
};

export const ForkConnectorButton = ({
  sourceDefinition,
  iconOnly = false,
}: {
  sourceDefinition: SourceDefinitionRead;
  iconOnly?: boolean;
}) => {
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
        iconOnly={iconOnly}
      />
    );
  }

  if (isBuilderCompatible(sourceDefinition)) {
    return (
      <Tooltip
        control={<InnerButton action="fork" iconOnly={iconOnly} onClick={forkAndOpenInBuilder} isLoading={isForking} />}
        placement="bottom"
      >
        <ActionTooltipContent action="fork" iconOnly={iconOnly} name={sourceDefinition.name} />
      </Tooltip>
    );
  }

  return (
    <Tooltip
      control={<InnerButton action="fork" iconOnly={iconOnly} disabled />}
      placement="bottom"
      cursor="not-allowed"
    >
      <FormattedMessage
        id="onboarding.sourceSetUp.forkInBuilder.tooltip.incompatible"
        values={{ name: sourceDefinition.name }}
      />
    </Tooltip>
  );
};

const InnerButton = ({
  action,
  iconOnly,
  onClick,
  isLoading,
  disabled,
}: {
  action: ButtonAction;
  iconOnly: boolean;
  onClick?: () => void;
  isLoading?: boolean;
  disabled?: boolean;
}) => {
  const { formatMessage } = useIntl();
  const content = BUTTON_CONTENT[action];

  return (
    <Button
      className={classNames(styles.button, { [styles.iconOnly]: iconOnly })}
      variant="secondary"
      icon={iconOnly ? content.iconOnlyIcon : "export"}
      iconSize="sm"
      aria-label={iconOnly ? formatMessage({ id: content.iconOnlyLabel }) : undefined}
      onClick={onClick}
      isLoading={isLoading}
      disabled={disabled || isLoading}
      type="button"
    >
      {!iconOnly && <FormattedMessage id={content.label} />}
    </Button>
  );
};
