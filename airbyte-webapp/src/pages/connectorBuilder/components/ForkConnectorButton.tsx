import { useCallback } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Link } from "components/ui/Link";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useCreateSourceDefForkedBuilderProject, useGetBuilderProjectIdByDefinitionId } from "core/api";
import { ConnectorBuilderProjectIdWithWorkspaceId, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import styles from "./ForkInBuilderButton.module.scss";
import { isBuilderCompatible } from "./useBuilderCompatibleSourceDefinitions";
import { getEditPath } from "../ConnectorBuilderRoutes";

export const ForkConnectorButton = ({ sourceDefinition }: { sourceDefinition: SourceDefinitionRead }) => {
  const createLink = useCurrentWorkspaceLink();
  const { data: builderProjectIdResponse } = useGetBuilderProjectIdByDefinitionId(sourceDefinition?.sourceDefinitionId);
  const builderProjectId = builderProjectIdResponse?.builderProjectId;

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

  if (builderProjectId) {
    return (
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
