import { faCog } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";

import { Link } from "components/ui/Link";

import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConnectionSettingsCell.module.scss";

interface IProps {
  id: string;
}

const ConnectorCell: React.FC<IProps> = ({ id }) => {
  const { workspaceId } = useCurrentWorkspace();

  const openSettings = (event: React.MouseEvent) => {
    event.stopPropagation();
  };

  const settingPath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${id}/${ConnectionRoutePaths.Replication}`;

  return (
    <button className={styles.button} onClick={openSettings} tabIndex={-1}>
      <Link className={styles.link} to={settingPath}>
        <FontAwesomeIcon className={styles.icon} icon={faCog} />
      </Link>
    </button>
  );
};

export default ConnectorCell;
