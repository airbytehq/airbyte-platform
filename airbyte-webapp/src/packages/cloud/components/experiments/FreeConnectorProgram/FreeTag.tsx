import { useIntl } from "react-intl";

import { useFreeConnectorProgram } from "core/api/cloud";
import { ReleaseStage } from "core/request/AirbyteClient";

import styles from "./FreeTag.module.scss";
import { freeReleaseStages } from "./lib/model";

interface FreeTagProps {
  releaseStage: ReleaseStage;
}

// A tag labeling a release stage pill as free. Defined here for easy reuse between the
// two release stage pill implementations (which should likely be refactored!)
export const FreeTag: React.FC<FreeTagProps> = ({ releaseStage }) => {
  const { programStatusQuery } = useFreeConnectorProgram();
  const { isEnrolled } = programStatusQuery.data || {};
  const { formatMessage } = useIntl();

  return isEnrolled && freeReleaseStages.includes(releaseStage) ? (
    <span className={styles.freeTag}>{formatMessage({ id: "freeConnectorProgram.releaseStageBadge.free" })}</span>
  ) : null;
};
