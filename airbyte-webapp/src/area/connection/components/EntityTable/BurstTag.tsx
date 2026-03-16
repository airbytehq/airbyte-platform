import { useIntl } from "react-intl";

import { TagBadge } from "components/ui/TagBadge";

import styles from "./BurstTag.module.scss";

export const BurstTag: React.FC = () => {
  const { formatMessage } = useIntl();

  return <TagBadge className={styles.burstTag} text={formatMessage({ id: "connection.tag.burst" })} icon="star" />;
};
