import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import styles from "./BackButton.module.scss";

export const BackButton = () => {
  const navigate = useNavigate();
  return (
    <Button
      className={styles.button}
      variant="clear"
      icon={<Icon type="chevronLeft" size="lg" />}
      onClick={() => {
        navigate(-1);
      }}
    >
      <FormattedMessage id="connectorBuilder.backButtonLabel" />
    </Button>
  );
};
