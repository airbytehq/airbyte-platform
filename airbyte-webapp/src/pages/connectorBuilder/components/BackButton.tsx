import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";

import styles from "./BackButton.module.scss";

export const BackButton = () => {
  const navigate = useNavigate();
  return (
    <Button
      className={styles.button}
      variant="clear"
      icon="chevronLeft"
      iconSize="lg"
      onClick={() => {
        navigate(-1);
      }}
    >
      <FormattedMessage id="connectorBuilder.backButtonLabel" />
    </Button>
  );
};
