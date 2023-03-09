import { faChevronLeft } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
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
      icon={<FontAwesomeIcon icon={faChevronLeft} />}
      onClick={() => {
        navigate(-1);
      }}
    >
      <FormattedMessage id="connectorBuilder.backButtonLabel" />
    </Button>
  );
};
