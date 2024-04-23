import classNames from "classnames";
import { useState } from "react";
import { Location, useLocation, useNavigate } from "react-router-dom";
import { useEffectOnce } from "react-use";

import styles from "./InputContainer.module.scss";

export interface LocationWithState extends Location {
  state: { action?: "scheduleType" };
}

export const InputContainer: React.FC<React.PropsWithChildren<{ highlightAfterRedirect?: boolean }>> = ({
  children,
  highlightAfterRedirect,
}) => {
  const [highlighted, setHighlighted] = useState(false);
  const navigate = useNavigate();
  const { state: locationState, pathname } = useLocation() as LocationWithState;

  useEffectOnce(() => {
    let highlightTimeout: number;

    if (highlightAfterRedirect && locationState?.action === "scheduleType") {
      // remove the redirection info from the location state
      navigate(pathname, { replace: true });

      setHighlighted(true);
      highlightTimeout = window.setTimeout(() => {
        setHighlighted(false);
      }, 1500);
    }

    return () => {
      window.clearTimeout(highlightTimeout);
    };
  });

  return <div className={classNames(styles.container, { [styles.highlighted]: highlighted })}>{children}</div>;
};
