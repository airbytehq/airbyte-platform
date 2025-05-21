import classNames from "classnames";
import React, { useCallback, useEffect } from "react";

import styles from "./Toast.module.scss";
import { Message, MessageProps } from "../Message";

export interface ToastProps extends MessageProps {
  timeout?: boolean;
}

export const Toast: React.FC<ToastProps> = ({ timeout, ...props }) => {
  const progressBarRef = React.useRef<HTMLDivElement>(null);
  const changeAnimationState = useCallback(
    (newState: "pause" | "play") => {
      if (!progressBarRef.current || !timeout) {
        return;
      }

      const progressBar = progressBarRef.current;
      const animations = progressBar.getAnimations();

      if (animations.length > 0) {
        const animation = animations[0];

        if (animation.playState === "finished") {
          return;
        }

        if (newState === "pause") {
          animation.pause();
        } else {
          animation.play();
        }
      }
    },
    [timeout]
  );

  useEffect(() => {
    if (!timeout || !progressBarRef.current) {
      return;
    }

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        changeAnimationState("play");
      } else {
        changeAnimationState("pause");
      }
    };

    // Set initial state
    handleVisibilityChange();

    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [changeAnimationState, timeout]);

  return (
    <div
      className={styles.container}
      onMouseEnter={() => changeAnimationState("pause")}
      onMouseLeave={() => changeAnimationState("play")}
    >
      <Message
        {...props}
        className={classNames(props.className, styles.toastMessage)}
        textClassName={styles.toastText}
        header={
          timeout ? (
            <div className={styles.progressBar} ref={progressBarRef} onAnimationEnd={props.onClose} />
          ) : undefined
        }
      />
    </div>
  );
};
