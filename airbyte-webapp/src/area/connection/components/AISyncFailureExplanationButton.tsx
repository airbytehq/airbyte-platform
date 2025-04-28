import { PropsWithChildren } from "react";

import { Button, ButtonProps } from "components/ui/Button";

type AISyncFailureExplanationButtonProps = Omit<ButtonProps, "variant" | "icon">;

export const AISyncFailureExplanationButton: React.FC<PropsWithChildren<AISyncFailureExplanationButtonProps>> = ({
  children,
  ...props
}) => {
  return (
    <Button {...props} icon="aiStars" variant="magic" data-testid="ai-sync-failure-explanation-button">
      {children}
    </Button>
  );
};
