import { ComponentProps, PropsWithChildren, forwardRef } from "react";
// eslint-disable-next-line no-restricted-imports
import { Link as ReactRouterLink, To } from "react-router-dom";

import { getLinkClassNames } from "./getLinkClassNames";

export interface LinkProps {
  className?: string;
  opensInNewTab?: boolean;
  variant?: "default" | "primary" | "button" | "buttonPrimary";
  onClick?: ComponentProps<typeof ReactRouterLink>["onClick"];
  title?: string;
  state?: unknown;
}

interface InternalLinkProps extends LinkProps {
  to: To;
}

export const Link = forwardRef<HTMLAnchorElement, PropsWithChildren<InternalLinkProps>>(
  ({ children, className, to, opensInNewTab = false, variant = "default", state, ...props }, ref) => {
    return (
      <ReactRouterLink
        ref={ref}
        {...props}
        className={getLinkClassNames({ className, variant })}
        target={opensInNewTab ? "_blank" : undefined}
        to={to}
        state={state}
      >
        {children}
      </ReactRouterLink>
    );
  }
);
Link.displayName = "Link";
