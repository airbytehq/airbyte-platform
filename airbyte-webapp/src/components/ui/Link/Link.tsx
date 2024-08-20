import { ComponentProps, PropsWithChildren, forwardRef } from "react";
// eslint-disable-next-line no-restricted-imports
import { Link as ReactRouterLink, To } from "react-router-dom";

import { getLinkClassNames } from "./getLinkClassNames";

export interface LinkProps {
  className?: string;
  opensInNewTab?: boolean;
  variant?: "default" | "primary" | "button";
  onClick?: ComponentProps<typeof ReactRouterLink>["onClick"];
  title?: string;
}

interface InternalLinkProps extends LinkProps {
  to: To;
}

export const Link = forwardRef<HTMLAnchorElement, PropsWithChildren<InternalLinkProps>>(
  ({ children, className, to, opensInNewTab = false, variant = "default", ...props }, ref) => {
    return (
      <ReactRouterLink
        ref={ref}
        {...props}
        className={getLinkClassNames({ className, variant })}
        target={opensInNewTab ? "_blank" : undefined}
        to={to}
      >
        {children}
      </ReactRouterLink>
    );
  }
);
Link.displayName = "Link";
