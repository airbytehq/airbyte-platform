import { lazy, Suspense } from "react";

import { isCloudApp } from "core/utils/app";
import { InviteUsersHintProps } from "packages/cloud/views/users/InviteUsersHint";

const LazyInviteUsersHint = lazy(() =>
  import("packages/cloud/views/users/InviteUsersHint").then(({ InviteUsersHint }) => ({ default: InviteUsersHint }))
);

export const CloudInviteUsersHint: React.VFC<InviteUsersHintProps> = (props) =>
  isCloudApp() ? (
    <Suspense fallback={null}>
      <LazyInviteUsersHint {...props} />
    </Suspense>
  ) : null;
