import React, { Suspense } from "react";

import { isDevelopment } from "core/utils/isDevelopment";

const FormDevToolsInternal = React.lazy(() => import("./FormDevToolsInternal"));

export const FormDevTools = isDevelopment()
  ? () => (
      <Suspense fallback={null}>
        <FormDevToolsInternal />
      </Suspense>
    )
  : () => null;
