import React from "react";

import { isDevelopment } from "utils/isDevelopment";

const FormDevToolsInternal = React.lazy(() => import("./FormDevToolsInternal"));

export const FormDevTools = isDevelopment() ? FormDevToolsInternal : () => null;
