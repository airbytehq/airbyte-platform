import type { HandlerMap } from "./handler";

import { builderHandlers } from "./builder";
import { commandHandlers } from "./commands";
import { connectionHandlers } from "./connections";
import { connectorHandlers } from "./connectors";
import { identityHandlers } from "./identity";
import { jobHandlers } from "./jobs";
import { mapperHandlers } from "./mappers";

/**
 * The closed set of operations the mock answers. A request for any other operation fails the test.
 * Add a handler here only when a converted test needs the operation.
 */
export const defaultHandlers: HandlerMap = {
  ...identityHandlers,
  ...connectorHandlers,
  ...connectionHandlers,
  ...mapperHandlers,
  ...commandHandlers,
  ...jobHandlers,
  ...builderHandlers,
};
