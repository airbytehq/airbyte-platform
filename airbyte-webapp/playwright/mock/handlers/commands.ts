import type { HandlerMap } from "./handler";
import type { Store } from "../store";

import { notFound } from "./handler";

/**
 * The server promotes a draft source or destination when the webapp reads a succeeded check of it
 * (CommandService.kt). A source or destination that is not a draft does not change.
 */
function promoteDraft(store: Store, actorId: string) {
  if (store.sources.find(actorId)?.isDraft) {
    store.sources.update(actorId, { isDraft: false });
  }
  if (store.destinations.find(actorId)?.isDraft) {
    store.destinations.update(actorId, { isDraft: false });
  }
}

/**
 * Connection checks. The webapp starts a check, polls its status every 2 seconds until it is done, then reads the
 * output. mock/commands.ts keeps the checks and ends a check that the test does not hold. mock/lifecycle.ts lets a
 * test hold a check and end it.
 */
export const commandHandlers: HandlerMap = {
  runCheckCommand: ({ body, store }) => ({ id: store.commands.startCheck(body).id }),

  getCommandStatus: ({ body, store }) => {
    const command = store.commands.readStatus(body.id);
    return command ? { id: command.id, status: command.status } : notFound();
  },

  getCheckCommandOutput: ({ body, store }) => {
    const command = store.commands.find(body.id);
    if (!command) {
      return notFound();
    }
    if (!command.output) {
      throw new Error(`The webapp read the output of check ${body.id} before the check was done`);
    }
    if (command.output.status === "succeeded" && command.request.actor_id) {
      promoteDraft(store, command.request.actor_id);
    }
    return { id: command.id, ...command.output };
  },

  cancelCommand: ({ body, store }) => (store.commands.cancel(body.id) ? { id: body.id } : notFound()),
};
