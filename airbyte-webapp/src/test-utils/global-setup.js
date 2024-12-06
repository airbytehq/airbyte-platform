/* eslint-disable import/no-anonymous-default-export */
import { generateIntents } from "../../packages/vite-plugins";

export default async () => {
  // We need to run the generate intent plugin manually, since Jest doesn't
  // run Vite plugins, and otherwise those types would be missing in some tested files.
  const plugin = generateIntents();
  console.log(`\n\nRun ${plugin.name} Vite plugin...`);
  plugin.buildStart();
  // Timezone setup as described here https://stackoverflow.com/a/56482581
  process.env.TZ = "US/Pacific";
};
