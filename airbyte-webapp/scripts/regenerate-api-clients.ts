import { statSync, existsSync } from "fs";
import { resolve, basename, join } from "path";

import chalk from "chalk";

import conf from "../orval.config";

const lastTouched = (filepath: string) => statSync(resolve(filepath)).mtime;

for (const api of Object.values(conf)) {
  if (
    process.env.CI ||
    !existsSync(api.output.target) ||
    !existsSync(api.output.target.replace(/\.ts$/, ".schemas.ts")) ||
    !existsSync(join(api.output.target, "../../types", basename(api.output.target))) ||
    lastTouched(api.input) > lastTouched(api.output.target) ||
    lastTouched(api.input) > lastTouched(api.output.target.replace(/\.ts$/, ".schemas.ts")) ||
    lastTouched(resolve(__dirname, "../orval.config.ts")) > lastTouched(api.output.target)
  ) {
    // no need to read and parse the orval lib if all files are up to date
    require("orval").generate(api);
  } else {
    console.log(`🎉 ${chalk.green(basename(api.output.target))} is up-to-date`);
  }
}
