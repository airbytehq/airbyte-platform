// This file should contain all stateful modification that need to be made to libraries.
// In general this is a bad pattern that should try to be avoided, but some libraries will
// require plugins to be registered to their global instance. This file encapsulates all those
// stateful modifications.

import dayjs from "dayjs";
import arraySupport from "dayjs/plugin/arraySupport";
import customParseFormat from "dayjs/plugin/customParseFormat";
import duration from "dayjs/plugin/duration";
import isBetween from "dayjs/plugin/isBetween";
import relativeTime from "dayjs/plugin/relativeTime";
import toArray from "dayjs/plugin/toArray";
import utc from "dayjs/plugin/utc";

// Configure dayjs instance
dayjs.extend(customParseFormat);
dayjs.extend(utc);
dayjs.extend(relativeTime);
dayjs.extend(duration);
dayjs.extend(toArray);
dayjs.extend(arraySupport);
dayjs.extend(isBetween);
