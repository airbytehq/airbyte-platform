import { Impact, PageStatus } from "../instatus/types";

export const ACTIVEINCIDENTS_COLORS: { [key in Impact]: { color: string; background: string } } = {
  MAJOROUTAGE: {
    color: "hsl(335, 100%, 30%)", // --color-red-900
    background: "hsl(350, 100%, 86%)", // --color-red-100
  },
  PARTIALOUTAGE: {
    color: "hsl(11, 70%, 51%)",
    background: "hsl(11, 97%, 87%)", // --color-orange-100
  },
  DEGRADEDPERFORMANCE: {
    color: "hsl(27, 100%, 48%)", // --color-yellow-900
    background: "hsl(48, 90%, 84%)", //  --color-yellow-100
  },
} as const;

export const COLORS: { [key in Partial<PageStatus>]: { color: string; background: string } } = {
  UP: {
    color: "hsl(180, 100%, 17%)", // --color-green-900
    background: "hsl(183, 64%, 79%)", // --color-green-100
  },
  UNDERMAINTENANCE: {
    color: "hsl(240, 12%, 40%)", // --color-grey-700
    background: "hsl(240, 12%, 92%)", //  --color-grey-100
  },
  HASISSUES: {
    color: "hsl(335, 100%, 30%)", // --color-red-900
    background: "hsl(350, 100%, 86%)", // --color-red-100
  },
} as const;

export const ACTIONS = {
  contactFormShown: "Contact Form Shown",
  helpCenterShown: "Help Center Shown",
} as const;
