export const getTestId = (testId: string, element = "") => `${element}[data-testid='${testId}']`;

export const joinTestIds = (...testIds: Array<string | undefined>) => testIds.filter((testId) => !!testId).join(" ");

export const getTestIds = (...testIds: Array<string | [string, string]>): string[] =>
  testIds.map((item) => (Array.isArray(item) ? getTestId(...item) : getTestId(item)));
