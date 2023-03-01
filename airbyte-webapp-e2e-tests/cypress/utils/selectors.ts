export const getTestId = (testId: string, element = "") => `${element}[data-testid='${testId}']`;

export const joinTestIds = (...testIds: Array<string | undefined>) => testIds.filter((testId) => !!testId).join(" ");
