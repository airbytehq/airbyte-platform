const CRON_REGEX_MAP = [
  /^(([0-9]|,|-|\*|\/)+)$/, // seconds
  /^(([0-9]|,|-|\*|\/)+)$/, // minutes
  /^(([0-9]|,|-|\*|\/)+)$/, // hours
  /^(([0-9]|,|-|\*|\/|\?|L|W)+)$/, // day of month
  /^(([1-9]|,|-|\*|\/|JAN|FEB|MAR|APR|JUN|JUL|AUG|SEP|OCT|NOV|DEC)+)$/, // month
  /^(([1-7]|,|-|\*|\/|\?|L|#|SUN|MON|TUE|WED|THU|FRI|SAT|SUN)+)$/, // day of week
  /^(([0-9]|,|-|\*|\/)+)/, // year
];

export function validateCronExpression(expression: string | undefined): { isValid: boolean; message?: string } {
  if (expression === undefined) {
    return { isValid: false };
  }

  try {
    const cronFields = expression.trim().split(/\s+/g);

    if (cronFields.length < 6) {
      throw new Error(
        `Cron expression "${expression}" must contain at least 6 fields (${cronFields.length} fields found)`
      );
    }

    if (cronFields.length > 7) {
      throw new Error(
        `Cron expression "${expression}" cannot be longer than 7 fields (${cronFields.length} fields found})`
      );
    }

    cronFields.forEach((field, index) => {
      if (!CRON_REGEX_MAP[index].test(field)) {
        throw new Error(`"${field}" did not match regex at index ${index}`);
      }
    });
  } catch (e) {
    return { isValid: false, message: e.message };
  }

  return { isValid: true };
}
