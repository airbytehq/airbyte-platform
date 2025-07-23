import { validateCronExpression } from "./validateCronExpression";

// Test cases are taken from http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html
describe("validateCronExpression", () => {
  it.each`
    expression                                               | isValid  | message
    ${"0 0 12 * * ?"}                                        | ${true}  | ${undefined}
    ${"0  0  12  *  *  ?  "}                                 | ${true}  | ${undefined}
    ${"0 0 12 * * ? "}                                       | ${true}  | ${undefined}
    ${" 0 0 12 * * ?"}                                       | ${true}  | ${undefined}
    ${"0/5 14,18,3-39,52 * ? JAN,MAR,SEP MON-FRI 2002-2010"} | ${true}  | ${undefined}
    ${"0 15 10 ? * *"}                                       | ${true}  | ${undefined}
    ${"0 15 10 * * ?"}                                       | ${true}  | ${undefined}
    ${"0 15 10 * * ? *"}                                     | ${true}  | ${undefined}
    ${"0 15 10 * * ? 2005"}                                  | ${true}  | ${undefined}
    ${"0 * 14 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 0/5 14 * * ?"}                                      | ${true}  | ${undefined}
    ${"0 0/5 14,18 * * ?"}                                   | ${true}  | ${undefined}
    ${"0 0-5 14 * * ?"}                                      | ${true}  | ${undefined}
    ${"0 10,44 14 ? 3 WED"}                                  | ${true}  | ${undefined}
    ${"0 15 10 ? * MON-FRI"}                                 | ${true}  | ${undefined}
    ${"0 15 10 15 * ?"}                                      | ${true}  | ${undefined}
    ${"0 15 10 L * ?"}                                       | ${true}  | ${undefined}
    ${"0 15 10 L-2 * ?"}                                     | ${true}  | ${undefined}
    ${"0 15 10 ? * 6L"}                                      | ${true}  | ${undefined}
    ${"0 15 10 ? * 6L"}                                      | ${true}  | ${undefined}
    ${"0 15 10 ? * 6L 2002-2005"}                            | ${true}  | ${undefined}
    ${"0 15 10 ? * 6#3"}                                     | ${true}  | ${undefined}
    ${"0 0 12 1/5 * ?"}                                      | ${true}  | ${undefined}
    ${"0 11 11 11 11 ?"}                                     | ${true}  | ${undefined}
    ${"* * * * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 0 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 1 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 10-19/10 ? * MON-FRI *"}                          | ${true}  | ${undefined}
    ${"0 0 1 1/1 * ? *"}                                     | ${true}  | ${undefined}
    ${"0 0 12 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 15 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 17 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 18 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 18 1 * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 18 2 * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 2 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 21 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 0 2 L * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 3 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 4 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 5 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 6 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 7 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 9 * * ?"}                                         | ${true}  | ${undefined}
    ${"0 0 9 ? * 5"}                                         | ${true}  | ${undefined}
    ${"0 1 0 * * ?"}                                         | ${true}  | ${undefined}
    ${"* * * 1 * ?"}                                         | ${true}  | ${undefined}
    ${"* * * 10 * ?"}                                        | ${true}  | ${undefined}
    ${"* * * 31 * ?"}                                        | ${true}  | ${undefined}
    ${"0 15,45 7-17 ? * MON-FRI"}                            | ${true}  | ${undefined}
    ${"0 15 6 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 30 1 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 30 2 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 30 6 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 30 8 ? * MON-FRI *"}                                | ${true}  | ${undefined}
    ${"0 35 12 ? * 7 "}                                      | ${true}  | ${undefined}
    ${"0 40 4,16 * * ? *"}                                   | ${true}  | ${undefined}
    ${"0 45 6 * * ?"}                                        | ${true}  | ${undefined}
    ${"0 5 0 ? * 7"}                                         | ${true}  | ${undefined}
    ${"40 4,16 * * * ?"}                                     | ${true}  | ${undefined}
    ${"* * * * * * *"}                                       | ${true}  | ${undefined}
    ${"wildly invalid"}                                      | ${false} | ${'Cron expression "wildly invalid" must contain at least 6 fields (2 fields found)'}
    ${"* * * * *"}                                           | ${false} | ${'Cron expression "* * * * *" must contain at least 6 fields (5 fields found)'}
    ${"0 0 0 0 0 0"}                                         | ${false} | ${'"0" did not match regex at index 4'}
    ${"* * * * * * ?"}                                       | ${false} | ${'"?" did not match regex at index 6'}
  `("'$expression' is valid: $isValid", ({ expression, isValid, message }) => {
    expect(validateCronExpression(expression).isValid).toEqual(isValid);
    expect(validateCronExpression(expression).message).toEqual(message);
  });
});
