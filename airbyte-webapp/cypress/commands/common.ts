export const getSubmitButton = () => {
  return cy.get("button[type=submit]");
};

export const submitButtonClick = (force = false) => {
  cy.get("button[type=submit]").click({ force });
};

export const updateField = (field: string, value: string, isDropdown = false) => {
  if (isDropdown) {
    selectFromDropdown(field, value);
  } else {
    setInputValue(field, value);
  }
};

const setInputValue = (name: string, value: string) => {
  cy.get(`input[name='${name}']`).clear();
  cy.get(`input[name='${name}']`).type(value);
};

const getDropdownSelector = (name: string) => {
  return `[data-testid="${name}"] .react-select__dropdown-indicator`;
};

const selectFromDropdown = (name: string, value: string) => {
  cy.get(getDropdownSelector(name)).last().click({ force: true });
  cy.get(`.react-select__option`).contains(value).click();
};

export const openConnectorPage = (name: string) => {
  cy.get("div").contains(name).click();
};

export const deleteEntity = () => {
  cy.get("button[data-id='open-delete-modal']").click();
  cy.get("button[data-id='delete']").click();
};

export const clearApp = () => {
  indexedDB.deleteDatabase("firebaseLocalStorageDb");
  cy.clearLocalStorage();
  cy.clearCookies();
};

// useful for ensuring that a name is unique from one test run to the next
export const appendRandomString = (string: string) => {
  const randomString = Math.random().toString(36).substring(2, 10);
  return `${string} _${randomString}`;
};

/**
 * Click on specific cell found by column name in desired table
 * @param tableSelector - table selector
 * @param columnName - column name
 * @param connectName - cell text
 */
export const clickOnCellInTable = (tableSelector: string, columnName: string, connectName: string) => {
  cy.contains(`${tableSelector} th`, columnName)
    .invoke("index")
    .then((value) => {
      cy.log(`${value}`);
      return cy.wrap(value);
    })
    .then((columnIndex) => {
      cy.contains("tbody tr", connectName).find("td").eq(columnIndex).find("a").click();
    });
};
