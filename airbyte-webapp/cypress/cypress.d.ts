declare namespace Cypress {
  interface AUTWindow {
    document: Document;
    _e2eOverwrites?: Partial<Experiments>;
  }
}
