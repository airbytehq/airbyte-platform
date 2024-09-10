class MockWorker {
  constructor(stringUrl) {
    this.url = stringUrl;
    this.onmessage = null;
  }

  postMessage(msg) {
    // Simulate immediate response (in a real worker, this would be asynchronous)
    if (this.onmessage) {
      this.onmessage({ data: "Mocked response" });
    }
  }

  terminate() {
    // Mock the termination of the worker
  }
}

export default MockWorker;
