// Script starting a basic webserver returning mocked data over an authenticated API to test the connector builder UI and connector builder server in an
// end to end fashion.

// Start with `npm run createdummyapi`

const http = require("http");

const itemsWithoutSlices = [{ name: "abc" }, { name: "def" }, { name: "xxx" }, { name: "yyy" }];
const itemsExceedingReadLimit = Array.from(Array(100).keys()).map((count) => ({
  exceedingPageLimit: `subitem${count}`,
}));

const paginateResults = function (result, offset, page_size) {
  return [...result].splice(offset, page_size);
};

const generateResults = function (item_id, count) {
  key = `subitem${item_id}`;
  return Array.from(Array(count).keys()).map((i) => ({ key: `subitem${i}` }));
};

const requestListener = function (req, res) {
  if (req.headers.authorization !== "Bearer theauthkey") {
    res.writeHead(403);
    res.end(JSON.stringify({ error: "Bad credentials" }));
    return;
  }

  if (!req.url.startsWith("/items")) {
    res.writeHead(404);
    res.end(JSON.stringify({ error: "Not found" }));
  } else {
    limit = req.headers.limit ? Number(req.headers.limit) : 2;
    offset = req.headers.offset ? Number(req.headers.offset) : 0;
    res.setHeader("Content-Type", "application/json");
    res.writeHead(200);
    if (req.url === "/items/") {
      res.end(JSON.stringify({ items: paginateResults(itemsWithoutSlices, offset, limit) }));
    } else if (req.url === "/items/exceeding-page-limit/") {
      res.end(JSON.stringify({ items: paginateResults(itemsExceedingReadLimit, offset, limit) }));
    } else {
      item_id = req.url.split("/").pop();
      res.end(JSON.stringify({ items: paginateResults(generateResults(item_id, 20), offset, limit) }));
    }
  }
};

const server = http.createServer(requestListener);
server.listen(6767);

process.on("SIGINT", () => {
  process.exit();
});
