const { app } = require("./app");

const port = Number(process.env.PORT || 3000);

if (!Number.isSafeInteger(port) || port <= 0 || port > 65535) {
  throw new TypeError("PORT must be an integer between 1 and 65535");
}

const server = app.listen(port, () => {
  console.log(`Server läuft auf http://localhost:${port}`);
});

module.exports = {
  server,
};
