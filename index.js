const express = require("express");
const cassandraClient = require("./cassandra-client");
const moment = require("moment");
const { cyan, green, grey } = require("kleur");
const os = require("os");

//load environment varialbles
const result = require("dotenv").config();

if (result.error) throw result.error;

const { PORT, NODE_ENV } = process.env;

const app = express();

app.disable("x-powered-by");
app.set("trust proxy", 1);

/**
 * test queries
 */
const query =
  "SELECT item_id, name, price_p_item FROM fruit_stock WHERE item_id = ?";

cassandraClient
  .execute(query, ["b1"])
  .then((result) => console.log("Item with id %s", result.rows[0]));

app.listen(PORT, () => {
  console.log(green("✓-- ") + `App running on host ${os.hostname}`);
  console.log(green("✓-- ") + `App listening on localhost:${PORT}`);
  console.log(green("✓-- ") + "Environment type = " + cyan(`${NODE_ENV}`));
  console.log(green("✓-- ") + "Time: " + grey(`${moment().format("llll")}`));
});

module.exports = app;
