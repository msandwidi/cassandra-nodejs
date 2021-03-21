const cassandra = require("cassandra-driver");

const contactPoints = ["localhost"];

const client = new cassandra.Client({
  contactPoints: contactPoints,
  localDataCenter: "datacenter1",
  keyspace: "grocery",
});

/*const query =
  "SELECT item_id, name, price_p_item FROM fruit_stock WHERE item_id = ?";

client
  .execute(query, ["b1"])
  .then((result) => console.log("Item with id %s", result.rows[0]));*/

module.exports = client;
