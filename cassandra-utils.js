const cassandraClient = require("./cassandra-client");

let utils = {};

/**
 * import data from csv file
 * @param {*} path
 * @param {*} table
 */
utils.importDataFromCSV = async (path, table) => {
  const query = "COPY " + table + " FROM " + path + " HEADER = FALSE";
  return cassandraClient.execute(query);
};

module.exports = utils;
