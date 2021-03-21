const lineReader = require("line-reader");

let utils = {};

utils.readFileLineByLine = (filename, callback) => {
  console.log("reading file line by line...");
  lineReader.eachLine(filename, function (line, last) {
    if (callback) callback(line);

    if (last) {
      return false; // stop reading
    }
  });
};

module.exports = utils;
