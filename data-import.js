const express = require("express");
const cassandraClient = require("./cassandra-client");
const moment = require("moment");
const { cyan, green, grey } = require("kleur");
const os = require("os");
const fs = require("fs").promises;
const utils = require("./utils");
const queries = require("./cassandra-queries");
const parse = require("csv-parse/lib/sync");

//load environment varialbles
const result = require("dotenv").config();

if (result.error) throw result.error;

const { PORT, NODE_ENV } = process.env;

const app = express();

app.disable("x-powered-by");
app.set("trust proxy", 1);

/**
 * test file reader
 */

utils.readFileLineByLine("input-data/test.txt", (line) => {
  //process line
  console.log(line);
});

//test query
cassandraClient
  .execute(queries.test, ["b1"])
  .then((result) => console.log("Item with id %s", result.rows[0]));

app.get("/grocery-select", async (req, res) => {
  const result = await cassandraClient.execute(queries.test, ["b1"]);
  return res.status(200).json({ result });
});

/**
 * count songs
 */
app.get("/count-songs", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT COUNT (*) FROM user_song_ratings.song_info",
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * test select song
 */
app.get("/test-select-song/:id", async (req, res) => {
  console.log(req.params.id);

  const id = parseInt(req.params.id);

  const result = await cassandraClient.execute(
    "SELECT user_id, song_id, rating FROM user_song_ratings.song_info where user_id = ? ALLOW FILTERING",
    [id],
    { prepare: true, hints: ["int"] }
  );
  res.status(200).json({ success: true, result });
});

/**
 * insert all songs in the database
 * change between files to import data
 * change column properties to match csv columns length
 */
app.post("/insert-all-songs", async (req, res) => {
  //read the csv file
  const json = await parseCsvToJson("input-data/dataset/song-attributes.csv");

  console.log(json.length, "lines");

  let queries = [];

  let selected = 0;

  for (let i = 0; i < json.length; i++) {
    //prepare record query
    let statement =
      `INSERT INTO user_song_ratings.song_attributes (song_id, album_id,` +
      ` artist_id, gender_id) VALUES (?, ?, ?, ?)`;

    //console.log(typeof json[i]);

    let query = {
      query: statement,
      params: [
        json[i].song_id,
        json[i].album_id,
        json[i].artist_id,
        json[i].gender_id,
      ],
    };

    ///add to batch queries
    queries.push(query);
    selected++;

    if (selected === 1000 || i === json.length) {
      console.log("running batch...");
      console.log("at", i, "th query items");

      //run batch
      const result = await cassandraClient.batch(queries, { prepare: true });

      //query result
      console.log("query result", result);

      //reset selected
      selected = 0;
      queries = [];
    }
  }

  return res.status(200).json({ success: true, message: "working on it..." });
});

app.listen(PORT, () => {
  console.log(green("✓-- ") + `App running on host ${os.hostname}`);
  console.log(green("✓-- ") + `App listening on localhost:${PORT}`);
  console.log(green("✓-- ") + "Environment type = " + cyan(`${NODE_ENV}`));
  console.log(green("✓-- ") + "Time: " + grey(`${moment().format("llll")}`));
});

module.exports = app;

/**
 * read and parse csv file
 * @param {csv file path as string} filePath
 */
const parseCsvToJson = async (filePath) => {
  console.log("parsing csv to json");
  return parseContent(await fs.readFile(filePath));
};

/**
 * parse file content
 * @param {content of the csv file} content
 * @return {array of records json objects}
 */
const parseContent = async (contentBuffer) => {
  // Parse the CSV content
  //into an array of object
  return parse(contentBuffer, {
    autoParse: true,
    delimiter: ",",
    trim: true, //trim contents
    from: 2, //skip the header of the file
    columns: ["song_id", "album_id", "artist_id", "gender_id"],
    cast: function (value, context) {
      if (parseInt(value) !== null && parseInt(value) !== undefined) {
        return parseInt(value);
      } else {
        return 0;
      }
    },
  });
};
