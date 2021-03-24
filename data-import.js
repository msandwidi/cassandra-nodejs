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
 * insert all songs in the database
 * change between files to import data
 * change column properties to match csv columns length
 */
app.post("/insert-all-songs", async (req, res) => {
  //read the csv files
  const songList = await parseCsvToJson(
    "input-data/dataset/test_0.csv",
    ["user_id", "song_id", "rating"],
    0
  );
  const attrList = await parseCsvToJson(
    "input-data/dataset/song-attributes.csv",
    ["song_id", "album_id", "artist_id", "gender_id"],
    0
  );
  const genderList = await parseCsvToJson(
    "input-data/dataset/genre-hierarchy.csv",
    ["gender_id", "parent_gender_id", "level", "gender_name"]
  );

  console.log(songList.length, "songs");
  console.log(attrList.length, "attributes");
  console.log(genderList.length, "gender types");

  let queries = [];

  let selected = 0;
  let counter = 0;

  for (let i = 0; i < songList.length; i++) {
    //denormailze data
    let song = songList[i];

    //find attributes
    let songAttrs = attrList.find((a) => a.song_id === song.song_id);

    //find song gender
    if (songAttrs) {
      let songGender = genderList.find(
        (g) => g.gender_id === songAttrs.gender_id
      );

      //merge song data

      //add attributes
      song = { ...song, ...songAttrs };

      if (songGender) {
        //add gender attributes
        song = { ...song, ...songGender };
      }
    }

    //prepare record query
    let statement =
      `INSERT INTO user_song_ratings.song_info (user_id, song_id, ` +
      `rating, album_id, artist_id, gender_id, parent_gender_id, ` +
      `gender_name, level) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    //console.log(typeof json[i]);

    let query = {
      query: statement,
      params: [
        song.user_id,
        song.song_id,
        song.rating,
        song.album_id,
        song.artist_id,
        song.gender_id,
        song.parent_gender_id,
        song.gender_name,
        song.level,
      ],
    };

    ///add to batch queries
    queries.push(query);
    selected++;
    counter++;

    if (selected === 400 || i === songList.length) {
      console.log("running batch...");
      console.log("at", i, "th query items");
      console.log("sample data", song);

      try {
        //run batch
        await cassandraClient.batch(queries, { prepare: true });
      } catch (error) {
        console.log(error);
      }
      //query result
      //console.log("query result", result);

      //write query result on a file
      //await fs.writeFile(`./results//data_${i}.json`, JSON.stringify(result));

      //reset selected
      selected = 0;
      queries = [];
    }
  }

  console.log("processed song ratings", counter);

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
const parseCsvToJson = async (filePath, columns, default_return) => {
  console.log("parsing csv to json");
  return parseContent(await fs.readFile(filePath), columns, default_return);
};

/**
 * parse file content
 * @param {content of the csv file} content
 * @return {array of records json objects}
 */
const parseContent = async (contentBuffer, columns, default_return) => {
  // Parse the CSV content
  //into an array of object
  return parse(contentBuffer, {
    autoParse: true,
    delimiter: ",",
    trim: true, //trim contents
    from: 2, //skip the header of the file
    columns,
    cast: function (value, context) {
      if (
        parseInt(value) !== null &&
        parseInt(value) !== undefined &&
        !Number.isNaN(parseInt(value))
      ) {
        return parseInt(value);
      } else {
        return default_return || value;
      }
    },
  });
};
