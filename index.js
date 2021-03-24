const express = require("express");
const cassandraClient = require("./cassandra-client");
const moment = require("moment");
const { cyan, green, grey } = require("kleur");
const os = require("os");
const queries = require("./cassandra-queries");

//load environment varialbles
const result = require("dotenv").config();

if (result.error) throw result.error;

const { PORT, NODE_ENV } = process.env;

const app = express();

app.disable("x-powered-by");
app.set("trust proxy", 1);

//test query
cassandraClient
  .execute(queries.test, ["b1"])
  .then((result) => console.log("Item with id %s", result.rows[0]));

/**
 * get null gender
 * easy question 1
 */
app.get("/null-gender-songs", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT * FROM user_song_ratings.song_info WHERE gender_name = ?",
    [null],
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * count songs with unknown gender
 * easy question 2
 */
app.get("/unknown-gender-songs", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT * FROM user_song_ratings.song_info WHERE gender_id = ?",
    [0],
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * count ratings
 * easy question 3
 */
app.get("/count-ratings", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT COUNT (*) FROM user_song_ratings.song_info",
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * count soungs with rating of 1
 * easy question 4
 */
app.get("/count-ratings", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT COUNT (*) FROM user_song_ratings.song_info WHERE rating = ?",
    [0],
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * find a user most liked song
 * moderate question 1
 */
app.get("/user-most-liked-song/:id", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT max(user_id) FROM user_song_ratings.song_info WHERE user_id = ?",
    [req.params.id],
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * find number of times a song was rated
 * moderate question 2
 */
app.get("/number-or-song-ratings/:id", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT COUNT (*) FROM user_song_ratings.song_info WHERE song_id = ?",
    [req.params.id],
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * find average rating of a gender
 * moderate question 3
 */
app.get("/average-rating-of-gender/:id", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT avg(user_id) FROM user_song_ratings.song_info WHERE gender_id = ?",
    [req.params.id],
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

/**
 * group user with similar interests
 * challenging question
 */
app.get("/group-by-interests", async (req, res) => {
  const result = await cassandraClient.execute(
    "SELECT * FROM song_info GROUP BY genre_id",
    { prepare: true }
  );
  res.status(200).json({ success: true, result });
});

app.listen(PORT, () => {
  console.log(green("✓-- ") + `App running on host ${os.hostname}`);
  console.log(green("✓-- ") + `App listening on localhost:${PORT}`);
  console.log(green("✓-- ") + "Environment type = " + cyan(`${NODE_ENV}`));
  console.log(green("✓-- ") + "Time: " + grey(`${moment().format("llll")}`));
});

module.exports = app;
