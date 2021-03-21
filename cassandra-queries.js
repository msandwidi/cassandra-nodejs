let queries = {};

//test query
queries.test =
  "SELECT item_id, name, price_p_item FROM fruit_stock WHERE item_id = ?";

/**
 * songs with no gender
 * Easy question 1
 */
queries.songsWithNoGender = "SELECT * FROM songInfo WHERE genre_name = NULL";

/**
 * songs with unknown gender
 * Easy question 2
 */
queries.songsWithUnkownGender =
  "SELECT COUNT(*)  FROM songInfo WHERE genre_id = 0";

/**
 * count number of user who have rated songs
 * Easy question 3
 */
queries.countUsersWhoRatedSongs = "SELECT COUNT(*) FROM songInfo";

/**
 * count how many time a song is been rated 1
 * Easy question 4
 */
queries.countNumberOfTimesSongIsRated1 =
  "SELECT COUNT(*)  FROM songInfo WHERE rating = 1 AND songId = ?";

/**
 * Find user most liked song
 * Moderate question 1
 */
queries.findUserMostLikedSong =
  "SELECT max(userId) FROM songInfo WHERE userId = ?";

/**
 * count number of times a song was rated with any rating
 * Moderate question 2
 */
queries.countNumberOfTimesSongWasRated = "SELECT COUNT(*) WHERE songId = ?";

/**
 * find average rating of a gender
 * Moderate question 3
 */
queries.findAverageRatingOfOneGender =
  "SELECT avg(userId) FROM songInfo WHERE genre_id = ?";

/**
 * find users with similar interests
 * Challenging question
 */
queries.findUsersWithSimilarInterests =
  "SELECT * FROM songInfo GROUP BY genre_id";

module.exports = queries;
