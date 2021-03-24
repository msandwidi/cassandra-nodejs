CREATE TABLE song_info (
    user_id int,
    song_id int,
    rating int,
    gender_id int,
    parent_gender_id int,
    gender_name text,
    artist_id int,
    album_id int,
    level int,
    PRIMARY KEY (user_id, song_id)
)