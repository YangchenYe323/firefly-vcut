from dataclasses import dataclass

@dataclass
class VtuberSong:
    song_id: int
    vtuber_song_id: int
    title: str
    lyrics_fragment: str

@dataclass
class Archive:
    id: int | None
    bvid: str
    title: str
    pubdate: int
    cover: str
    duration: int

@dataclass
class SongOccurrence:
    # song_id is the id of the song in the database
    song_id: int
    # vtuber_song_id is the id of the song in the vtuber's song list
    vtuber_song_id: int
    # archive_id is the id of the archive in the database
    archive_id: int
    # start is the start time of the song in the archive
    start: int
    # page is the page of the song in the archive
    page: int

@dataclass
class Series:
    series_id: int
    name: str