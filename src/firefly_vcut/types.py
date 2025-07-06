from dataclasses import dataclass

@dataclass
class Song:
    id: int
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
    song_id: int
    archive_id: int
    start: int
    page: int

@dataclass
class Series:
    series_id: int
    name: str