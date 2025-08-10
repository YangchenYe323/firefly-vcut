import modal

from .app import app
from .recording import discover_new_recordings
from .stream import stream_recordings
from .transcribe import transcribe_recordings
from .occurrence import populate_occurrences


@app.function(
    timeout=60 * 60, # 60 minutes
    schedule=modal.Cron(timezone="Asia/Shanghai", cron_string="0 19 * * *") # 7:00PM Asia/Shanghai everyday.
)
def main():
    new_recordings = discover_new_recordings.remote()
    if new_recordings == 0:
        return
    stream_recordings.remote()
    transcribe_recordings.remote()
    populate_occurrences.remote()
