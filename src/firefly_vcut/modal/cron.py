import modal

from .app import app
from .recording import discover_new_recordings
from .stream import stream_recordings
from .transcribe import transcribe_recordings
from .occurrence import populate_occurrences


@app.function(
    timeout=120 * 60, # 120 minutes
    schedule=modal.Cron(timezone="Asia/Shanghai", cron_string="0 3 * * *") # 3:00AM Asia/Shanghai everyday.
)
def main():
    # Note: we should probably divide this into multiple crons that run at their own cadence.
    # The recording discovery should match the cadence of when and how often the vtuber streams and when
    # the bilibili publishes the recording series, but others are not bound by this.
    discover_new_recordings.remote()
    stream_recordings.remote()
    transcribe_recordings.remote()
    populate_occurrences.remote()
