# firefly-vcut

Transcribe vtuber live recordings & Tools I use to manage the data pipeline for firefly : )

## Dependencies

In addition to uv-managed python dependencies, this project needs access to the below binaries in PATH:

- [BBDown](https://github.com/nilaoda/BBDown)
- [ffmpeg](https://ffmpeg.org/)

## Build from source

```
git clone https://github.com/YangchenYe323/firefly-vcut
cd firefly-vcut
uv pip install .
```

## Usage

The entry point is the `vcut` script:

```
(firefly-vcut) mark@DESKTOP-KUNOCMR:~/git/YangchenYe323/firefly-vcut$ vcut --help
Usage: vcut [OPTIONS] COMMAND [ARGS]...

Options:
  --root PATH    Root directory for vcut operations
  -v, --verbose
  --help         Show this message and exit.

Commands:
  search            Fuzzy search for the given lyric in the transcript.
  sync-archive      Syncs locally stored live recording archives with the...
  sync-occurrences  Syncs song occurrence in live recordings with the...
  transcriber       Download and transcribe **ALL** live recordings of a...
```

