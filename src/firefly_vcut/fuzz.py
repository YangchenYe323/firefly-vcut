from rapidfuzz import fuzz

def search_text_in_transcript(transcript: list[list[dict]], text: str) -> tuple[int, int, int, str] | None:
    """
    Search for the given text in the transcript.

    Args:
        transcript: The transcript to search in
        text: The text to search for

    Returns:
        tuple[int, int, int, str] | None: The start time, page, similarity score, and text of the text
        in the transcript with the highest similarity score
    """

    max_score = 0
    max_page = None
    max_segment_start = None
    max_segment_text = None

    num_segment_in_text = len(text.split("\n"))

    for page, segment in enumerate(transcript):
        if len(segment) < num_segment_in_text:
            segment_text = "\n".join([sg["text"] for sg in segment])
            score = fuzz.ratio(text, segment_text)
            if max_page is None or score > max_score:
                max_score = score
                max_page = page
                max_segment_text = segment_text
                max_segment_start = segment[0]["start"]
            continue

        for i in range(len(segment) - num_segment_in_text + 1):
            segment_text = "\n".join([sg["text"] for sg in segment[i : i + num_segment_in_text]])
            score = fuzz.ratio(text, segment_text)
            if max_page is None or score > max_score:
                max_score = score
                max_page = page
                max_segment_text = segment_text
                max_segment_start = segment[i]["start"]
    
    return (max_segment_start, max_page+1, max_score, max_segment_text)
    
    