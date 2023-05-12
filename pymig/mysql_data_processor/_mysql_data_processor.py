import io


def process_mysql_data(batch: tuple[tuple[str, ...], ...]) -> io.StringIO:
    """
    TODO: add description.
    """
    # Note, the list comprehension below wrapped in square brackets on purpose.
    # DO NOT strip the brackets, since it will work slower.
    rows: str = '\n'.join(['\t'.join(record) for record in batch])
    text_stream: io.StringIO = io.StringIO()
    text_stream.write(rows)
    text_stream.seek(0)
    return text_stream
