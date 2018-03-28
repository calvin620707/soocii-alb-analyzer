def get_file_line_count(in_file):
    rv = sum(1 for _ in in_file)
    in_file.seek(0)
    return rv