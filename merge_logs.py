#!/usr/bin/env python
import gzip
from pathlib import Path

from lib.args_parsers import build_duration_parser
from lib.downloaders import LogDownloader

if __name__ == '__main__':
    parser = build_duration_parser("Get ALB logs by datetime duration")
    args = parser.parse_args()
    LogDownloader(args.start, args.end, external=True, internal=True).download()

    out = Path('out/merged_alb_logs_{}_{}.txt'.format(args.start, args.end))
    if out.exists():
        cont = input("{} exist. Do you want to continue? [Y|n]".format(out))
        if cont.lower() == 'n':
            exit()

    with out.open('wb') as out_f:
        for zip_file in LogDownloader.folder.glob('*.gz'):
            with gzip.open(zip_file, 'r') as in_f:
                out_f.write(in_f.read())
