#!/usr/bin/env python
import gzip
from argparse import ArgumentParser
from pathlib import Path

import common.args_parsers
from common.downloaders import LogDownloader
from common.loggers import ProgressLogger

if __name__ == '__main__':
    logger = ProgressLogger()

    parser = ArgumentParser(description="Get ALB logs by datetime duration")
    parser = common.args_parsers.setup_duration_parser(parser)
    args = parser.parse_args()
    LogDownloader(args.start, args.end, external=True, internal=True).download()

    out = Path('out/merged_alb_logs_{}_{}.txt'.format(
        args.start.isoformat(), args.end.isoformat())
    )
    if out.exists():
        cont = input("{} exist. Do you want to continue? [Y|n]".format(out))
        if cont.lower() == 'n':
            exit()

    logs = list(LogDownloader.folder.glob('*.gz'))
    total = len(logs)
    count = 0
    with out.open('wb') as out_f:
        for zip_file in logs:
            with gzip.open(zip_file, 'r') as in_f:
                out_f.write(in_f.read())
                count += 1
                logger.log("Merging", count, total)
