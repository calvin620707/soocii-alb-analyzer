#!/usr/bin/env python
import csv
import gzip
from argparse import ArgumentParser
from pathlib import Path

import re
from tempfile import TemporaryFile

import common.args_parsers
from common.downloaders import LogDownloader, DownloadFilePeriodFilter
from common.funcs import get_file_line_count
from common.loggers import ProgressLogger


def _merge_logs(out_f):
    logs = DownloadFilePeriodFilter(args.start, args.end).files
    total = len(logs)
    count = 0

    for zip_file in logs:
        with gzip.open(zip_file, 'r') as in_f:
            out_f.write(in_f.read())
            count += 1
            logger.log("Merging", count, total)
    print("Merged!" + " " * 10)


if __name__ == '__main__':
    logger = ProgressLogger()

    parser = ArgumentParser(description="Get ALB logs by datetime duration")
    parser = common.args_parsers.setup_duration_parser(parser)
    args = parser.parse_args()

    out = Path('out/alb_logs_{}_{}.csv'.format(args.start.isoformat(), args.end.isoformat()))
    with TemporaryFile() as merged_file:
        LogDownloader(args.start, args.end, external=True, internal=True).download()

        _merge_logs(merged_file)
        merged_file.seek(0)

        alb_log_ptn = re.compile(
            r'(?P<schema>http|https|h2|ws|wss) '
            r'(?P<timestamp>[\d\-.:TZ]+) '
            r'(?P<elb>\S+) '
            r'(?P<client_port>\S+) '
            r'(?P<target_port>\S+) '
            r'(?P<request_processing_time>[\d\-.]+) '
            r'(?P<target_processing_time>[\d\-.]+) '
            r'(?P<response_processing_time>[\d\-.]+) '
            r'(?P<elb_status_code>[\d]+) '
            r'(?P<target_status_code>[\d\-]+) '
            r'(?P<received_bytes>\d+) '
            r'(?P<sent_bytes>\d+) '
            r'"(?P<request>\S+ \S+ \S+)" '
            r'"(?P<user_agent>.+?)" '
            r'(?P<ssl_cipher>\S+) '
            r'(?P<ssl_protocol>\S+) '
            r'(?P<target_group_arn>\S+) '
            r'"(?P<trace_id>\S+)" '
            r'"(?P<domain_name>\S+)" '
            r'"(?P<chosen_cert_arn>\S+)" '
            r'(?P<matched_rule_priority>\S+)'
        )

        print("Convert to csv")
        total = get_file_line_count(merged_file)
        count = 0
        with out.open('w') as f:
            writer = csv.writer(f)
            writer.writerow(
                ['schema',
                 'timestamp',
                 'elb',
                 'client_port',
                 'target_port',
                 'request_processing_time',
                 'target_processing_time',
                 'response_processing_time',
                 'elb_status_code',
                 'target_status_code',
                 'received_bytes',
                 'sent_bytes',
                 'request',
                 'user_agent',
                 'ssl_cipher',
                 'ssl_protocol',
                 'target_group_arn',
                 'trace_id',
                 'domain_name',
                 'chosen_cert_arn',
                 'matched_rule_priority']
            )
            for line in merged_file:
                line = line.decode()
                ret = alb_log_ptn.match(line)
                writer.writerow(ret.groups())
                count += 1
                logger.log("Converting", count, total)
        print("Converted!")
