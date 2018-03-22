﻿#!/usr/bin/env python
import csv
import gzip
import re
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
import dateutil.parser


class ProgressLogger:
    prev_print_at = datetime.now()

    def log(self, prefix, count, total=None):
        if self.prev_print_at > datetime.now() - timedelta(seconds=1):
            return
        print(
            "{}... {:03.2f}{}".format(
                prefix,
                count * 100 / total if total else count,
                '%' if total else ''
            ), end='\r'
        )
        self.prev_print_at = datetime.now()


progress_logger = ProgressLogger()

download_folder = Path("./download")


class LogDownloader:
    def download(self, date, external=False, internal=False):
        if not download_folder.exists():
            download_folder.mkdir()

        base_prefix = 'AWSLogs/710026814108/elasticloadbalancing/ap-northeast-1/{}/{:02d}/{:02d}/'.format(
            date.year, date.month, date.day
        )
        external_prefix = '710026814108_elasticloadbalancing_ap-northeast-1_app.api-prod-elb.'
        internal_prefix = '710026814108_elasticloadbalancing_ap-northeast-1_app.api-prod-internal-elb.'

        if external:
            self._download_with_prefix(
                base_prefix + external_prefix, 'external')

        if internal:
            self._download_with_prefix(
                base_prefix + internal_prefix, 'internal')

    def _download_with_prefix(self, prefix, source):
        bucket = 'prod-lbs-access-log'
        s3_client = boto3.client('s3')

        ret = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        print("{} have {} objects".format(source, ret['KeyCount']))
        if 'Contents' not in ret:
            raise RuntimeError("no files be found on S3")

        count = 0
        total = len(ret['Contents'])
        exist = 0
        for key in (content['Key'] for content in ret['Contents']):
            file_name = key.replace(prefix, '')
            if (download_folder / file_name).exists():
                total -= 1
                exist += 1
                continue
            boto3.resource('s3').Object(bucket, key).download_file(
                str(download_folder / file_name)
            )
            count += 1
            progress_logger.log('Download', count, total)
        print("Download complete! {} existed files.".format(exist) + " " * 10)


merged_file = Path('./merged')


def merge_logs():
    if merged_file.exists():
        print("File, {}, exists. Skip merging logs.".format(merged_file))
        return

    log_paths = list(download_folder.glob('*.gz'))
    total = len(log_paths)
    count = 0
    with merged_file.open('wb') as out_f:
        for p in log_paths:
            with gzip.open(p, 'rb') as in_f:
                out_f.write(in_f.read())
                count += 1
                progress_logger.log('Decompression', count, total)
    print("Decompression {} files complete!".format(total) + " " * 10)


parsed_file = Path('./parsed')


def parse_logs():
    if parsed_file.exists():
        print("File, {}, exists. Skip parsing logs.".format(parsed_file))
        return

    count = 0
    total = sum(1 for _ in merged_file.open('r'))
    with merged_file.open('r') as in_f, parsed_file.open('w') as out_f:
        for line in in_f:
            split = line.split(' ')
            out_f.write("{datetime} {method} {url}\n".format(
                datetime=split[1], method=split[12][1:], url=split[13])
            )
            count += 1
            progress_logger.log('Parsing', count, total)
    print("Parsing logs complete!" + " " * 10)


class LogAnalyzer:
    def __init__(self):
        self.stat_file = Path('./stat.csv')
        self.count = 0
        self.normalize_handler = {
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\d*/achievements'):
                "https://api.soocii.me:443/graph/v1.2/<id>/achievements",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\d*/followees/count'):
                "https://api.soocii.me:443/graph/v1.2/<id>/followees/count",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\d*/followers/count'):
                "https://api.soocii.me:443/graph/v1.2/<id>/followers/count",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/users/\d*/followees'):
                "https://api.soocii.me:443/graph/v1.2/users/<id>/followees",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/users/\d*/followers'):
                "https://api.soocii.me:443/graph/v1.2/users/<id>/followers",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\d*/friendship'):
                "https://api.soocii.me:443/graph/v1.2/<id>/friendship",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\d*/posts'):
                "https://api.soocii.me:443/graph/v1.2/<id>/posts",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/me/feed/\w*-\w*'):
                "https://api.soocii.me:443/graph/v1.2/me/feed/<status_id>",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\w*-\w*/comments'):
                "https://api.soocii.me:443/graph/v1.2/<status_id>/comments",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/posts/\w*-\w*/likes'):
                "https://api.soocii.me:443/graph/v1.2/posts/<status_id>/likes",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/\d*/pinned-posts'):
                "https://api.soocii.me:443/graph/v1.2/<id>/pinned-posts",
            re.compile(r'https://api\.soocii\.me:443/graph/v1\.2/me/posts/\w*-\w'):
                "https://api.soocii.me:443/graph/v1.2/me/posts/<status_id>"
        }

    def stat_api_calls(self, start, end):
        stats = defaultdict(lambda: 0)
        total = sum(1 for _ in parsed_file.open('r'))
        with parsed_file.open('r') as in_f:
            for line in in_f:
                self.count += 1
                progress_logger.log("Analyzing", self.count, total)
                line = line.replace('\n', '')
                log_at, method, url = self._parse_line(line)

                if log_at < start or log_at > end:
                    continue
                if 'content/corpus' in url:
                    continue

                service = self._identify_service(url)
                url = self._normalize_url(url)
                stats["{} {} {}".format(service, method, url)] += 1

        with self.stat_file.open('w') as out_f:
            writer = csv.writer(out_f)
            for key, count in stats.items():
                split = key.split(' ')
                service, method, url = split[0], split[1], split[2]
                writer.writerow([service, method, url, count])

        print("Analyzing logs complete!" + " " * 12)

    def _parse_line(self, line):
        log_datetime, method, url = line.split(' ')
        log_datetime = datetime.strptime(
            log_datetime, '%Y-%m-%dT%H:%M:%S.%fZ'
        )
        return log_datetime, method, url

    def _identify_service(self, url):
        service = ''
        if 'api/v1' in url:
            service = 'jarvis'
        if 'api/getG' in url:
            service = 'jarvis'
        if 'graph/v' in url:
            service = 'pepper'
        if 'recommendation/v' in url:
            service = 'vision'
        if 'search' in url:
            service = 'pym'
        if 'titan' in url:
            service = 'titan'
        return service

    def _normalize_url(self, url):
        url = url.split('?')[0]
        for ptn, endpoint in self.normalize_handler.items():
            if ptn.match(url):
                url = endpoint
        return url


if __name__ == '__main__':
    def convert_datetime_str(d_str):
        dt = dateutil.parser.parse(d_str)
        dt = dt.astimezone(timezone.utc)
        dt = dt.replace(tzinfo=None)
        return dt


    arg_parser = ArgumentParser(description="Analyze ALB logs by datetime duration")
    arg_parser.add_argument('start', type=convert_datetime_str, help="Start datetime")
    arg_parser.add_argument('end', type=convert_datetime_str, help="End datetime")
    arg_parser.add_argument("-e", "--external", action="store_true", dest="ext", default=True,
                            help="Analyze external ALB")
    arg_parser.add_argument("-i", "--internal", action="store_true", dest="int", default=False,
                            help="Analyze internal ALB")

    args = arg_parser.parse_args()

    LogDownloader().download(args.start.date())
    if args.start.date() != args.end.date():
        LogDownloader().download(args.end.date())

    merge_logs()
    parse_logs()
    LogAnalyzer().stat_api_calls(args.start, args.end)
