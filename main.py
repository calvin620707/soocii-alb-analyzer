#!/usr/bin/env python
import csv
import gzip
import re
import shutil
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
import dateutil.parser
import os


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
    def __init__(self, start, end, external=False, internal=False):
        self.start, self.end = start, end
        self.external, self.internal = external, internal

    def download(self):
        if not download_folder.exists():
            download_folder.mkdir()

        self._download_with_date(self.start.date())
        if self.start.date() != self.end.date():
            self._download_with_date(self.end.date())

    def _download_with_date(self, date):
        base_prefix = 'AWSLogs/710026814108/elasticloadbalancing/ap-northeast-1/{}/{:02d}/{:02d}/'.format(
            date.year, date.month, date.day
        )
        external_prefix = '710026814108_elasticloadbalancing_ap-northeast-1_app.api-prod-elb.'
        internal_prefix = '710026814108_elasticloadbalancing_ap-northeast-1_app.api-prod-internal-elb.'

        if self.external:
            self._download_with_prefix(base_prefix + external_prefix)

        if self.internal:
            self._download_with_prefix(base_prefix + internal_prefix)

    def _download_with_prefix(self, prefix):
        bucket = 'prod-lbs-access-log'
        s3_client = boto3.client('s3')

        ret = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        if 'Contents' not in ret:
            raise RuntimeError("no files be found on S3")

        keys = [content['Key'] for content in ret['Contents']]
        keys = self._filter_object_keys(keys, prefix)
        if not keys:
            raise RuntimeError("No objects matched given time period.")

        count = 0
        total = len(keys)
        exist = 0
        for key in keys:
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

        results = "Download complete!"
        if exist:
            results += " Skip {} existed files.".format(exist)
        print(results + " " * 10)

    def _filter_object_keys(self, keys, prefix):
        def is_valid(key):
            key = key.strip(prefix)
            obj_datetime = datetime.strptime(key.split("_")[1], "%Y%m%dT%H%MZ")
            if self.start < obj_datetime < self.end:
                return True
            else:
                return False

        return list(filter(is_valid, keys))


merged_file = Path('./merged')


# TODO: merge logs which only match duration
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
    def __init__(self, start, end):
        self.start, self.end = start, end
        self.stat_file = Path('./stat_{}_{}.csv'.format(start.isoformat(), end.isoformat()))
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

    def stat_api_calls(self):
        stats = defaultdict(lambda: 0)
        total = sum(1 for _ in parsed_file.open('r'))
        count = 0
        with parsed_file.open('r') as in_f:
            for line in in_f:
                count += 1
                progress_logger.log("Analyzing", count, total)
                line = line.replace('\n', '')
                log_at, method, url = self._parse_line(line)

                if log_at < self.start or log_at > self.end:
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
        if 'api/' in url:
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


def setup_args_parser():
    def convert_datetime_str(d_str):
        dt = dateutil.parser.parse(d_str)
        dt = dt.astimezone(timezone.utc)
        dt = dt.replace(tzinfo=None)
        return dt

    arg_parser = ArgumentParser(description="Analyze ALB logs by datetime duration")
    arg_parser.add_argument('start', type=convert_datetime_str, help="Start datetime")
    arg_parser.add_argument('end', type=convert_datetime_str, help="End datetime")
    arg_parser.add_argument("-e", "--external", action="store_true", dest="ext", default=True,
                            help="Analyze external ALB (default on)")
    arg_parser.add_argument("-i", "--internal", action="store_true", dest="int", default=False,
                            help="Analyze internal ALB (default off)")
    arg_parser.add_argument("--no-cache", action="store_true", dest="rm_cache", default=False,
                            help="Remove cached files.")
    return arg_parser


def clean_cache():
    if download_folder.exists():
        shutil.rmtree(str(download_folder))
        print("Deleted {}.".format(download_folder))
    if merged_file.exists():
        os.remove(str(merged_file))
        print("Deleted {}.".format(merged_file))
    if parsed_file.exists():
        os.remove(str(parsed_file))
        print("Deleted {}.".format(parsed_file))


if __name__ == '__main__':
    arg_parser = setup_args_parser()
    args = arg_parser.parse_args()

    analyzer = LogAnalyzer(args.start, args.end)
    if analyzer.stat_file.exists():
        cont = input("{} exist. Do you want to continue? [Y|n]".format(analyzer.stat_file))
        if cont.lower() == 'n':
            exit()

    if args.rm_cache:
        clean_cache()

    LogDownloader(args.start, args.end, args.ext, args.int).download()
    merge_logs()
    parse_logs()
    analyzer.stat_api_calls()
