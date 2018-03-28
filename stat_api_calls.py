#!/usr/bin/env python
import csv
import gzip
import re
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile

import common.args_parsers
from common.downloaders import LogDownloader, DownloadFilePeriodFilter
from common.funcs import get_file_line_count
from common.loggers import ProgressLogger

progress_logger = ProgressLogger()


class ParsedLogFile:
    def __init__(self, start, end):
        self.start, self.end = start, end

    def __enter__(self):
        self.__temp_file = TemporaryFile()
        self.__parse()
        self.__temp_file.seek(0)
        return self.__temp_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__temp_file.close()

    def __parse(self):
        logs = DownloadFilePeriodFilter(self.start, self.end).files
        total = len(logs)
        count = 0
        for l in logs:
            with gzip.open(l, 'rb') as in_f:
                for line in in_f:
                    text = line.decode()
                    split = text.split(" ")
                    text = "{datetime} {method} {url}\n".format(
                        datetime=split[1], method=split[12].strip("\""), url=split[13]
                    )
                    self.__temp_file.write(text.encode())
                count += 1
                progress_logger.log("Parsing gz files", count, total)
        self.__temp_file.seek(0)
        print("Parsing gz files complete!" + " " * 10)


class LogAnalyzer:
    def __init__(self, start, end):
        self.start, self.end = start, end

        self.stats_file = Path('./out/stats_{}_{}.csv'.format(start.isoformat(), end.isoformat()))
        self.stats_file.parent.mkdir(parents=True, exist_ok=True)

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

    def stat_api_calls(self, parsed_file):
        stats = defaultdict(lambda: 0)
        total = get_file_line_count(parsed_file)
        parsed_file.seek(0)
        count = 0
        for line in parsed_file:
            count += 1
            progress_logger.log("Analyzing", count, total)
            line = line.decode()
            line = line.strip('\n')
            log_at, method, url = self._parse_line(line)

            if not (self.start < log_at < self.end):
                continue
            if 'content/corpus' in url:
                continue

            service = self._identify_service(url)
            url = self._normalize_url(url)
            stats["{} {} {}".format(service, method, url)] += 1

        with self.stats_file.open('w') as out_f:
            writer = csv.writer(out_f)
            writer.writerow(['service', 'method', 'url', 'count'])
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
        url = url.rstrip('/')
        for ptn, endpoint in self.normalize_handler.items():
            if ptn.match(url):
                url = endpoint
        return url


def setup_args_parser():
    parser = ArgumentParser(description="Analyze ALB logs by datetime duration")
    parser = common.args_parsers.setup_duration_parser(parser)
    parser = common.args_parsers.setup_alb_parser(parser)
    parser.add_argument("--force-download", action="store_true", dest="force_download", default=False,
                        help="Download files from S3 even file exists locally.")
    return parser


if __name__ == '__main__':
    arg_parser = setup_args_parser()
    args = arg_parser.parse_args()

    analyzer = LogAnalyzer(args.start, args.end)
    if analyzer.stats_file.exists():
        cont = input("{} exist. Do you want to continue? [Y|n]".format(analyzer.stats_file))
        if cont.lower() == 'n':
            exit()

    downloader = LogDownloader(args.start, args.end, args.ext, args.int, args.force_download)
    if args.force_download:
        print("Force download on. Overwriting existed files in {} folder.".format(downloader.folder))
    downloader.download()

    with ParsedLogFile(args.start, args.end) as parsed_file:
        analyzer.stat_api_calls(parsed_file)
