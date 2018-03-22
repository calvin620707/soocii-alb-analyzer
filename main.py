#!/usr/bin/env python
import time
import optparse
import datetime
import gzip
import boto3
from pathlib import Path
from datetime import date


class LogAnalysis():
    def __init__(self, file_name):
        self.file_name = file_name

    def start(self):
        stats = {}
        with open(self.file_name, 'r') as log_file:
            iterate = 0
            for line in log_file:
                log_time, method, url = line.split(' ')
                log_time = datetime.datetime.strptime(
                    log_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                method = method.strip('"')
                url = url.strip()
                five_time = int(time.mktime(log_time.timetuple()))
                five_time = five_time - (five_time % 300)
                # print(five_time, log_time, method, url)
                if 'content/corpus' in url:
                    continue
                classify = ''
                if 'api/v1' in url:
                    classify = 'jarvis'
                if 'api/getG' in url:
                    classify = 'jarvis'
                if 'graph/v' in url:
                    classify = 'pepper'
                if 'recommendation/v' in url:
                    classify = 'vision'
                if 'search' in url:
                    classify = 'pym'
                if 'titan' in url:
                    classify = 'titan'
                url = url.split('?')
                url = url[0]
                url = '%s_%s' % (method, url)
                if five_time in stats:
                    if classify in stats[five_time]:
                        if url in stats[five_time][classify]:
                            stats[five_time][classify][url] += 1
                        else:
                            stats[five_time][classify][url] = 1
                    else:
                        stats[five_time][classify] = {}
                        stats[five_time][classify][url] = 1
                else:
                    stats[five_time] = {}
                    stats[five_time][classify] = {}
                    stats[five_time][classify][url] = 1
                iterate += 1
        a = self.parse_dict(stats)
        for k, v in a.iteritems():
            print(k)

    def parse_dict(self, init, lkey=''):
        ret = {}
        for rkey, val in init.items():
            key = lkey + str(rkey)
            if isinstance(val, dict):
                ret.update(self.parse_dict(val, key + ','))
            else:
                key += ',' + str(val)
                ret[key] = ''
        return ret


def print_progress(prefix, count, total=None):
    print(
        "{}... {:03.2f}{}".format(
            prefix,
            count * 100 / total if total else count,
            '%' if total else ''
        ), end='\r'
    )


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
        for key in (content['Key'] for content in ret['Contents']):
            file_name = key.replace(prefix, '')
            if (download_folder / file_name).exists():
                total -= 1
                continue
            boto3.resource('s3').Object(bucket, key).download_file(
                str(download_folder / file_name)
            )
            count += 1
            print_progress('Download', count, total)
        print("Download complete!" + " " * 10)


merged_file = Path('./merged')


def merge_logs():
    logs = list(download_folder.glob('*.gz'))
    total = len(logs)
    count = 0
    for p in logs:
        with gzip.open(p, 'rb') as in_f, merged_file.open('wb') as out_f:
            out_f.write(in_f.read())
            count += 1
            print_progress('Decompression', count, total)
    print("Decompression complete!" + " " * 10)


parsed_file = Path('./parsed')


def parse_logs():
    count = 0
    with merged_file.open('r') as in_f, parsed_file.open('w') as out_f:
        for line in in_f:
            split = line.split(' ')
            out_f.write("{datetime} {method} {url}\n".format(
                datetime=split[1], method=split[12][1:], url=split[13])
            )
            count += 1
            print_progress('Parseing', count)
    print("Parse complete!" + " " * 10)


if __name__ == '__main__':
    LogDownloader().download(date(2018, 3, 21), external=True)
    merge_logs()
    parse_logs()

    # parser = optparse.OptionParser()
    # parser.add_option('-f', '--file',
    #                   dest='file_name',
    #                   default='',
    #                   help='Give the time(Mins) for check')
    # options, remainder = parser.parse_args()
    # if options.file_name:
    #     la = LogAnalysis(options.file_name)
    #     la.start()
