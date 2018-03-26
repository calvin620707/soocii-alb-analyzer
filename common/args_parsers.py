from argparse import ArgumentParser

import dateutil.parser
from datetime import timezone


def setup_duration_parser(parser):
    def convert_datetime_str(d_str):
        dt = dateutil.parser.parse(d_str)
        dt = dt.astimezone(timezone.utc)
        dt = dt.replace(tzinfo=None)
        return dt

    parser.add_argument('start', type=convert_datetime_str, help="Start datetime in ISO format.")
    parser.add_argument('end', type=convert_datetime_str, help="End datetime in ISO format.")
    return parser


def setup_alb_parser(parser):
    parser.add_argument("-e", "--external", action="store_true", dest="ext", default=True,
                        help="Analyze external ALB (default on)")
    parser.add_argument("-i", "--internal", action="store_true", dest="int", default=False,
                        help="Analyze internal ALB (default off)")
    return parser