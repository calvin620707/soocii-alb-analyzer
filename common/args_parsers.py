from datetime import timezone

import dateutil.parser


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
    parser.add_argument("--no-external", action="store_false", dest="ext", default=True,
                        help="Exclude external ALB")
    parser.add_argument("--no-internal", action="store_false", dest="int", default=True,
                        help="Exclude internal ALB")
    return parser
