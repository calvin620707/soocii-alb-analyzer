from datetime import datetime, timedelta


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