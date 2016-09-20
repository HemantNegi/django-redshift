import sys
import os
import django
import time

sys.path.append("../base")
sys.path.append("../")

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')

django.setup()


# basic django setup over.


import setproctitle
import argparse
import logging
import global_var
import signal
from redshift.models import *
from redshift.utils import SpoolManager, RedshitWriter, FileTail

logging.basicConfig(filename='redshift.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(
    description='Redshift transfer functions'
)


# Add arguments
parser.add_argument(
    '--func', type=str, help='Function name', required=True)

parser.add_argument(
    '--log_file', type=str, help='Function name', required=False)

args = parser.parse_args()


def tail_log(log_file_path):
    query = """
        INSERT INTO nginx (request_id, visitor_id, user_id, process_name,
        ip, request_type, http_status, url, redirection_url,
        event_at, adposition, device_type, device_model, network,
        network_category, utm_campaign, utm_medium, utm_source,
        utm_term, keyword, gclid, creative, source, utm_content,
        created_at)
    """
    spool_file = "tail_log.spool"
    spool_manager = SpoolManager(spool_file)
    writer = RedshitWriter(query, spool_manager, buffer_size=1000)
    with FileTail(log_file_path, spool_file) as file_tail:
        for log, spool_data in file_tail.get_next_line():
            if not log:
                time.sleep(1)

            try:
                row = parser(log, format="json")
            except:
                logger.error(
                    "Exception occurred while parsing log (%s)" % log,
                    exc_info=True
                )
                continue

            writer.write(
                (
                    row["request_id"],
                    row["visitor_id"],
                    row["user_id"],
                    row["process_name"],
                    row["ip"],
                    row["request_type"],
                    row["http_status"],
                    row["url"],
                    row["redirection_url"],
                    row["event_at"],
                    row["adposition"],
                    row["device_type"],
                    row["device_model"],
                    row["network"],
                    row["network_category"],
                    row["utm_campaign"],
                    row["utm_medium"],
                    row["utm_source"],
                    row["utm_term"],
                    row["keyword"],
                    row["gclid"],
                    row["creative"],
                    row["source"],
                    row["utm_content"],
                    row["created_at"]
                ),
                spool_data
            )




def run():

    if args.func == "tail_nginx_log":
        setproctitle.setproctitle("tail_nginx_log")
        if not args.log_file:
            raise Exception("To do tail specifify log path")

        tail_log(
            args.log_file
        )
        return

    raise Exception("Invalid function")


def kill_signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    global_var.SIGKILL = True

    sys.exit(0)


def usr2_signal_handler(signal, frame):
    global_var.SIGUSR2 = True

signal.signal(signal.SIGINT, kill_signal_handler)
signal.signal(signal.SIGUSR2, usr2_signal_handler)
run()