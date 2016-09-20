import re
import sys
import signal
import traceback
import json
import time
import os
import copy
import urlparse
import uuid
import argparse
import logging

from datetime import datetime
from dateutil import tz

import setproctitle

import global_var
from utils import Queue, get_field_params, get_next_user_id, \
    find_between, id_generator, get_row_format, get_next_file, \
    SpoolManager, get_user_full_data, Kinesis, ActivityActions, \
    FileTail, parse_kinesis_stream, RedshitWriter, FileReader

logging.basicConfig(filename='redshift.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(
    description='Redshift transfer functions'
)
# ---------------------------------------------------------------------
# Compiled regexes
DATETIME_MATCH_REGEX = r'[0-9]{2}\/.*\/[0-9]{4}\:[0-9]{2}\:[0-9]{2}\:[0-9]{2}\s\+[\d]+'
datetime_parser = re.compile(DATETIME_MATCH_REGEX)

GROUP_BY_QUOTE_REGEX = r'\".*\"'
group_by_quote = re.compile(GROUP_BY_QUOTE_REGEX)

timezone = tz.gettz('Asia/Kolkata')
# ---------------------------------------------------------------------

# Add arguments
parser.add_argument(
    '--func', type=str, help='Function name', required=True)

parser.add_argument(
    '--log_file', type=str, help='Function name', required=False)

parser.add_argument(
    '--nginx_dir', type=str, help='Function name', required=False)

parser.add_argument(
    '--kinesis_stream', type=str, help='Function name', required=False
)

parser.add_argument(
    '--kinesis_shard', type=str, help='Function name', required=False
)

args = parser.parse_args()
q_row = []
tail_queue = Queue()


def create_param(params):
    fields = {
        "adposition": "adposition",
        "device": "device_type",
        "device_model": "device_model",
        "network": "network",
        "category": "network_category",
        "utm_campaign": "utm_campaign",
        "utm_medium": "utm_medium",
        "utm_source": "utm_source",
        "utm_term": "utm_term",
        "keyword": "keyword",
        "gclid": "gclid",
        "creative": "creative",
        "source": "source",
        "utm_content": "utm_content"
    }

    new_params = copy.copy(get_field_params())

    for key, val in params.iteritems():
        if key in fields:
            new_params[fields[key]] = val[0]

    return new_params


def parser(log, format="str"):
    row = ""
    splitted = log.split()
    gr2 = group_by_quote.search(log).group().replace('"', '').split()
    # Request ID
    request_id = str(uuid.uuid4())
    # Visitor ID
    visitor_id = id_generator()
    # UserID
    user_id = get_next_user_id().next()
    process_name = "nginx"

    ip = splitted[0]

    request_type = gr2[0]

    # Get url with different way to deal with database.
    url = urlparse.urlparse(gr2[1]).path
    # 06/Nov/2015:06:47:45 +0000
    # str_dt = re.search(r'[0-9]{2}\/.*\/[0-9]{4}\:[0-9]{2}\:[0-9]{2}\:[0-9]{2}\s\+[\d]+', log).group(),
    # str_dt = datetime_parser.search(log).group(),
    str_dt = find_between(log, "[", "]").lstrip()

    dt_format = "%d/%b/%Y:%H:%M:%S +0000"

    dt = datetime.strptime(str_dt, dt_format)
    try:
        dt = dt.replace(tzinfo=tz.gettz('UTC')).astimezone(timezone)
    except:
        import traceback
        traceback.print_exc()
    # row += "%s|" % dt.strftime("%Y-%m-%d %H:%M:%S")
    status = int(gr2[3])
    # row += "%s|" % status

    params = create_param(urlparse.parse_qs(urlparse.urlparse(gr2[1]).query))
    redirection_url = gr2[5]
    if redirection_url == "-":
        redirection_url = ""

    # prepare data.
    params["request_id"] = request_id
    params["redirection_url"] = redirection_url
    params["user_id"] = user_id if user_id else 'NULL'
    params["visitor_id"] = visitor_id
    params["process_name"] = process_name
    params["ip"] = ip
    params["request_type"] = request_type
    params["url"] = url
    params["event_at"] = "%s" % dt.strftime("%Y-%m-%d %H:%M:%S")
    params["http_status"] = status
    params["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row = ""
    if format == "str":
        row = get_row_format().format(
            **params
        )
    elif format == "tuple":
        row = params.values()
    elif format == "json":
        return params

    return row


def read_file(log_file=None, file_format="txt", spool_manager=None):
    query = """
        INSERT INTO nginx (request_id, visitor_id, user_id, process_name,
        ip, request_type, http_status, url, redirection_url,
        event_at, adposition, device_type, device_model, network,
        network_category, utm_campaign, utm_medium, utm_source,
        utm_term, keyword, gclid, creative, source, utm_content,
        created_at)
    """
    writer = RedshitWriter(
        query,
        buffer_size=1000,
        timeout=1000,
    )

    with FileReader(log_file, "%s.spool" % log_file, "txt") as file_reader:
        for log, spool_data in file_reader.get_next_line():
            try:
                row = parser(log, format="json")
            except:
                logger.error(
                    "Exception occurred while parsing log (%s)" % log,
                    exc_info=True
                )

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


def parse_nginx_dir(directory, spool_manager=None):
    if not os.path.isdir(directory):
        raise Exception("Invalid directory %s path" % directory)

    filename, _ = spool_manager.get_file_point_file()
    resume_check = True
    for log_file in get_next_file(directory):
        if filename and resume_check is True:
            if log_file != filename:
                continue

        resume_check = False
        logger.info("Loading %s" % log_file)
        print("Loading %s" % log_file)
        if log_file.endswith(".gz"):
            # Get log file.
            read_file(
                os.path.join(directory, log_file),
                spool_manager,
                format="gzip"
            )
        else:
            read_file(
                os.path.join(directory, log_file),
                spool_manager,
                format="gzip"
            )


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


def insert_user_data():
    user_data = get_user_full_data()
    query = "INSERT INTO users VALUES "
    for user in user_data:
        # Create query
        query += "('%s', '%s', '%s),'" % \
            (
                user["id"],
                user["email"],
                user["username"]
            )
        query += ","

        if query[-1] == ",":
            query = query[0:-1]

        query += ";"


def sync_activity_log(log_file):
    # Do kinesis connection.
    kinesis_instance = Kinesis("", stream="activity")
    with FileTail(log_file, "activity_log.spool", spool_write=True) as follower:
        for line, _ in follower.get_next_line():
            if not line or line == "\n":
                continue

            # TODO: Need to write converter to modify logs to proper format.
            print(line.replace("\n", ""))
            kinesis_instance.put_records(line)


def consume_kinesis_stream(stream, shard=None, starting_point=None):
    kinesis_instance = Kinesis("Consumer1", stream)
    activity_actions = ActivityActions()
    for record in kinesis_instance.get_next_record():
        for item in record:
            activity = parse_kinesis_stream(item["Data"])
            if not hasattr(ActivityActions, activity["activity_type"]):
                logger.error("Invalid activity %s" % activity["activity_type"])
                logger.error("Activity log %s" % record)
                return

            getattr(activity_actions, activity["activity_type"])(activity)
            kinesis_instance.write_seq_no(item["SequenceNumber"])

        time.sleep(0.5)


def print_stream_shards(stream):
    kinesis_instance = Kinesis()
    print(json.dumps(kinesis_instance.list_shards(stream)))


def run():
    if args.func == "read_file":
        if not args.log_file:
            raise Exception("Log file is required.")

        read_file(args.log_file)

        return

    if args.func == "parse_nginx_dir":
        if not args.nginx_dir:
            raise Exception("Nginx directory path required")

        with SpoolManager("nginx_dir.spool") as spool_manager:
            parse_nginx_dir(args.nginx_dir, spool_manager=spool_manager)

        return

    if args.func == "tail_nginx_log":
        setproctitle.setproctitle("tail_nginx_log")
        if not args.log_file:
            raise Exception("To do tail specifify log path")

        tail_log(
            args.log_file
        )
        return

    if args.func == "sync_activity_log":
        if not args.log_file:
            raise Exception("To do activity sync specify log path")

        sync_activity_log(
            args.log_file
        )

        return

    if args.func == "consume_kinesis_stream":
        if not args.kinesis_stream:
            raise Exception("To consume kinesis stream provide stream name")

        if not args.kinesis_stream:
            raise Exception("Kinesis stream provide kiness stream")

        try:
            consume_kinesis_stream(
                args.kinesis_stream,
                shard=args.kinesis_shard
            )
        except:
            traceback.print_exc()

        return

    if args.func == "kinesis_shards":
        if not args.kinesis_stream:
            raise Exception("Kinesis stream provide kiness stream")

        print_stream_shards(args.kinesis_stream)
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
