import subprocess
import time
import gzip
import uuid
import string
import datetime
import random
import re
import json
import pickle
import sys
import traceback
import os.path
import threading

from Queue import Empty, Queue as PyQueue

import psycopg2

from boto import kinesis

import global_var

configuration = {
    'dbname': 'coverfox',
    'user': 'coverfox',
    'pwd': 'Coverfox123',
    'host': 'logdb.cwbaksyl3qj0.ap-southeast-1.redshift.amazonaws.com',
    'port': 5439
}

kinesis_auth = {
    "aws_access_key_id": "AKIAJVTBJSWKYM6NG6UQ",
    "aws_secret_access_key": "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"
}

#user_ids = pickle.load(open("users.pickle", "rb"))
HTTP_METHODS = ["GET", "POST", "HEAD", "PUT", "OPTIONS", "DELETE", "CONNECT", "TRACE"]
rd_conn = None
dt_format = "%d/%b/%Y:%H:%M:%S +0000"
action_queue_pool = []
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
SPOOL_FILE_DIR = "/var/spool/cfox/"
# ----------------------------------------------------------------------------


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def get_local_postgresql():
    # Local postgresql connection.
    try:
        # conn = psycopg2.connect("dbname='coverfox' user='root' host='0.tcp.ngrok.io' port=48169 password='root'")
        conn = psycopg2.connect("dbname='coverfox' user='root' host='localhost' password='root'")
        cur = conn.cursor()
    except:
        print "I am unable to connect to the database"
        sys.exit()

    return conn, cur


class RedshiftConnection(object):
    __metaclass__ = Singleton

    def __init__(self, **kwargs):
        config = kwargs['config']
        try:
            conn = psycopg2.connect(
                dbname=config['dbname'],
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['pwd']
            )
            print("Redshift connection established.")
        except Exception as err:
            print(err)
            print err
            sys.exit()

        self.conn = conn

    def get_redshift_conn(self, **kwargs):
        return self.conn


def execute_redshift_query(query, fetch_result=True):
    redshift_conn = RedshiftConnection(config=configuration).get_redshift_conn()
    cur = redshift_conn.cursor()
    try:
        cur.execute(query)
        redshift_conn.commit()
        result = None
        if fetch_result:
            result = cur.fetchall()

        cur.close()
        return result
    except:
        import traceback
        traceback.print_exc()
        redshift_conn.rollback()
        print(query)


def execute_transaction(queries):
    redshift_conn = RedshiftConnection(config=configuration).get_redshift_conn()
    cur = redshift_conn.cursor()

    cur.execute("BEGIN;")
    for query in queries:
        try:
            cur.execute(query)
        except:
            import traceback
            traceback.print_exc()
            redshift_conn.rollback()
            print(query)

    redshift_conn.commit()
    return cur.fetchall()


# ----------------------------------------------------------------------------


def get_latest_file_no(logpath):
    log_files = os.listdir(logpath)
    highest = 1
    files_dict = {}
    # Store files into dict,
    for file in log_files:
        if "access" in file:
            files_dict[file] = None

            if re.match(r'^access.log\.([\d]+\.gz|[\d]+)$', file):
                file_no = int(file.split(".")[2])

                if file_no > highest:
                    highest = file_no

    return highest, files_dict


def get_next_file(log_dir):
    highest, files_dict = get_latest_file_no(log_dir)

    for item in range(1, highest + 1):
        # Get file.
        log_file = "access.log.%s" % item
        if log_file in files_dict:
            yield log_file
            continue

        gz_file = "access.log.%s.gz" % item
        if gz_file in files_dict:
            yield gz_file
            continue


def extract_file(filepath, dest):
    # Copy file to temp directory.
    if not os.path.isfile(filepath):
        raise Exception("Invalid path %s" % filepath)

    subprocess.call(["cp", "-rf", filepath, dest])
    subprocess.call(["gunzip", os.path.basename(filepath)], cwd=dest)


def id_generator(size=32, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def get_next_user_id():
    u_count = len(user_ids)
    cnt = 0
    diff = 400
    next = 400

    while True:
        if cnt == next:
            cnt += 1
            next += diff
            yield user_ids[random.randint(0, u_count - 1)][0]
        else:
            cnt += 1
            yield None


# ----------------------------------------------------------------------------

def get_field_params():
    field_params = {
        "adposition": "",
        "device_type": "",
        "device_model": "",
        "network": "",
        "network_category": "",
        "utm_campaign": "",
        "utm_medium": "",
        "utm_source": "",
        "utm_term": "",
        "keyword": "",
        "gclid": "",
        "creative": "",
        "source": "",
        "utm_content": ""
    }
    return field_params


def get_row_format():
    row_format = """
    (
    '{request_id}', '{visitor_id}', {user_id}, '{process_name}',
    '{ip}', '{request_type}', {http_status}, '{url}', '{redirection_url}',
    '{event_at}', '{adposition}', '{device_type}', '{device_model}',
    '{network}', '{network_category}', '{utm_campaign}', '{utm_medium}',
    '{utm_source}', '{utm_term}', '{keyword}', '{gclid}', '{creative}',
    '{source}', '{utm_content}', '{created_at}'
    )
    """
    row_format = row_format.replace(" ", "")
    return row_format

# ----------------------------------------------------------------------------


def write_to_spool(data, filename):
    with open(".spool", "w+") as f:
        f.write("%s %s" % (filename, data))


def get_file_point_file():
    data = None
    try:
        with open(".spool", "r") as f:
            data = f.read()

        filename, pointer = data.split()
    except:
        return (None, None)

    return filename, pointer


# ---------------------------------------------------------------------------
class Queue(object):
    def __init__(self):
        self.queue = []
        self.last_updated = None

    def put(self, value):
        self.queue.append(value)
        if not self.last_updated:
            self.last_updated = int(time.time())
            return

        if int(time.time()) - self.last_updated >= 2 and self.queue:
            self.last_updated = int(time.time())
            rows = self.get_all()
            main_query = "INSERT INTO events VALUES "

            values = ','.join(rows)
            query = "%s %s;" % (main_query, values)
            execute_redshift_query(query)
            print("Query done")

    def get_all(self):
        values = self.queue
        self.queue = []
        return values
# -----------------------------------------------------------------------------


class SpoolManager(object):
    def __init__(self, filename):
        if not filename:
            raise Exception("Invalid filename")

        self.completed = False
        self.spool_file = os.path.join(
            SPOOL_FILE_DIR,
            os.path.basename(filename)
        )

        if not os.path.isfile(self.spool_file):
            open(
                self.spool_file,
                "w+"
            ).close()

        self.file = open(
            self.spool_file,
            "r+"
        )

    def write_to_spool(self, data, filename):
        self.truncate_file()
        # Write into file.
        self.file.write("%s %s" % (filename, data))
        # TODO: Need to find proper way to write changes without closing a file.
        self.file.close()
        self.file = open(self.spool_file, "r+")

    def get_file_point_file(self):
        data = None
        try:
            data = self.file.read()
            self.file.seek(0)
            if not data:
                return (None, 0)

            filename, pointer = data.split()
            pointer = int(pointer)
        except:
            traceback.print_exc()
            return (None, 0)

        return filename, pointer

    def truncate_file(self):
        self.file.seek(0)
        self.file.truncate()
        self.file.close()
        self.file = open(self.spool_file, "r+")

    def __enter__(self):
        return self

    def exit(self):
        # Truncate file.
        if self.completed:
            self.truncate_file()

        self.file.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit()


class FileTail(object):
    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        # Close files.
        self.file.close()
        self.spool_manager.exit()

    def __init__(self, filepath, spool_file, consumer_name=None, spool_write=False):
        self.spool_manager = SpoolManager(spool_file)
        self.spool_write = spool_write
        if not os.path.isfile(filepath):
            raise Exception("Invalid file path(%s)" % filepath)

        self.file = open(filepath, "r")

        filename, pointer = self.spool_manager.get_file_point_file()
        if filename == os.path.basename(self.file.name):
            self.file.seek(pointer)
            print(pointer)
            print("SEEK")

    def get_next_line(self):
        while True:

            if global_var.SIGKILL:
                break

            line = self.file.readline()

            if self.spool_write or not global_var.SIGUSR2:
                self.spool_manager.write_to_spool(
                    self.file.tell(), os.path.basename(self.file.name)
                )

            if global_var.SIGUSR2:
                self.file.seek(0)
                self.spool_manager.truncate_file()
                global_var.SIGUSR2 = False

            yield line.replace("\n", ""), (
                self.file.tell(), os.path.basename(self.file.name)
            )

    def tail_file(self, func, tail_interval=1):
        while True:
            if global_var.SIGKILL:
                break

            line = self.file.readline()
            if not line:
                time.sleep(tail_interval)
                continue

            func(line, self.file)
            self.spool_manager.write_to_spool(
                self.file.tell(), os.path.basename(self.file.name)
            )

        # Close spool file.
        self.spool_manager.exit()


# ---------------------------------------------------------------------
class QueryWriteThread(threading.Thread):
    def __init__(
        self,
        query,
        data_queue,
        spool_manager,
        buffer_size=1000,
        timeout=1000
    ):
        threading.Thread.__init__(self)
        self.spool_manager = spool_manager
        self.data_queue = data_queue
        self.query = query
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.current_timeout = 0
        self.query_items = []
        self.current_time = 0

    def prepare_execute_query(self):
        # Get all data from data queue until None or empty.
        query = self.query
        query += " VALUES "
        start = True
        count = 0
        # Get upto buffer limit.
        values = []
        if self.data_queue.empty():
            return

        print(query)
        for _ in xrange(self.buffer_size):
            data = self.data_queue.get(timeout=1)
            self.data_queue.task_done()
            if not data:
                print("NONE")
                break

            count += 1

            if start:
                values = "("
                start = False
            else:
                values = ", ("

            spool_data = data["spool_data"]
            records = data["records"]

            for item in records:
                if item is None:
                    values += 'NULL,'
                else:
                    values += "'%s'," % item

            values = values[0:-1]
            values += ")"
            query += values

            if self.data_queue.empty() or count >= self.buffer_size:
                query += ";"
                print(query)
                execute_redshift_query(query, fetch_result=False)
                self.spool_manager.write_to_spool(*spool_data)
                count = 0
                break

    def run(self):
        while True:
            if global_var.SIGKILL:
                break

            time.sleep(1)
            self.current_time += 1

            if self.current_time >= self.timeout:
                # Reset counter.
                self.prepare_execute_query()
                self.current_time = 0

            # Check queue length.
            if self.data_queue.qsize() >= self.buffer_size:
                self.prepare_execute_query()
                self.current_time = 0


class RedshitWriter(object):
    def __init__(
        self,
        query,
        spool_manager,
        buffer_size=1000,
        timeout=1000,
    ):
        self.data_queue = PyQueue()
        query_execute_thread = QueryWriteThread(
            query,
            self.data_queue,
            spool_manager,
            buffer_size=buffer_size,
            timeout=timeout
        )
        self.buffer_size = buffer_size
        query_execute_thread.daemon = True
        query_execute_thread.start()
        self.cur_items = 0
        self.spool_manager = spool_manager

    def write(self, records, spool_data):
        self.data_queue.put(
            {
                "records": records,
                "spool_data": spool_data
            }
        )

    def finalize(self):
        self.data_queue.put(None)
        self.action_queue.put("execute_query")

    def stop(self):
        self.action_queue.put("STOP")


# ---------------------------------------------------------------------
class FileReader(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def __init__(self, filepath, spool_file, file_format):
        if not os.path.isfile(filepath):
            raise Exception("Invalid file (%s)" % filepath)

        if file_format == "gzip":
            self.file = gzip.open(filepath, "r")
        else:
            self.file = open(filepath, "r")

        self.spool_manager = SpoolManager(spool_file)
        filename, pointer = self.spool_manager.get_file_point_file()
        if filename == os.path.basename(self.file.name):
            self.file.seek(pointer)
            print(pointer)
            print("SEEK")

    def get_next_line(self):
        while True:
            if global_var.SIGKILL:
                break

            line = self.file.readline()
            yield line, (
                self.file.tell(), os.path.basename(self.file.name)
            )


def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""


def write_errors(data):
    with open("errors.log", "a+") as f:
        f.write("\n")
        f.write(data)


def write_info(data):
    with open("info.log", "a+") as f:
        f.write("\n")
        f.write(data)


def get_user_full_data():
    instance = None
    """
    with open("user_full_data.pkl", "rb") as f:
        instance = pickle.load(f)

    return instance
    """
    return instance


# ---------------------------------------------------------------------
def get_from_group2(grp2):
    g = grp2.split()
    # Check for groups
    if g[0] not in HTTP_METHODS:
        raise Exception("Invalid http method %s" % grp2[0])

    del g[0]

    # Get url.
    url = ""
    for item in grp2:
        if "HTTP/" in item:
            break

        url += item


# ---------------------------------------------------------------------
class Kinesis(object):
    def __init__(
        self,
        consumer_name,
        stream,
        shard=None,
        starting_point=None,
        region='ap-southeast-1'
    ):
        self.connection = kinesis.connect_to_region(region, **kinesis_auth)
        self.stream = stream
        self.shard = shard
        self.shard_starting_point = SpoolManager(
            "kinesis_sync"
        )
        _, self.starting_point = self.shard_starting_point.get_file_point_file()
        self.starting_point = None
        shards = self._list_shards(stream)
        if self.shard:
            for _shard in shards:
                if _shard["ShardId"] == shard:
                    break
        else:
            self.shard = shards[0]["ShardId"]

        self.shard_iterator = self._get_stream_iterator(
            self.stream,
            self.shard
        )

    def put_records(self, record, partation_key='activity'):
        self.connection.put_record(
            self.stream,
            record,
            partation_key
        )

    def get_next_record(self):
        while True:
            out = self.connection.get_records(self.shard_iterator)
            self.shard_iterator = out["NextShardIterator"]
            yield out["Records"]

    def write_seq_no(self, seq_no):
        self.shard_starting_point.write_to_spool(seq_no, "null")

    def _get_stream_iterator(self, stream, shard):
        shard_iterator = self.connection.get_shard_iterator(
            stream,
            shard,
            'LATEST',
            starting_sequence_number=self.starting_point
        )["ShardIterator"]
        return shard_iterator

    def _list_shards(self, stream):
        shards = self.connection.describe_stream(
            stream
        )['StreamDescription']['Shards']

        return shards


class QueryValue(object):
    def __init__(self):
        self.value = []

    def add_value(self, value, type):
        self.value.append([value, type])

    def get_value_str(self):
        query = "("
        for val, typ in self.value:
            found = False
            if typ.lower() == "int":
                query += "%s" % val
                found = True

            if typ.lower() == "str":
                query += "'%s'" % val
                found = True

            if typ.lower() == "null":
                query += "NULL"
                found = True

            if typ.lower() == "field":
                query += "%s" % val
                found = True

            if found:
                query += ","

        query = query[0:-1]
        query += ')'
        return query


class QueryUpdateValue(object):
    def __init__(self):
        self.value = []

    def add_value(self, field, value, type):
        self.value.append([field, value, type])

    def get_value_str(self):
        query = ""
        for field, val, typ in self.value:
            found = False
            if typ.lower() == "int":
                query += "%s=%s" % (field, val)
                found = True

            if typ.lower() == "str":
                query += "%s='%s'" % (field, val)
                found = True

            if typ.lower() == "null":
                query += "%s=NULL" % (field)
                found = True

            if found:
                query += ","

        query = query[0:-1]
        return query


def data_to_insert_query(data, table, field_type={}):
    query = "INSERT INTO %s" % table

    query_fields = QueryValue()
    query_val = QueryValue()
    for key, val in data.iteritems():
        field_d_type = "str"
        if key in field_type:
            field_d_type = field_type[key]

        query_fields.add_value(key, "field")
        query_val.add_value(val, field_d_type)

    # Prepare final query.
    query += " %s" % query_fields.get_value_str()
    query += " VALUES%s" % query_val.get_value_str()
    query += ";"
    return query


def data_to_update_query(data, table, where_data="", field_type={}):
    query = "UPDATE %s " % table

    q_fields = QueryUpdateValue()
    for key, val in data.iteritems():
        q_fields.add_value(key, val, "str")

    # Prepare final query.
    query += "SET %s" % q_fields.get_value_str()
    if where_data:
        query += " WHERE %s;" % where_data

    return query


class ActivityActionsUtils(object):
    def prepare_activity_data(self, activity_data):
        # Prepare data to do insertion into users table.
        activity_data["created_at"] = "convert_timezone('Universal', 'Asia/Kolkata', sysdate)"

        if activity_data["request_id"] is not None:
            del activity_data["request_id"]

        return activity_data

    def person_id_exists(self, person_id):
        query = "SELECT count(person_id) from user_model " \
                " WHERE person_id = '%s'" % person_id
        result = execute_redshift_query(query)
        if result[0][0] >= 1:
            return True,
        else:
            return False

    def get_user_model_by_pid(self, pid, fields):
        field_qry = ""
        for field in fields:
            field_qry += "%s," % field

        field_qry = field_qry[0:-1]
        query = "SELECT %s FROM user_model " \
                "WHERE person_id = '%s'" % (field_qry, pid)

        result = execute_redshift_query(query)
        return result

    def delete_alias_by_usr_model_id(self, usr_model_id):
        query = "DELETE FROM alias_user_model WHERE " \
                "user_model_id = '%s'" % usr_model_id
        execute_redshift_query(query)

    def add_alias(self, alias_id, user_model_id):
        query = "INSERT INTO alias_user_model " \
                " (alias_id, user_model_id) " \
                " VALUES ('%s', %s)" % \
                (alias_id, user_model_id)

        execute_redshift_query(query)

    def check_alias_user_model_exist(self, alias_id, usr_model_id):
        query = """
            SELECT count(id) alias_user_model
            WHERE alias_id = '%s'
            AND user_model_id = '%s'
        """ % (alias_id, usr_model_id)
        result = execute_redshift_query(query)
        if result:
            return True
        else:
            return False

    def get_alias_id_by(self, email=None, phone=None):
        # Get user model with email and phone.
        if not email and not phone:
            return False

        where = ""

        if phone:
            where += "phone = '%s'" % phone
        else:
            where += "phone = NULL"

        if email:
            where += " AND email = '%s' " % email
        else:
            where += "email = NULL"

        query = """
            SELECT id FROM user_model
            WHERE %s
        """ % where
        result = execute_redshift_query(query)

        id = None
        if result:
            id = result[0][0]
        else:
            return str(uuid.uuid4()), True

        if id:
            query = """
                SELECT alias_id from alias_user_model
                WHERE user_model_id = '%s'
            """ % id
            result = execute_redshift_query(query)
            if result:
                return result[0][0], False
            else:
                return uuid.uuid4(), True
        else:
            return False

    def update_person_id(self, person_id, data):
        if "created_at" in data:
            del data["created_at"]

        query = data_to_update_query(
            data,
            "user_model",
            "person_id = '%s'" % person_id,
        )
        execute_redshift_query(query)

    def get_id_by_pid(self, pid):
        query = """
            SELECT id from user_model
            WHERE person_id = '%s'
        """ % pid
        return execute_redshift_query(query)[0][0]

    def insert_data_into_user_model(self, data):
        query = data_to_insert_query(
            data,
            'user_model',
            field_type={
                "created_at": "field"
            }
        )
        execute_redshift_query(query)

    def insert_data_into_user_model_get_id(self, data):
        query = data_to_insert_query(
            data,
            'user_model',
            field_type={
                "created_at": "field"
            }
        )
        result = execute_transaction([query, "SELECT MAX(id) from user_model;"])
        # TODO: Handle this case.
        if result:
            return result[0][0]

    def check_user_exist(self, user_id):
        query = """
            SELECT count(id) FROM user_model
            WHERE user_id = '%s'
        """ % user_id
        result = execute_redshift_query(query)
        if result:
            if result[0][0] >= 1:
                return True
            else:
                return False
        else:
            return False

    def create_user_data(self, data):
        data["created_at"] = "convert_timezone('Universal', 'Asia/Kolkata', sysdate)"
        query = data_to_insert_query(
            data,
            'user_model',
            field_type={"created_at": "field"}
        )
        execute_redshift_query(query)


class ActivityActions(object):
    """
    # TODO: Need to implement transactions.
    """
    def __init__(self):
        self.activity_utils = ActivityActionsUtils()

    def _update_user_model_field(self, field, value, person_id=None, user_id=None):
        if not user_id and person_id:
            raise Exception("Invalid person_id and user_id both cannot be None")

        where_query = ""
        # Update user model.
        if user_id:
            where_query += "user_id = '%s'" % user_id

        if person_id:
            where_query += "person_id = '%s'" % person_id

        query = """
            UPDATE user_model
            set %s = '%s'
            WHERE %s;
        """ % (field, value, where_query)
        execute_redshift_query(query, fetch_result=True)

    def _insert_activity(self, data):
        data["created_at"] = data["created_at"].strftime(
            "%d/%b/%Y:%H:%M:%S.%f"
        )

        query = """
            INSERT INTO activity (activity_type, server_id, user_id, person_id,
            request_id, app, user_agent, data1, data2, data, created_at)
            VALUES (
        """

        values = ["activity_type", "server_id", "user_id", "person_id", "request_id",
                  "app", "user_agent", "data1", "data2", "data", "created_at"]
        for item in values:
            if data[item]:
                if "'" in data[item]:
                    query += "'%s'," % data[item].replace("'", "\'")
                else:
                    query += "'%s'," % data[item]
            else:
                query += "NULL,"

        query = query[0:-1]
        query += ");"

        execute_redshift_query(query, fetch_result=True)

    def website_visit(self, data):
        self._insert_activity(data)

    def health_quote_viewed(self, data):
        self._insert_activity(data)

    def car_quote_viewed(self, data):
        self._insert_activity(data)

    def create_identity(self, data):
        self._insert_activity(data)

        # Prepare activity data to insert into db.
        activity_data = self.activity_utils.prepare_activity_data(data)
        activity_data["data"] = json.loads(activity_data["data"])
        activity_data["data"]["user_id"] = activity_data["user_id"]
        activity_data["data"]["person_id"] = activity_data["person_id"]
        # Check if activity data has person id.
        if activity_data["person_id"] is not None:
            person_id = activity_data["person_id"]
            pid_exists = self.activity_utils.person_id_exists(
                person_id
            )
            if pid_exists:
                if "phone" not in activity_data:
                    activity_data["data"]["phone"] = None

                if "email" not in activity_data:
                    activity_data["data"]["email"] = None

                alias_id, new = self.activity_utils.get_alias_id_by(
                    activity_data["data"]["email"],
                    activity_data["data"]["phone"]
                )
                # Modify person.
                self.activity_utils.update_person_id(person_id, activity_data)

                uid = self.activity_utils.get_id_by_pid(person_id)
                self.activity_utils.delete_alias_by_usr_model_id(
                    uid
                )
                self.activity_utils.add_alias(
                    alias_id,
                    uid
                )
                return
            else:
                # ------------------------------------------------------------
                # All insert logic.
                # ------------------------------------------------------------
                alias_id, _ = self.activity_utils.get_alias_id_by(
                    email=activity_data["data"]["email"],
                    phone=activity_data["data"]["phone"]
                )

                user_model_id = self.activity_utils.insert_data_into_user_model_get_id(
                    activity_data["data"]
                )

                if activity_data["data"]["email"] or activity_data["data"]["phone"]:
                    if not alias_id:
                        # TODO: Need to print log for this.
                        return

                    self.activity_utils.add_alias(
                        alias_id,
                        user_model_id
                    )

        if activity_data["user_id"] is not None:
            # Check if user already exist or not.
            user_id = activity_data["user_id"]
            u_exist = self.activity_utils.check_user_exist(user_id)
            if u_exist:
                # TODO: Need to raise exception or warning that user
                # already created so cannot be created again.
                pass
            else:
                # Insert new user.
                uid = self.activity_utils.insert_data_into_user_model_get_id(
                    activity_data["data"]
                )

                # Insert alias.
                alias_id, _ = self.activity_utils.get_alias_id_by(
                    activity_data["data"]["email"],
                    activity_data["data"]["phone"]
                )
                self.activity_utils.add_alias(
                    alias_id, uid
                )

        return


def clean_field(field_val):
    if field_val == "-":
        return None
    else:
        return field_val


def parse_kinesis_stream(line):
    # Remove sys log.
    line = re.sub(r'.*.py\:', '', line).lstrip()

    activity_data = {}
    # Parse log.
    splitted_line = line.split()
    basic_data = splitted_line[0:9]
    del splitted_line[0:9]
    extra = ' '.join(splitted_line)
    data1 = find_between(extra, "[[", "]]")
    data2 = find_between(extra, "**", "**")

    data1 = data1 if data1 else "-"
    data2 = data2 if data2 else "-"

    extra = re.sub(r'\[\[(.*)\]\]', "", extra)
    extra = re.sub(r'\(\((.*)\)\)', "", extra)

    try:
        json_data = json.loads(extra)
        extra = json.dumps(json_data)
    except ValueError:
        raise Exception("Invalid json %s" % extra)

    dt_format = "%Y-%m-%dT%H:%M:%S.%f"

    dt = datetime.datetime.strptime(basic_data[0], dt_format)
    activity_data["created_at"] = dt
    activity_data["ip"] = basic_data[1]
    activity_data["server_id"] = basic_data[2]
    activity_data["user_id"] = clean_field(basic_data[3])
    activity_data["person_id"] = clean_field(basic_data[4])
    activity_data["request_id"] = clean_field(basic_data[5])
    activity_data["app"] = basic_data[6]
    activity_data["activity_type"] = basic_data[7]
    activity_data["user_agent"] = clean_field(basic_data[8])
    activity_data["data1"] = clean_field(data1)
    activity_data["data2"] = clean_field(data2)
    activity_data["data"] = extra
    return activity_data
