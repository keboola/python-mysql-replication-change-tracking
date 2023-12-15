# -*- coding: utf-8 -*-
import functools
import logging
import struct
from distutils.version import LooseVersion
from typing import List, Callable

import backoff
import pymysql
from pymysql.constants.COMMAND import COM_BINLOG_DUMP, COM_REGISTER_SLAVE
from pymysql.cursors import DictCursor

from .constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT, QUERY_EVENT
from .ddl_parser import TableSchemaChange, TableChangeType, AlterStatementParser
from .event import (
    QueryEvent, RotateEvent, FormatDescriptionEvent,
    XidEvent, GtidEvent, StopEvent, XAPrepareEvent,
    BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    HeartbeatLogEvent, NotImplementedEvent, MariadbGtidEvent, QueryEventWithSchemaChanges)
from .exceptions import BinLogNotEnabled, SchemaOffsyncError
from .gtid import GtidSet
from .packet import BinLogPacketWrapper
from .row_event import (
    UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent, TableMapEvent)

try:
    from pymysql.constants.COMMAND import COM_BINLOG_DUMP_GTID
except ImportError:
    # Handle old pymysql versions
    # See: https://github.com/PyMySQL/PyMySQL/pull/261
    COM_BINLOG_DUMP_GTID = 0x1e

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]


class TableColumnSchemaCache:
    """
    Container object for table Schema cache. Caches table column schema based on table name.

    The column schema object is a dictionary:

    ```
                   stream-id: {
                        'COLUMN_NAME': 'name',
                        'ORDINAL_POSITION': 1,
                        'COLLATION_NAME': None,
                        'CHARACTER_SET_NAME': None,
                        'COLUMN_COMMENT': None,
                        'COLUMN_TYPE': 'BLOB',
                        'COLUMN_KEY': ''
                    }
    ```

    """

    def __init__(self, table_schema_cache: dict, table_schema_current: dict = None,
                 cache_index_callable: Callable = lambda a, b: a + '-' + b):
        """

        Args:
            table_schema_cache: Column schemas cached from the last time. It needs to be updated with each ALTER event.
            table_schema_current:
                internal cache of column schema that is actual at the time of execution
                (possibly newer than table_schema_cache)
        """
        self.table_schema_cache = table_schema_cache or {}
        self.__table_indexes = {}

        # internal cache of column schema that is actual at the time of execution
        #         (possibly newer than table_schema_cache)
        self._table_schema_current = table_schema_current or {}
        self._cache_index_generator = cache_index_callable

    def _get_db_default_schema(self):
        table_schema = {}
        if self._table_schema_current:
            key = next(iter(self._table_schema_current))
            current_schema = self._table_schema_current[key]
            if current_schema:
                table_schema = current_schema[0]

        if not table_schema.get('DEFAULT_CHARSET'):
            logging.warning('No default charset found, using utf8',
                            extra={"full_message": self._table_schema_current})
        return table_schema.get('DEFAULT_CHARSET', 'utf8')

    @staticmethod
    def build_column_schema(column_name: str, ordinal_position: int, column_type: str, is_primary_key: bool,
                            collation=None,
                            character_set_name=None, column_comment=None) -> dict:
        if is_primary_key:
            key = 'PRI'
        else:
            key = ''
        return {
            'COLUMN_NAME': column_name,
            'ORDINAL_POSITION': ordinal_position,
            'COLLATION_NAME': collation,
            'CHARACTER_SET_NAME': character_set_name,
            'COLUMN_COMMENT': column_comment,
            'COLUMN_TYPE': column_type,
            'COLUMN_KEY': key
        }

    def is_current_schema_cached(self, schema: str, table: str):
        if self._table_schema_current.get(self.get_table_cache_index(schema, table)):
            return True
        else:
            return False

    def update_current_schema_cache(self, schema: str, table: str, column_schema: []):
        """
        Update internal cache of column schema that is actual at the time of execution
        (possibly newer than table_schema_cache)
        Args:
            column_schema:

        Returns:

        """
        index = self.get_table_cache_index(schema, table)
        self._table_schema_current[index] = column_schema

    def get_column_schema(self, schema: str, table: str) -> List[dict]:
        index = self.get_table_cache_index(schema, table)
        return self.table_schema_cache.get(index)

    def update_table_ids_cache(self, schema: str, table: str, mysql_table_id: int):
        """
        Keeps MySQL internal table_ids cached

        Args:
            schema:
            table:
            mysql_table_id: Internal Mysql table id

        Returns:

        """
        index = self.get_table_cache_index(schema, table)
        if not self.__table_indexes.get(index):
            self.__table_indexes[index] = set()

        self.__table_indexes[index].add(mysql_table_id)

    def invalidate_table_ids_cache(self):
        self.__table_indexes = {}

    def get_table_ids(self, schema: str, table: str):
        """
        Returns internal table ID from cache.
        Args:
            schema:
            table:

        Returns:

        """
        index = self.get_table_cache_index(schema, table)
        return self.__table_indexes.get(index, [])

    def set_column_schema(self, schema: str, table: str, column_schema: List[dict]):
        index = self.get_table_cache_index(schema, table)
        self.table_schema_cache[index] = column_schema

    def get_table_cache_index(self, schema: str, table: str):
        # index as not case sensitive
        return self._cache_index_generator(schema, table)

    def update_cache(self, table_change: TableSchemaChange):
        """
        Updates schema cache based on table changes.

        Args:
            table_changes:

        Returns:

        """

        if table_change.type == TableChangeType.DROP_COLUMN:
            self.update_cache_drop_column(table_change)
        elif table_change.type == TableChangeType.ADD_COLUMN:
            self.update_cache_add_column(table_change)

    def update_cache_drop_column(self, drop_change: TableSchemaChange):
        index = self.get_table_cache_index(drop_change.schema, drop_change.table_name)
        column_schema = self.table_schema_cache.get(index, [])

        # drop column if exists
        drop_at_position = None
        update_ordinal_position = False
        new_schema = []
        # 1 based
        for idx, col in enumerate(column_schema, start=1):
            if col['COLUMN_NAME'].upper() == drop_change.column_name.upper():
                # mark and skip
                update_ordinal_position = True
                continue

            if update_ordinal_position:
                # shift ordinal position
                col['ORDINAL_POSITION'] = idx - 1
            new_schema.append(col)

        if not update_ordinal_position:
            raise SchemaOffsyncError(f'Dropped column: "{drop_change.column_name}" '
                                     f'is already missing in the provided starting '
                                     f'schema of table {drop_change.table_name} => may lead to value shift!'
                                     f' The affected query is: {drop_change.query}')

        if not column_schema:
            # should not happen
            raise SchemaOffsyncError(f'Table {index} not found in the provided table schema cache!')

        self.table_schema_cache[index] = new_schema

    def __add_column_at_position(self, original_schema, added_column, after_col, table_name):
        new_schema = []
        # add column if not exists
        update_ordinal_position = False
        # 1 based
        for idx, col in enumerate(original_schema, start=1):

            # on first position
            if idx == 1 and after_col == '':
                added_column['ORDINAL_POSITION'] = 1
                update_ordinal_position = True
                new_schema.append(col)

            # after specific
            elif after_col and col['COLUMN_NAME'].upper() == after_col.upper():
                added_column['ORDINAL_POSITION'] = idx + 1
                # mark and add both
                new_schema.append(col)
                new_schema.append(added_column)
                update_ordinal_position = True

            elif update_ordinal_position:
                # shift ordinal position of others
                col['ORDINAL_POSITION'] = idx + 1
                new_schema.append(col)
            else:
                # otherwise append unchanged
                new_schema.append(col)

        if not update_ordinal_position:
            raise SchemaOffsyncError(f'Dropped column: "{added_column["COLUMN_NAME"]}" in table {table_name}'
                                     f'is already missing in the provided starting schema => may lead to value shift!')
        return new_schema

    def update_cache_add_column(self, add_change: TableSchemaChange):
        index = self.get_table_cache_index(add_change.schema, add_change.table_name)
        column_schema = self.table_schema_cache.get(index, [])

        logging.debug(f'Current column schema cache: {self.table_schema_cache}, index: {index}')
        column_names = [c['COLUMN_NAME'].upper() for c in column_schema]
        if not column_names:
            raise RuntimeError(f'The schema cache for table {index} is not initialized!')

        if add_change.column_name.upper() in column_names:
            logging.warning(f'The added column "{add_change.column_name.upper()}" is already present '
                            f'in the schema "{index}", skipping.')
            return
        logging.debug(f"New schema ADD change received {add_change}")
        added_column = self._build_new_column_schema(add_change)
        logging.debug(f"New column schema built {added_column}")
        new_schema = []
        # get after_column
        if add_change.first_position:
            after_col = ''
        elif add_change.after_column:
            after_col = add_change.after_column
        else:
            # add after last
            added_column['ORDINAL_POSITION'] = len(column_schema) + 1
            column_schema.append(added_column)
            new_schema = column_schema

        # exit
        if not new_schema:
            new_schema = self.__add_column_at_position(column_schema, added_column, after_col, add_change.table_name)

        if not column_schema:
            # should not happen
            raise SchemaOffsyncError(f'Table {index} not found in the provided table schema cache!')

        self.table_schema_cache[index] = new_schema

    def _build_new_column_schema(self, table_change: TableSchemaChange) -> dict:
        index = self.get_table_cache_index(table_change.schema, table_change.table_name)

        current_schema = self._table_schema_current.get(index, [])
        if not current_schema:
            logging.warning(f'Table {table_change.table_name} not found in current schema cache.',
                            extra={'full_message': self._table_schema_current})
        existing_col = None

        # check if column exists in current schema
        # this allows to get all column metadata properly in case
        # we missed some ALTER COLUMN statement, e.g. for changing datatypes
        for c in current_schema:
            logging.debug(
                f"Added column '{table_change.column_name.upper()}' "
                f"exists in the current schema: {current_schema}")
            if c['COLUMN_NAME'].upper() == table_change.column_name.upper():
                # convert name to upper_case just in case
                # TODO: consider moving this to current_schema build-up
                c['COLUMN_NAME'] = c['COLUMN_NAME'].upper()
                existing_col = c

        if existing_col:
            new_column = existing_col
        else:
            # add metadata from the ALTER event
            is_pkey = table_change.column_key == 'PRI'

            charset_name = table_change.charset_name or self._get_db_default_schema()
            new_column = self.build_column_schema(column_name=table_change.column_name.upper(),
                                                  # to be updated later
                                                  ordinal_position=0,
                                                  column_type=table_change.data_type,
                                                  is_primary_key=is_pkey,
                                                  collation=table_change.collation,
                                                  character_set_name=charset_name
                                                  )

        return new_column


class ReportSlave(object):
    """Represent the values that you may report when connecting as a slave
    to a master. SHOW SLAVE HOSTS related"""

    hostname = ''
    username = ''
    password = ''
    port = 0

    def __init__(self, value):
        """
        Attributes:
            value: string or tuple
                   if string, then it will be used hostname
                   if tuple it will be used as (hostname, user, password, port)
        """

        if isinstance(value, (tuple, list)):
            try:
                self.hostname = value[0]
                self.username = value[1]
                self.password = value[2]
                self.port = int(value[3])
            except IndexError:
                pass
        elif isinstance(value, dict):
            for key in ['hostname', 'username', 'password', 'port']:
                try:
                    setattr(self, key, value[key])
                except KeyError:
                    pass
        else:
            self.hostname = value

    def __repr__(self):
        return '<ReportSlave hostname=%s username=%s password=%s port=%d>' % \
            (self.hostname, self.username, self.password, self.port)

    def encoded(self, server_id, master_id=0):
        """
        server_id: the slave server-id
        master_id: usually 0. Appears as "master id" in SHOW SLAVE HOSTS
                   on the master. Unknown what else it impacts.
        """

        # 1              [15] COM_REGISTER_SLAVE
        # 4              server-id
        # 1              slaves hostname length
        # string[$len]   slaves hostname
        # 1              slaves user len
        # string[$len]   slaves user
        # 1              slaves password len
        # string[$len]   slaves password
        # 2              slaves mysql-port
        # 4              replication rank
        # 4              master-id

        lhostname = len(self.hostname.encode())
        lusername = len(self.username.encode())
        lpassword = len(self.password.encode())

        packet_len = (1 +  # command
                      4 +  # server-id
                      1 +  # hostname length
                      lhostname +
                      1 +  # username length
                      lusername +
                      1 +  # password length
                      lpassword +
                      2 +  # slave mysql port
                      4 +  # replication rank
                      4)  # master-id

        MAX_STRING_LEN = 257  # one byte for length + 256 chars

        return (struct.pack('<i', packet_len) +
                bytes(bytearray([COM_REGISTER_SLAVE])) +
                struct.pack('<L', server_id) +
                struct.pack('<%dp' % min(MAX_STRING_LEN, lhostname + 1),
                            self.hostname.encode()) +
                struct.pack('<%dp' % min(MAX_STRING_LEN, lusername + 1),
                            self.username.encode()) +
                struct.pack('<%dp' % min(MAX_STRING_LEN, lpassword + 1),
                            self.password.encode()) +
                struct.pack('<H', self.port) +
                struct.pack('<l', 0) +
                struct.pack('<l', master_id))


class BinLogStreamReader(object):
    """Connect to replication stream and read event
    """
    report_slave = None

    def __init__(self, connection_settings, server_id,
                 ctl_connection_settings=None, resume_stream=False,
                 blocking=False, only_events=None, log_file=None,
                 log_pos=None, end_log_pos=None,
                 filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, ignored_tables=None,
                 only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None,
                 report_slave=None, slave_uuid=None,
                 pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False,
                 slave_heartbeat=None,
                 is_mariadb=False,
                 ignore_decode_errors=False,
                 table_schema_cache: dict = None):
        """
        Attributes:
            ctl_connection_settings: Connection settings for cluster holding
                                     schema information
            resume_stream: Start for event from position or the latest event of
                           binlog or from older available event
            blocking: When master has finished reading/sending binlog it will
                      send EOF instead of blocking connection.
            only_events: Array of allowed events
            ignored_events: Array of ignored events
            log_file: Set replication start log file
            log_pos: Set replication start log pos (resume_stream should be
                     true)
            end_log_pos: Set replication end log pos
            auto_position: Use master_auto_position gtid to set position
            only_tables: An array with the tables you want to watch (only works
                         in binlog_format ROW)
            ignored_tables: An array with the tables you want to skip
            only_schemas: An array with the schemas you want to watch
            ignored_schemas: An array with the schemas you want to skip
            freeze_schema: If true do not support ALTER TABLE. It's faster.
            skip_to_timestamp: Ignore all events until reaching specified
                               timestamp.
            report_slave: Report slave in SHOW SLAVE HOSTS.
            slave_uuid: Report slave_uuid in SHOW SLAVE HOSTS.
            fail_on_table_metadata_unavailable: Should raise exception if we
                                                can't get table information on
                                                row_events
            slave_heartbeat: (seconds) Should master actively send heartbeat on
                             connection. This also reduces traffic in GTID
                             replication on replication resumption (in case
                             many event to skip in binlog). See
                             MASTER_HEARTBEAT_PERIOD in mysql documentation
                             for semantics
            is_mariadb: Flag to indicate it's a MariaDB server, used with auto_position
                    to point to Mariadb specific GTID.
            ignore_decode_errors: If true, any decode errors encountered 
                                  when reading column data will be ignored.
            table_schema_cache: Latest schemas of the synced tables (from previous execution). Indexed by table name.
        """

        self.__connection_settings = connection_settings
        self.__connection_settings.setdefault("charset", "utf8")

        self.__connected_stream = False
        self.__connected_ctl = False
        self.__resume_stream = resume_stream
        self.__blocking = blocking
        self._ctl_connection_settings = ctl_connection_settings
        if ctl_connection_settings:
            self._ctl_connection_settings.setdefault("charset", "utf8")

        self.__only_tables = only_tables
        self.__ignored_tables = ignored_tables
        self.__only_schemas = only_schemas
        self.__ignored_schemas = ignored_schemas
        self.__freeze_schema = freeze_schema
        self.__allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events)
        self.__fail_on_table_metadata_unavailable = fail_on_table_metadata_unavailable
        self.__ignore_decode_errors = ignore_decode_errors

        # We can't filter on packet level TABLE_MAP and rotate event because
        # we need them for handling other operations
        self.__allowed_events_in_packet = frozenset(
            [TableMapEvent, RotateEvent, QueryEventWithSchemaChanges]).union(self.__allowed_events)

        self.__server_id = server_id
        self.__use_checksum = False

        # Store table meta information
        self.table_map = {}
        self.log_pos = log_pos
        self.end_log_pos = end_log_pos
        self.log_file = log_file
        self.auto_position = auto_position
        self.skip_to_timestamp = skip_to_timestamp
        self.is_mariadb = is_mariadb

        if end_log_pos:
            self.is_past_end_log_pos = False

        if report_slave:
            self.report_slave = ReportSlave(report_slave)
        self.slave_uuid = slave_uuid
        self.slave_heartbeat = slave_heartbeat

        if pymysql_wrapper:
            self.pymysql_wrapper = pymysql_wrapper
        else:
            self.pymysql_wrapper = pymysql.connect
        self.mysql_version = (0, 0, 0)

        # Store table meta information cached from the last time
        self.schema_cache = TableColumnSchemaCache(table_schema_cache)

        self.alter_parser = AlterStatementParser()

        # init just in case of redeploying new version
        # self._init_column_schema_cache(only_schemas[0], only_tables)

    def close(self):
        if self.__connected_stream:
            self._stream_connection.close()
            self.__connected_stream = False
        if self.__connected_ctl:
            # break reference cycle between stream reader and underlying
            # mysql connection object
            self._ctl_connection._get_table_information = None
            self._ctl_connection.close()
            self.__connected_ctl = False

    def __connect_to_ctl(self):
        if not self._ctl_connection_settings:
            self._ctl_connection_settings = dict(self.__connection_settings)
        self._ctl_connection_settings["db"] = "information_schema"
        self._ctl_connection_settings["cursorclass"] = DictCursor
        self._ctl_connection = self.pymysql_wrapper(**self._ctl_connection_settings)
        self._ctl_connection._get_table_information = self.__get_table_information
        self.__connected_ctl = True

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        cur = self._stream_connection.cursor()
        cur.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
        result = cur.fetchone()
        cur.close()

        if result is None:
            return False
        var, value = result[:2]
        if value == 'NONE':
            return False
        return True

    def _register_slave(self):
        if not self.report_slave:
            return

        packet = self.report_slave.encoded(self.__server_id)

        if pymysql.__version__ < LooseVersion("0.6"):
            self._stream_connection.wfile.write(packet)
            self._stream_connection.wfile.flush()
            self._stream_connection.read_packet()
        else:
            self._stream_connection._write_bytes(packet)
            self._stream_connection._next_seq_id = 1
            self._stream_connection._read_packet()

    def __connect_to_stream(self):
        # log_pos (4) -- position in the binlog-file to start the stream with
        # flags (2) BINLOG_DUMP_NON_BLOCK (0 or 1)
        # server_id (4) -- server id of this slave
        # log_file (string.EOF) -- filename of the binlog on the master
        self._stream_connection = self.pymysql_wrapper(**self.__connection_settings)

        self.__use_checksum = self.__checksum_enabled()

        # If checksum is enabled we need to inform the server about the that
        # we support it
        if self.__use_checksum:
            cur = self._stream_connection.cursor()
            cur.execute("set @master_binlog_checksum= @@global.binlog_checksum")
            cur.close()

        if self.slave_uuid:
            cur = self._stream_connection.cursor()
            cur.execute("set @slave_uuid= '%s'" % self.slave_uuid)
            cur.close()

        if self.slave_heartbeat:
            # 4294967 is documented as the max value for heartbeats
            net_timeout = float(self.__connection_settings.get('read_timeout',
                                                               4294967))
            # If heartbeat is too low, the connection will disconnect before,
            # this is also the behavior in mysql
            heartbeat = float(min(net_timeout / 2., self.slave_heartbeat))
            if heartbeat > 4294967:
                heartbeat = 4294967

            # master_heartbeat_period is nanoseconds
            heartbeat = int(heartbeat * 1000000000)
            cur = self._stream_connection.cursor()
            cur.execute("set @master_heartbeat_period= %d" % heartbeat)
            cur.close()

        # When replicating from Mariadb 10.6.12 using binlog coordinates, a slave capability < 4 triggers a bug in
        # Mariadb, when it tries to replace GTID events with dummy ones. Given that this library understands GTID
        # events, setting the capability to 4 circumvents this error.
        # If the DB is mysql, this won't have any effect so no need to run this in a condition
        cur = self._stream_connection.cursor()
        cur.execute("SET @mariadb_slave_capability=4")
        cur.close()

        self._register_slave()

        if not self.auto_position:
            # only when log_file and log_pos both provided, the position info is
            # valid, if not, get the current position from master
            if self.log_file is None or self.log_pos is None:
                cur = self._stream_connection.cursor()
                cur.execute("SHOW MASTER STATUS")
                master_status = cur.fetchone()
                if master_status is None:
                    raise BinLogNotEnabled()
                self.log_file, self.log_pos = master_status[:2]
                cur.close()

            prelude = struct.pack('<i', len(self.log_file) + 11) \
                      + bytes(bytearray([COM_BINLOG_DUMP]))

            if self.__resume_stream:
                prelude += struct.pack('<I', self.log_pos)
            else:
                prelude += struct.pack('<I', 4)

            flags = 0
            if not self.__blocking:
                flags |= 0x01  # BINLOG_DUMP_NON_BLOCK
            prelude += struct.pack('<H', flags)

            prelude += struct.pack('<I', self.__server_id)
            prelude += self.log_file.encode()
        else:
            if self.is_mariadb:
                # https://mariadb.com/kb/en/5-slave-registration/
                cur = self._stream_connection.cursor()
                cur.execute("SET @slave_connect_state='%s'" % self.auto_position)
                cur.execute("SET @slave_gtid_strict_mode=1")
                cur.execute("SET @slave_gtid_ignore_duplicates=0")
                cur.close()

                # https://mariadb.com/kb/en/com_binlog_dump/
                header_size = (
                        4 +  # binlog pos
                        2 +  # binlog flags
                        4 +  # slave server_id,
                        4  # requested binlog file name , set it to empty
                )

                prelude = struct.pack('<i', header_size) + bytes(bytearray([COM_BINLOG_DUMP]))

                # binlog pos
                prelude += struct.pack('<i', 4)

                flags = 0
                if not self.__blocking:
                    flags |= 0x01  # BINLOG_DUMP_NON_BLOCK

                # binlog flags
                prelude += struct.pack('<H', flags)

                # server id (4 bytes)
                prelude += struct.pack('<I', self.__server_id)

                # empty_binlog_name (4 bytes)
                prelude += b'\0\0\0\0'

            else:
                # Format for mysql packet master_auto_position
                #
                # All fields are little endian
                # All fields are unsigned

                # Packet length   uint   4bytes
                # Packet type     byte   1byte   == 0x1e
                # Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
                # Server id       uint   4bytes
                # binlognamesize  uint   4bytes
                # binlogname      str    Nbytes  N = binlognamesize
                #                                Zeroified
                # binlog position uint   4bytes  == 4
                # payload_size    uint   4bytes

                # What come next, is the payload, where the slave gtid_executed
                # is sent to the master
                # n_sid           ulong  8bytes  == which size is the gtid_set
                # | sid           uuid   16bytes UUID as a binary
                # | n_intervals   ulong  8bytes  == how many intervals are sent
                # |                                 for this gtid
                # | | start       ulong  8bytes  Start position of this interval
                # | | stop        ulong  8bytes  Stop position of this interval

                # A gtid set looks like:
                #   19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10,
                #   1c2aad49-ae92-409a-b4df-d05a03e4702e:42-47:80-100:130-140
                #
                # In this particular gtid set,
                # 19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10
                # is the first member of the set, it is called a gtid.
                # In this gtid, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457 is the sid
                # and have two intervals, 1-3 and 8-10, 1 is the start position of
                # the first interval 3 is the stop position of the first interval.

                gtid_set = GtidSet(self.auto_position)
                encoded_data_size = gtid_set.encoded_length

                header_size = (2 +  # binlog_flags
                               4 +  # server_id
                               4 +  # binlog_name_info_size
                               4 +  # empty binlog name
                               8 +  # binlog_pos_info_size
                               4)  # encoded_data_size

                prelude = b'' + struct.pack('<i', header_size + encoded_data_size) \
                          + bytes(bytearray([COM_BINLOG_DUMP_GTID]))

                flags = 0
                if not self.__blocking:
                    flags |= 0x01  # BINLOG_DUMP_NON_BLOCK
                flags |= 0x04  # BINLOG_THROUGH_GTID

                # binlog_flags (2 bytes)
                # see:
                #  https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
                prelude += struct.pack('<H', flags)

                # server_id (4 bytes)
                prelude += struct.pack('<I', self.__server_id)
                # binlog_name_info_size (4 bytes)
                prelude += struct.pack('<I', 3)
                # empty_binlog_namapprovale (4 bytes)
                prelude += b'\0\0\0'
                # binlog_pos_info (8 bytes)
                prelude += struct.pack('<Q', 4)

                # encoded_data_size (4 bytes)
                prelude += struct.pack('<I', gtid_set.encoded_length)
                # encoded_data
                prelude += gtid_set.encoded()

        if pymysql.__version__ < LooseVersion("0.6"):
            self._stream_connection.wfile.write(prelude)
            self._stream_connection.wfile.flush()
        else:
            self._stream_connection._write_bytes(prelude)
            self._stream_connection._next_seq_id = 1
        self.__connected_stream = True

    def fetchone(self):
        while True:
            if self.end_log_pos and self.is_past_end_log_pos:
                return None

            if not self.__connected_stream:
                self.__connect_to_stream()

            if not self.__connected_ctl:
                self.__connect_to_ctl()

            try:
                if pymysql.__version__ < LooseVersion("0.6"):
                    pkt = self._stream_connection.read_packet()
                else:
                    pkt = self._stream_connection._read_packet()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self._stream_connection.close()
                    self.__connected_stream = False
                    continue
                raise

            if pkt.is_eof_packet():
                self.close()
                return None

            if not pkt.is_ok_packet():
                continue

            binlog_event = BinLogPacketWrapper(pkt, self.table_map,
                                               self._ctl_connection,
                                               self.mysql_version,
                                               self.__use_checksum,
                                               self.__allowed_events_in_packet,
                                               self.__only_tables,
                                               self.__ignored_tables,
                                               self.__only_schemas,
                                               self.__ignored_schemas,
                                               self.__freeze_schema,
                                               self.__fail_on_table_metadata_unavailable,
                                               self.__ignore_decode_errors)

            if binlog_event.event_type == ROTATE_EVENT:
                self.log_pos = binlog_event.event.position
                self.log_file = binlog_event.event.next_binlog
                # Table Id in binlog are NOT persistent in MySQL - they are in-memory identifiers
                # that means that when MySQL master restarts, it will reuse same table id for different tables
                # which will cause errors for us since our in-memory map will try to decode row data with
                # wrong table schema.
                # The fix is to rely on the fact that MySQL will also rotate to a new binlog file every time it
                # restarts. That means every rotation we see *could* be a sign of restart and so potentially
                # invalidates all our cached table id to schema mappings. This means we have to load them all
                # again for each logfile which is potentially wasted effort but we can't really do much better
                # without being broken in restart case
                self.table_map = {}
            elif binlog_event.log_pos:
                self.log_pos = binlog_event.log_pos

            if self.end_log_pos and self.log_pos >= self.end_log_pos:
                # We're currently at, or past, the specified end log position.
                self.is_past_end_log_pos = True

            # This check must not occur before clearing the ``table_map`` as a
            # result of a RotateEvent.
            #
            # The first RotateEvent in a binlog file has a timestamp of
            # zero.  If the server has moved to a new log and not written a
            # timestamped RotateEvent at the end of the previous log, the
            # RotateEvent at the beginning of the new log will be ignored
            # if the caller provided a positive ``skip_to_timestamp``
            # value.  This will result in the ``table_map`` becoming
            # corrupt.
            #
            # https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
            # From the MySQL Internals Manual:
            #
            #   ROTATE_EVENT is generated locally and written to the binary
            #   log on the master. It is written to the relay log on the
            #   slave when FLUSH LOGS occurs, and when receiving a
            #   ROTATE_EVENT from the master. In the latter case, there
            #   will be two rotate events in total originating on different
            #   servers.
            #
            #   There are conditions under which the terminating
            #   log-rotation event does not occur. For example, the server
            #   might crash.
            if self.skip_to_timestamp and binlog_event.timestamp < self.skip_to_timestamp:
                continue

            if binlog_event.event_type == TABLE_MAP_EVENT and \
                    binlog_event.event is not None:
                table_obj = binlog_event.event.get_table()
                self.table_map[binlog_event.event.table_id] = table_obj

                # store current schema
                self._update_current_schema(table_obj.schema, table_obj.table)

                # store internal table ids for convenience
                self.schema_cache.update_table_ids_cache(table_obj.schema, table_obj.table,
                                                         table_obj.table_id)

            # Process ALTER events and update schema cache so the mapping works properly
            if binlog_event.event_type == QUERY_EVENT and 'ALTER' in binlog_event.event.query.upper():
                table_changes = self._update_cache_and_map(binlog_event.event)
                binlog_event.event.schema_changes = table_changes

            # event is none if we have filter it on packet level
            # we filter also not allowed events
            if binlog_event.event is None or (binlog_event.event.__class__ not in self.__allowed_events):
                continue

            return binlog_event.event

    def _allowed_event_list(self, only_events, ignored_events,
                            filter_non_implemented_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = set((
                QueryEvent,
                RotateEvent,
                StopEvent,
                FormatDescriptionEvent,
                XAPrepareEvent,
                XidEvent,
                GtidEvent,
                BeginLoadQueryEvent,
                ExecuteLoadQueryEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                DeleteRowsEvent,
                TableMapEvent,
                HeartbeatLogEvent,
                NotImplementedEvent,
                MariadbGtidEvent
            ))
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    def __get_table_information(self, schema, table):
        if self.schema_cache.get_column_schema(schema, table):
            return self._get_column_schema_from_cache(schema, table)
        else:
            # hacky way to call the parent secret method
            current_column_schema = self._get_table_information_from_db(schema, table)
            # update cache with current schema
            self.schema_cache.set_column_schema(schema, table, current_column_schema)

            # TODO: consider moving this to the binlog init so it's in sync and done only once
            self.schema_cache.update_current_schema_cache(schema, table, current_column_schema)
            return current_column_schema

    def _get_table_information_from_db(self, schema: str, table: str):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                cur.execute("""
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION,
                        DATA_TYPE, CHARACTER_OCTET_LENGTH
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = %s AND table_name = %s
                    ORDER BY ORDINAL_POSITION
                    """, (schema, table))

                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error

    def _update_current_schema(self, schema, table):
        """
        Keeps current schema (at the point of execution) for later reference
        """
        if not self.schema_cache.is_current_schema_cached(schema, table):
            column_schema = self._get_table_information_from_db(schema, table)
            self.schema_cache.update_current_schema_cache(schema, table, column_schema)

    def _update_cache_and_map(self, binlog_event: QueryEvent):
        """
        Updates schema cache based on given ALTER events. Refreshes the table_map
        """
        table_changes = self.alter_parser.get_table_changes(binlog_event.query, binlog_event.schema.decode())
        monitored_changes = []
        for table_change in table_changes:
            # normalize table_name. We expect table names in lower case here.
            table_change.table_name = table_change.table_name.lower()
            table_change.schema = table_change.schema.lower()
            # only monitored tables
            logging.debug(
                f'Table change detected: {table_change}, monitored tables: {self._BinLogStreamReader__only_tables}, '
                f'monitored schemas: {self._BinLogStreamReader__only_schemas}')
            if table_change.table_name not in self._BinLogStreamReader__only_tables:
                continue
            # only monitored schemas
            if table_change.schema not in self._BinLogStreamReader__only_schemas:
                continue
            monitored_changes.append(table_change)
            self.schema_cache.update_cache(table_change)

            # invalidate table_map cache so it is rebuilt from schema cache next TABLE_MAP_EVENT
            self._invalidate_table_map(table_change.schema, table_change.table_name)
        if not monitored_changes:
            logging.debug(f"ALTER statements detected, but no table change recognised. {binlog_event.query}")
        return monitored_changes

    def _invalidate_table_map(self, schema: str, table_name: str):
        indexes = self.schema_cache.get_table_ids(schema, table_name)
        for i in indexes:
            self.table_map.pop(i, None)

    def _init_column_schema_cache(self, schema: str, tables: List[str]):
        """
        Helper method to initi schema cache in case the extraction started with empty state
        Returns:

        """
        for table in tables:
            if self.schema_cache.get_column_schema(schema, table):
                continue
            logging.warning(f"Schema for table {schema}-{table} is not initialized, using current schema")
            current_column_schema = self._get_table_information_from_db(schema, table)
            # update cache with current schema
            self.schema_cache.set_column_schema(schema, table, current_column_schema)

            self.schema_cache.update_current_schema_cache(schema, table, current_column_schema)

    def _BinLogStreamReader__get_table_information(self, schema, table):
        """ Uglily overridden BinLogStreamReader private method via the "name mangling" black magic.
            Get table information from Cache or fetch the current state.

            This is being used in the TableMapEvent to get the schema if not present in the cache.
            It allows to retrieve schema from the cache if present.

        """
        if self.schema_cache.get_column_schema(schema, table):
            return self._get_column_schema_from_cache(schema, table)
        else:
            # hacky way to call the parent secret method
            current_column_schema = self._get_table_information_from_db(schema, table)
            # update cache with current schema
            self.schema_cache.set_column_schema(schema, table, current_column_schema)

            # TODO: consider moving this to the binlog init so it's in sync and done only once
            self.schema_cache.update_current_schema_cache(schema, table, current_column_schema)
            return current_column_schema

    def _table_info_backoff(func):
        @functools.wraps(func)
        @backoff.on_exception(backoff.expo,
                              pymysql.OperationalError,
                              max_tries=3)
        def wrapper(self, *args, **kwargs):
            if not self._BinLogStreamReader__connected_ctl:
                logging.warning('Mysql unreachable, trying to reconnect')
                self._BinLogStreamReader__connect_to_ctl()
            return func(self, *args, **kwargs)

        return wrapper

    @_table_info_backoff
    def _get_table_information_from_db(self, schema, table):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                query = cur.mogrify("""SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION, DATA_TYPE,
                        defaults.DEFAULT_COLLATION_NAME,
                        defaults.DEFAULT_CHARSET
                    FROM
                        information_schema.columns col
                    JOIN (SELECT
                              default_character_set_name AS DEFAULT_CHARSET
                            , DEFAULT_COLLATION_NAME     AS DEFAULT_COLLATION_NAME
                        FROM information_schema.SCHEMATA
                        WHERE
                            SCHEMA_NAME = %s) as defaults ON 1=1
                    WHERE
                        table_schema = %s AND table_name = %s
                    ORDER BY ORDINAL_POSITION;
                    """, (schema, schema, table))
                logging.debug(query)
                cur.execute(query)
                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self._BinLogStreamReader__connected_ctl = False
                    raise pymysql.OperationalError("Getting the initial schema failed, server unreachable!") from error
                else:
                    raise error

    def _get_column_schema_from_cache(self, schema: str, table: str):
        column_schema = self.schema_cache.get_column_schema(schema, table)
        # convert to upper case
        for c in column_schema:
            c['COLUMN_NAME'] = c['COLUMN_NAME'].upper()
            # backward compatibility after update to mysql-replication==0.43.0
            if not c.get('DATA_TYPE'):
                c['DATA_TYPE'] = c['COLUMN_TYPE']

        return column_schema

    def __iter__(self):
        return iter(self.fetchone, None)
