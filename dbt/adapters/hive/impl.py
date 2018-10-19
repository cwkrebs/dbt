from contextlib import contextmanager

import pyhive

import dbt
from dbt.adapters.hive.relation import HiveRelation

from dbt.logger import GLOBAL_LOGGER as logger
from pyhive import hive, exc


class HiveAdapter(dbt.adapters.default.DefaultAdapter):

    RELATION_TYPES = {
        'TABLE': HiveRelation.Table,
        'VIEW': HiveRelation.View,
    }

    Relation = HiveRelation

    @classmethod
    def type(cls):
        return 'hive'

    @classmethod
    def coalesce(cls, d, key, default):
        if key in d:
            return d[key]
        else:
            return default

    @classmethod
    def open_connection(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials
        try:
            # TODO clarify & simply & FIXME
            handle = hive.connect(
                host=cls.coalesce(credentials, 'host', None),
                port=cls.coalesce(credentials, 'port', None),
                database=cls.coalesce(credentials, 'schema', 'default'),
                username=cls.coalesce(credentials, 'user', None),
                password=cls.coalesce(credentials, 'password', None),
                auth=cls.coalesce(credentials, 'auth', None),
                thrift_transport=cls.coalesce(credentials, 'thrift_transport', None),
                kerberos_service_name=cls.coalesce(credentials, 'kerberos_service_name', None),
                configuration=cls.coalesce(credentials, 'configuration', None))

            connection.handle = handle
            connection.state = 'open'
        except pyhive.exc.Error as e:
            logger.debug("Got an error when attempting to open a hive "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    @classmethod
    def close(cls, connection):
        connection.state = 'closed'
        connection.handle.close()

        return connection

    @classmethod
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(error)
        error_msg = "\n".join(
            [item['message'] for item in error.errors])

        raise dbt.exceptions.DatabaseException(error_msg)

    @contextmanager
    def exception_handler(self, sql, model_name=None,
                          connection_name=None):
        try:
            yield

        except hive.exc as e:
            message = "Hive exception :\n{sql}" # TODO FIXME
            self.handle_error(e, message, sql)

    def begin(self, name):
        pass

    def commit(self, connection):
        pass

    @classmethod
    def date_function(cls):
        return 'from_unixtime(unix_timestamp())'

    @classmethod
    def get_status(cls, cursor):
        return cursor.poll().operationState

    def alter_column_type(self, schema, table, column_name, new_column_type,
                          model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`alter_column_type` is not implemented for this adapter!')

    def get_existing_schemas(self, model_name=None):
        connection = self.get_connection(model_name)
        cursor = connection.handle.cursor()

        try:
            cursor.execute('show databases')
            return [db for db, *_ in cursor.fetchall()]
        finally:
            cursor.close()

    def check_schema_exists(self, schema, model_name=None):
        return any(schema == db for db in self.get_existing_schemas(model_name))

    def cancel_connection(self, connection):
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!')

    def _hive_table_to_relation(self, schema, hive_table, table_type):
        if hive_table is None:
            return None

        return self.Relation.create(
            schema=schema,
            identifier=hive_table,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=table_type)

    def _list_relations(self, schema, model_name=None):
        connection = self.get_connection(model_name)
        cursor = connection.handle.cursor()

        relations=[]
        try:
            cursor.execute('use ' + schema)
            cursor.execute('show tables')

            for table, *_ in cursor.fetchall():
                cursor.execute('show create table ' + table)

                table_type=self.RELATION_TYPES.get('Table')
                if ' '.join([line for line, *_ in cursor.fetchall()]).lstrip().lower().startswith('create view'):
                    table_type=self.RELATION_TYPES.get('View')


                relations.append(self._hive_table_to_relation(schema, table, table_type))

            return relations
        finally:
            cursor.close()

    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                    .format(relation))

        sql = 'drop {} if exists {}'.format(relation.type, relation)

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
