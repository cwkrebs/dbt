from contextlib import contextmanager

import pyhive
from pyhive import hive, exc
from thrift.Thrift import TException

import dbt
from dbt.adapters.hive.relation import HiveRelation
from dbt.logger import GLOBAL_LOGGER as logger


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
                # configuration=cls.coalesce(credentials, 'configuration', None))
                # see https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties
                # TODO checkme
                configuration={'hive.execution.engine': 'spark', 'hive.txn.timeout': '28800'})

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
            [resp.status.errorMessage for resp in error.args])

        raise dbt.exceptions.DatabaseException(error_msg)

    @contextmanager
    def exception_handler(self, sql, model_name=None,
                          connection_name=None):
        try:
            yield

        except pyhive.exc.Error as e:
            message = "Hive exception :\n{sql}" # TODO FIXME
            self.handle_error(e, message, sql)

        except TException as e:
            message = "Thrift exception :\n{sql}" # TODO FIXME
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

    def rename_relation(self, from_relation, to_relation,
                        model_name=None):
        self.cache.rename(from_relation, to_relation)
        rel_type = 'table'
        if from_relation.type is HiveRelation.View:
            rel_type = 'view'
        sql = 'alter {} {} rename to {}'.format(
            rel_type, from_relation, to_relation)

        connection, cursor = self.add_query(sql, model_name)

    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                    .format(relation))

        sql = 'drop {} if exists {}'.format(relation.type, relation)

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)

    def get_catalog(self, manifest):
        connection = self.get_connection('catalog')
        cursor = connection.handle.cursor()

        column_names = (
            'table_schema',
            'table_name',
            'table_type',
            'table_comment',
            # does not exist in hive, but included for consistency
            'table_owner',
            'column_name',
            'column_index',
            'column_type',
            'column_comment',
        )

        columns = []
        try:
            cursor.execute('show databases')
            for db, *_ in cursor.fetchall():
                for relation in self._list_relations(db):

                    try:
                        cursor.execute('describe {}.{}'.format(db, relation.identifier))

                        col_index = 1
                        for col_name, data_type, comment, *_ in cursor.fetchall():

                            column_data = (
                                db,  # table_schema
                                relation.identifier,  # table_name
                                relation.type,  # table_type
                                None,  # table_comment
                                None,  # table_owner
                                col_name, # column_name
                                col_index,  # column_index
                                data_type,  # column_type
                                comment,  # column_comment
                            )
                            col_index += 1

                            column_dict = dict(zip(column_names, column_data))
                            columns.append(column_dict)
                    except exc.OperationalError as error:
                        logger.debug(error)
                        error_msg = "\n".join(
                            [resp.status.errorMessage for resp in error.args])

            return dbt.clients.agate_helper.table_from_data(columns, column_names)

        finally:
            cursor.close()
