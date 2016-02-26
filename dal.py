#!/usr/bin/python

import sqlite3
from util import synchronized


class Dal:
    """
    Class to handle database abstraction for app

    Methods:
            __init__(db_file)
              Initialize Database Connection.  db_file is the sqlite db file to use.
              if the file does not exist, it will be created in the current working directory.
            insert(table, (val,val,val) | {col:val, col:val...} | record={col:val, col:val})
              inserts a record into the database.  May be called one of three ways:
                insert ('table', (val,val,val)):
                        insert values into the table.  Must specify exact number of values IN THE ORDER THEY
                                ARE listed in the database.  second argument may be a list or tuple.
                insert ('table', record={col:val, col:val, col:val ...})
                        keword argument record should be a dictionary in the form 'column':value
                insert ('table', {col:val, col:val...})
                        same as above, but instead of a keyword argument, an unnamed dict may be used in the same format
              returns number of rows effected.
            get(table, id, id_field='col', fields=['col', 'col', ...])

            search(table, criteria=[(col, operator, value), (col,operator, value), ...], fields=['col', 'col', ...])
              executes SQL select statement against table sepecified.  Returns rows as list of dictionary elements.
            update(table, {col:val, col:val, ...}, criteria=[(col, operator, value), (col,operator, value), ...])
              updates rows
              returns rows effected by query
	              This helps prevent executing a dangerous SQL update that
	                otherwise may change every record in the table.  To update all records pass criteria something like:
  	              update('table', criteria=[('id', '>', 0)])
            delete(table, criteria=[(col, operator, value), (col,operator, value), ...])
                deletes rows. 
              Returns rows effected by query.
              Throws error if criteria not specified.
            create_table(table, fields)
                    creates table specified
            drop_table(table)
                    drops table specified

    All returned records will be returned in dictionary form.
    """

    _conn = None
    _db_schema = None

    def __init__(self, db_file='sqlite.db'):
        """
        Constructor.

        Optional db_file opens a connection to the specified file.

        Keyword Arguments:
                db_file {string} -- SQLite3 Database filename (default: 'sqlite.db')
        """
        self._conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES)

        # populate schema info
        self._get_db_schema()

    def _get_db_schema(self):
        """_get_db_schema()

        For internal use only.

        Fetches all tables from the sqlite_master table.  then executes pragma command
        to discover table columns.  Updates _db_schema as a dict keyed by table name and
        containing a list of column names.

        The resulting schema data will be used to sanity check field names and table names
        in database DAL calls.
        """
        cur = self._conn.cursor()
        cur.row_factory = self._dict_factory
        rows = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()

        if rows:
            tables = []
            for row in rows:
                tables.append(row['name'])

            self._db_schema = {}
            cur.row_factory = None
            for table in tables:
                cols = []
                for row in cur.execute("pragma table_info('%s')" % table).fetchall():
                    cols.append(row[1])
                self._db_schema.update({table: cols})

    def _dict_factory(self, cursor, row):
        """_dict_factory(cursor, row)

        For interal use only.

        factory method that will return a dictionary for each row returned from the sqlite db cursor.
        Dictionary keys will be string and correspond to the column name.

        Arguments:
                cursor {[type]} -- [cursor]
                row {[type]} -- [row]

        Returns:
                dict() -- {'column':value}
        """
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def insert(self, table, *args, **kwargs):
        """
        Insert a record to the database.  Return the record inserted to the caller if successful.

        Arguments:
                table name (required as first argument.  sqlite table name)

                *args
                        tuple or list of values
                         *or*
                        dictionary of column names and values 

                **kwargs
                        To insert a record as a dictionary:
                                insert_record('table', record=record_dict)

                This method will insert only the columns specified in the record dictionary

        Returns:
                Returns the inserted record.
        """

        if table not in self._db_schema.keys():
            raise ValueError(
                'Table name specified %s does not exist in DB.' % table)

        @synchronized
        def _insert(table, values, cols=None):
            """
            nested insert function

            inserts a row into the database.  Only callable inside of insert_record.
            Can be called from several places, depending on how insert_record is called.

            Arguments:
                    table {[type]} -- [description]
                    cols {[type]} -- [description]
                    values {[type]} -- [description]

            Returns:
                    [type] -- [description]

            Raises:
                    ValueError -- [description]
            """
            if cols:
                if len(cols) != len(values):
                    raise ValueError("%s columns specified. %s values specified. Length must match" % (
                        len(cols), len(values)))

                col_list = ""
                qs = ""
                for col in cols:
                    col_list += "%s," % col
                    qs += "?,"
                col_list = col_list.strip(',')
                qs = qs.strip(',')
                sql = "INSERT into %s (%s) VALUES (%s)" % (table, col_list, qs)
            else:
                qs = ""
                for val in values:
                    qs += "?,"
                qs = qs.strip(',')
                sql = "INSERT INTO %s VALUES (%s)" % (table, qs)

            cur = self._conn.cursor()
            cur.row_factory = self._dict_factory

            cur.execute(sql, values)
            self._conn.commit()
            return cur.rowcount

        if 'record' in kwargs.keys():
            # a record has been passed with a keyword argument
            errors = []
            columns = []
            values = []
            for column in kwargs['record'].keys():
                if column not in self._db_schema[table]:
                    errors.append("Column %s not in table %s" %
                                  (column, table))
                else:
                    columns.append(column)
                    values.append(kwargs['record'][column])
            if errors:
                raise ValueError(str(errors))

            return _insert(table, values, columns)

        elif args and len(args) == 1 and (isinstance(args[0], list) or isinstance(args[0], tuple)):
            # a list of values only
            return _insert(table, list(args[0]))

        elif args and len(args) == 1 and isinstance(args[0], dict):
            # a single dictionary of records supplied without keyword argument
            record = args[0]
            errors = []
            columns = []
            values = []
            for column in record.keys():
                if column not in self._db_schema[table]:
                    errors.append("Column %s not in table %s" %
                                  (column, table))
                else:
                    columns.append(column)
                    values.append(record[column])
            if errors:
                raise ValueError(str(errors))

            return _insert(table, values, columns)

    def get(self, table, *args, **kwargs):
        """get_record_by_id(table, ...)

        Returns record by specified ID

        first arg after table should be the ID value.

        Optionally specify an array of field names like:
                get_record_by_id('test', 203, fields=['name', 'email'])
                        will return the 'name' and 'email' columns from the test table
                        where ID = 203

        Arguments:
                table {[string]} -- [description]
                *args {[ID]} -- [add ID value]
                **kwargs {[type]} -- [fields - optional]

        Returns:
                [type] -- [description]

        Raises:
                ValueError -- [description]
        """
        if table not in self._db_schema.keys():
            raise ValueError(
                'Table name specified %s does not exist in DB.' % table)
        cur = self._conn.cursor()

        args_ = list(args)
        if args_ and 'as-dict' in args_:
            args_.pop(args.index('as-dict'))
            cur.row_factory = self._dict_factory
        else:
            cur.row_factory = sqlite3.Row

        if not kwargs and len(args_) == 1:
            # should mean the id field is 'id' (default)
            # and single argument after table is the ID

            sql = "SELECT * from %s WHERE id=?" % table
            return cur.execute(sql, str(args_[0])).fetchall()
        if kwargs:
            kwargs_ = dict(kwargs)
            if 'fields' in kwargs_:
                f_list = ""
                for field in kwargs_.pop('fields'):
                    f_list += "%s ," % field
                f_list = f_list.strip(',')

                sql = "SELECT %s from %s " % (f_list, table)
            return cur.execute(sql).fetchall()

    def search(self, table, *args, **kwargs):
        """search(table, fields=(field,field,field, ...), criteria=[(col,op,val), (), ...])

        Execute select against database table, if specified, list or tuple of columns, will restrict return 
                results to those columns only.
        criteria is in the form of a list of 3-tuples (column, operator, value) 

        Arguments:
                table {[string]} -- [description]
                *args {[type]} -- [description]
                **kwargs {[type]} -- [description]

        Returns:
                [list] -- [list of dictionary represented rows]

        Raises:
                ValueError -- [table name missing or not in database]
        """

        # TODO: add logic to support select from multiple tables and
        # inner-joins

        if table not in self._db_schema.keys():
            raise ValueError(
                'Table name specified %s does not exist in DB.' % table)
        args_ = list(args)
        if args_ and 'as-dict' in args_:
            args_.pop(args_.index('as-dict'))
            self._conn.row_factory = self._dict_factory
        else:
            self._conn.row_factory = sqlite3.Row

        if kwargs:
            kwargs_ = dict(kwargs)
            if 'fields' in kwargs_:
                f_list = ""
                for field in kwargs_.pop('fields'):
                    f_list += "%s ," % field
                f_list = f_list.strip(',')
                sql = "SELECT %s from %s " % (f_list, table)
            else:
                sql = "SELECT * from %s " % table

            if 'criteria' in kwargs_:
                num_criteria = len(kwargs_['criteria'])
                x = 0
                sql += "WHERE "
                criterium = []
                for (field, op, criteria) in kwargs_['criteria']:
                    sql += "%s %s ? " % (str(field), str(op))
                    criterium.append(str(criteria))
                    x += 1
                    if x < num_criteria:
                        sql += "AND "

                return self._conn.execute(sql, criterium).fetchall()
            else:
                return self._conn.execute(sql).fetchall()
        else:
            sql = "SELECT * from %s" %table
            return self._conn.execute(sql).fetchall()

    @synchronized
    def update(self, table, *args, **kwargs):
        """update(table, args, kwargs)

        Update rows in table.

        first argument after table must be a dictionary of column:value pairs.

        named argument criteria must be an array of tuples in the form: (column, operator, value)
        Multiple criteria are stitched together with AND.  All criteria must be true in order for 
        operation to succeed

        Example:
                update('test', {'firstname':'Frank'}, criteria=[('id', '=', 1)])
        Decorators:
                synchronized

        Arguments:
                table {[type]} -- [description]
                *args {[type]} -- [description]
                **kwargs {[type]} -- [description]

        Returns:
                [int] -- [number of rows affected]

        Raises:
                ValueError -- [if table name is not specified]
                ValueError -- [if criteria is not passed] - potentially dangerous update that would change all rows
        """
        if table not in self._db_schema.keys():
            raise ValueError(
                'Table name specified %s does not exist in DB.' % table)

        sql = "UPDATE %s SET" % table

        if args and len(args) == 1 and isinstance(args[0], dict):
            # fields to update dict is only non keyword arg
            val_array = []
            for (col, val) in args[0].items():
                sql += " %s = ?," % col
                val_array.append(val)
            sql = sql.strip(',')

            if 'criteria' in kwargs.keys():
                # criteria passed as keword argument
                num_criteria = len(kwargs['criteria'])
                x = 0
                sql += " WHERE "
                criterium = []
                for (field, op, criteria) in kwargs['criteria']:
                    sql += "%s %s ? " % (str(field), str(op))
                    criterium.append(str(criteria))
                    x += 1
                    if x < num_criteria:
                        sql += "AND "

                r = self._conn.execute(sql, val_array + criterium)
                self._conn.commit()
                return r.rowcount
            else:
                raise ValueError(
                    "criteria not specified.  Dangerous update aborted.")

    def delete(self, table, *args, **kwargs):
        """delete ('table', criteria=[(col, op, val), (), ...])

        Delete row(s) from database.  criteria should be in the form of a 3-tuple (column, operator, value)

        Arguments:
                table {[string]} -- [table name]
                *args {[type]} -- [description]
                **kwargs {[type]} -- [description]

        Returns:
                [int] -- [Number of Rows effected]

        Raises:
                ValueError -- [table not in database]
                ValueError -- [criteria missing]
        """
        if table not in self._db_schema.keys():
            raise ValueError(
                'Table name specified %s does not exist in DB.' % table)
        if not 'criteria' in kwargs:
            raise ValueError('ERROR: criteria missing.')

        sql = "DELETE from %s WHERE " % table

        num_criteria = len(kwargs['criteria'])
        x = 0
        criterium = []
        for (field, op, criteria) in kwargs['criteria']:
            sql += "%s %s ? " % (str(field), str(op))
            criterium.append(str(criteria))
            x += 1
            if x < num_criteria:
                sql += "AND "

        r = self._conn.execute(sql, criterium)
        self._conn.commit()
        return r.rowcount

    @synchronized
    def create_table(self, table, fields):
        """create_table(table, fields)

        Fields should be a dictionary:
                key: column name
                value: column type and options

        Arguments:
                table {[string]} -- [table name]
                fields {[list of dictionaries]} -- [array of dictionaries:]
                        column_name
                        type
                        options (optional)

        Raises:
                ValueError -- [description]

        NOTE: This method should never be called from any web-facing code.  It is potentially
        unsafe and care should be taken to avoid SQL injection.
        """
        if table in self._db_schema.keys():
            raise ValueError(
                'Table name specified %s is already in DB.' % table)

        sql = 'CREATE TABLE %s (' % table
        num_fields = len(fields)
        i = 0
        for field in fields:
            sql += "%s %s" % (field['column_name'], field['type'])
            if 'options' in field:
                sql += " %s" % field['options']
            i += 1
            if i < num_fields:
                sql += ','
        sql += ')'

        self._conn.execute(sql)
        self._conn.commit()
        self._get_db_schema()

    @synchronized
    def drop_table(self, table):
        """
        drop_table(table)

        drops table specified

        Arguments:
                table {[string]} -- [table name to drop]

        NOTE:  This method should never be called from any web-facing code.  It is potentially
        unsafe and care should be taken to avoid SQL injection.
        """

        self._conn.execute("DROP TABLE %s" % table)
        self._conn.commit()
        self._get_db_schema()
