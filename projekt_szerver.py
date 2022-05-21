import json
import os
import sys
import shutil
import socket
from datetime import datetime
from sys import stdin, stdout, stderr
from error import Error, ServerError
from itertools import chain
from pymongo import MongoClient
import pymongo


class Server:

    def __init__(self, server_dir):
        assert os.path.exists(server_dir)

        self.server_dir = server_dir
        self.database = None
        self.db = None  # holds current mongo database
        self.idb = None  # holds index cluster associated with current db
        self.mongo = MongoClient(os.getenv('MONGO_HOST'))

    #
    # UTILITY METHODS
    #

    def db_path(self, database):
        return os.path.join(self.server_dir, database)

    def tab_path(self, table):
        return os.path.join(self.database[1], f'{table}.json')

    def check_table(self, table):
        if self.database is None:
            raise ServerError(Error.NO_DATABASE_IN_USE)
        path = self.tab_path(table)
        if not os.path.exists(path):
            raise ServerError(Error.DOES_NOT_EXIST, f"table: {table}")

    def open_table(self, table, mode):
        self.check_table(table)
        path = self.tab_path(table)
        return open(path, mode)

    def read_table(self, table) -> list | Error:
        table_io = self.open_table(table, 'r')
        with table_io as f:
            table_def = f.read()
        try:
            data = json.loads(table_def)
            return data
        except json.decoder.JSONDecodeError:
            raise ServerError(Error.INVALID_JSON, f"path: {self.tab_path(table)}")

    def read_table_dict(self, table) -> dict | Error:
        tab_def = self.read_table(table)
        tab_dict = self.table_def_to_dict(tab_def)
        return tab_dict

    # NOTE: the order of iteration does not correspond to the order of insertion
    def table_def_to_dict(self, tab_def) -> dict:
        tab_dict = {}
        for col in tab_def:
            tab_dict[col['name']] = col
        return tab_dict

    def write_table(self, table, table_def) -> None | Error:
        table_io = self.open_table(table, 'w')
        with table_io:
            table_io.write(json.dumps(table_def))
            data = json.dumps(table_def)
            return data

    def is_reference_valid(self, reference):
        try:
            self.get_reference(reference)
            return True
        except ServerError:
            return False

    def get_reference(self, reference):
        split = reference.split('.')
        if len(split) != 2:
            raise ServerError(Error.INVALID_REFERENCE, reference)
        rtab, rcol = split
        tab_def = self.read_table(rtab)
        tab_dict = self.table_def_to_dict(tab_def)

        col_def = tab_dict.get(rcol)
        if col_def is None:
            raise ServerError(Error.INVALID_REFERENCE, f'ref: {reference}')
        else:
            return tab_def, col_def

    #
    # TYPE UTILITIES
    #

    def check_type(self, input, type):
        try:
            self.cast_to(input, type)
            return True
        except ServerError:
            return False

    def cast_to(self, input, type):
        try:
            match type:
                case 'int':
                    return int(input)
                case 'float':
                    return float(input)
                case 'string':
                    return str(input)
                case 'bit':
                    val = int(input)
                    assert val in [0, 1]
                    return val
                case 'date':
                    return datetime.strptime(input, '%Y-%m-%d')
                case 'datetime':
                    return datetime.strptime(input, '%Y-%m-%d-%H:%M:%S')
        except:
            pass
        raise ServerError(Error.INVALID_TYPE, 'value: {input}: not of type: {type}')

    #
    # INNER DATABASE METHODS
    #

    def create_database(self, database: str):
        path = self.db_path(database)
        if os.path.exists(path):
            raise ServerError(Error.ALREADY_EXISTS)
        os.makedirs(path)
        # mongo: noop

    def use_database(self, database: str):
        path = self.db_path(database)
        if not os.path.exists(path):
            raise ServerError(Error.DOES_NOT_EXIST)
        self.database = (database, path)
        self.db = self.mongo[database]
        self.idb = self.mongo[f'_{database}_index']

    def drop_database(self, database):
        path = self.db_path(database)
        if self.database is not None and database == self.database[0]:
            self.database = None
            self.db = None
            self.idb = None
        if not os.path.exists(path):
            raise ServerError(Error.DOES_NOT_EXIST)
        shutil.rmtree(path)

        self.mongo.drop_database(database)
        self.mongo.drop_database(f'_{database}_index')

    def create_table(self, table: str):
        if self.database is None:
            raise ServerError(Error.NO_DATABASE_IN_USE)
        path = self.tab_path(table)
        if os.path.exists(path):
            raise ServerError(Error.ALREADY_EXISTS)
        with open(path, 'w') as f:
            f.write('[]')

        # NOTE: no need to create collection
        # self.db.create_collection(table)

    def create_column(self, table, col, col_type, index_type):
        tab_def = self.read_table(table)
        tab_dict = self.table_def_to_dict(tab_def)

        if tab_dict.get(col) is not None:
            raise ServerError(Error.ALREADY_EXISTS, f'column: {col}')

        # TODO: ask whether two primary keys should be handled like a composite key
        # if index_type == 'primary-key-unique' and any(map(lambda c: c['role'] == 'primary-key-unique', tab_def)):
        #     raise ServerError(Error.DUPLICATE_KEY,
        #         "Cannot have more than one unique primary key")

        column = {
            "name": col,
            "type": col_type,
            "role": index_type
        }

        if index_type.startswith('foreign-key'):
            ref = index_type.split('=')[1]
            rtab_def, rcol_def = self.get_reference(ref)

            if rcol_def['role'] not in ['primary-key', 'unique']:
                raise ServerError(Error.INVALID_REFERENCE, f'ref: {ref}: column not unique')

            column['role'] = 'foreign-key'
            column['reference'] = ref

        tab_def.append(column)
        self.write_table(table, tab_def)

        # mongo: noop

    def drop_table(self, table):
        self.check_table(table)
        self.delete(table, {})

        os.remove(self.tab_path(table))
        self.db[table].drop()
        self.idb[f'{table}_fk'].drop()
        self.idb[f'{table}_uq'].drop()

    def insert(self, table, values):
        tab_def = self.read_table(table)

        # check the number of values given
        if len(tab_def) != len(values):
            raise ServerError(Error.INVALID_NUMBER_OF_FIELDS, f"table has {len(tab_def)} columns; {len(values)} values were given")

        # check type of values
        for col, val in zip(tab_def, values):
            if not parser.parser_input(val, col['type']):
                raise ServerError(Error.INVALID_TYPE, f"col: {col['name']}: {val} is not {col['type']}")

        # encode values in {_id, values} format
        keys = []
        vals = []
        for col, val in zip(tab_def, values):
            if col['role'] == 'primary-key':
                keys.append(val)
            else:
                vals.append(val)

        key = "#".join(keys)

        # check for duplicate primary key
        # NOTE: redundant, mongo handles this, but need to synchronize
        # duplicate key exceptions with normal and index inserts
        # TODO: try-except mongo's exception, delete index, then rethrow
        row = self.db[table].find_one({'_id': key})
        if row is not None:
            raise ServerError(Error.DUPLICATE_KEY)

        # check that values are unique and fk are valid refs
        for col, val in zip(tab_def, values):
            match col['role']:
                case 'foreign-key':
                    # check that referenced key exists
                    rtab, rcol = col['reference'].split('.')
                    ref_rows = self.__query(rtab, ['*'], [{'lhs': rcol, 'rhs': val, 'op': '=', 'inner': False}])
                    if len(ref_rows) == 0:
                        raise ServerError(Error.INVALID_REFERENCE, f"ref: {col['reference']}={val}: no such row")
                case 'unique' | 'primary-key':
                    # unique index
                    row = self.idb[f'{table}_uq']\
                        .find_one({'_id': {col['name']: val}})
                    if row is not None:
                        raise ServerError(Error.DUPLICATE_UNIQUE, f"col: {col['name']}={val} already exists")

        # everything is valid, let's index it
        for col, val in zip(tab_def, values):
            match col['role']:
                case 'foreign-key':
                    # fk index
                    rtab, rcol = col['reference'].split('.')
                    row = self.idb[f'{rtab}_fk'].find_one_and_update(
                        {'_id': {rcol: val}}, {"$push": {"refs": {'table': table, 'key': key}}})
                    if row is None:
                        self.idb[f'{rtab}_fk'].insert_one(
                            {'_id': {rcol: val}, "refs": [{'table': table, 'key': key}]})
                case 'unique' | 'primary-key':
                    # unique index
                    row = self.idb[f'{table}_uq']\
                        .find_one({'_id': {col['name']: val}})
                    if row is not None:
                        raise ServerError(Error.DUPLICATE_UNIQUE, f"col: {col['name']}={val} already exists")
                    self.idb[f'{table}_uq'].insert_one(
                        {'_id': {col['name']: val}, "key": key})
                case 'index':
                    # not unique index
                    row = self.idb[f'{table}_nq'].find_one_and_update(
                        {'_id': {col['name']: val}}, {"$push": {"keys": key}})
                    if row is None:
                        self.idb[f'{table}_nq'].insert_one(
                            {'_id': {col['name']: val}, "keys": [key]})

        doc = {
            "_id": '#'.join(keys),
            "values": '#'.join(vals)
        }
        self.db[table].insert_one(doc)

    def delete(self, table, where):
        tab_def = self.read_table(table)
        tab_dict = self.table_def_to_dict(tab_def)

        # no need to validate 'where', select handles it
        rows = self.__query(table, ['*'], where)

        # check if fk constraints let us delete
        # dont delete anything until then
        for row in rows:
            for col in row:
                res = self.idb[f'{table}_fk']\
                    .find_one({'_id': {col: row[col]}})
                if res is not None and len(res['refs']) != 0:
                    refs = ', '.join(map(lambda ref: f'{ref["table"]}:{ref["key"]}', res['refs']))
                    raise ServerError(Error.FOREIGN_KEY_CONSTRAINT, f"{table}.{col}:{row['_id']} referenced by {refs}")

        # TODO: build buffer and bulk mongo commands
        # delete
        for row in rows:
            for colname in row:
                key = row['_id']
                val = row[colname]
                col = tab_dict.get(colname)
                if col is None:
                    continue

                # delete fk index
                match col['role']:
                    case 'foreign-key':
                        ref_tab, ref_col = col['reference'].split('.')
                        self.idb[f'{ref_tab}_fk'].find_one_and_update(
                            {'_id': {ref_col: val}}, {"$pull": {"refs": {"table": table, "key": key}}})
                    # delete uq index
                    case 'unique' | 'primary-key':
                        self.idb[f'{table}_uq'].delete_one(
                            {'_id': {col['name']: val}})
                    # delete nq index
                    case 'index':
                        self.idb[f'{table}_nq'].find_one_and_update(
                            {'_id': {col['name']: val}}, {"$pull": {"keys": key}})
            # delete row
            self.db[table].delete_one({'_id': key})

    # generator function that joins two sets of rows
    def __join_tables(self, name1, name2, rows1, rows2):
        # for all pairs of rows
        for r1 in rows1:
            for r2 in rows2:
                # execute join
                joined = {}
                exclude = []
                for k1 in r1:
                    if r2.get(k1) is not None:
                        joined[f'{name1}.{k1}'] = r1[k1]
                        joined[f'{name2}.{k1}'] = r2[k1]
                        exclude.append(k1)
                    else:
                        joined[k1] = r1[k1]
                for k2 in r2:
                    if k2 not in exclude:
                        joined[k2] = r2[k2]

                yield joined

    def __reconstruct_row(self, tab_dict, cols, row):
        any_select = '*' in cols

        keys = row['_id'].split('#')
        vals = row['values'].split('#')

        doc = {'_id': row['_id']}
        for col in tab_dict:
            if tab_dict[col]['role'] == 'primary-key':
                val = keys.pop(0)
            else:
                val = vals.pop(0)

            if not any_select and col not in cols:
                continue

            match tab_dict[col]['type']:
                case 'int': val = int(val)
                case 'float': val = float(val)
                case 'string': pass
                case 'bit': val = int(val)
                case 'date': val = datetime.strptime(val, '%Y-%m-%d')
                case 'datetime': val = datetime.strptime(val, '%Y-%m-%d-%H:%M:%S')

            doc[col] = val

        return doc

    def __where_check(self, row, where):
        a = {
            "=": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b
        }
        a["/="] = a["!="]

        def wh_check(wh):
            lhs = row[wh['lhs']]
            if wh['inner']:
                if (rhs := row.get(wh['rhs'])) is not None:
                    pass
                elif '.' in wh['rhs'] and (rhs := row.get(wh['rhs'].split('.')[1])) is not None:
                    pass
                else:
                    raise ServerError(Error.INVALID_REFERENCE, 'row: {row}: does not contain column: {wh["rhs"]}')
            else:
                rhs = wh['rhs']
            return a[wh['op']](lhs, rhs)

        return all(map(lambda wh: wh_check(wh), where))

    # check if all primary keys are present in search
    # if so, reconstruct _id by which to search
    def __pk_reconstruction(self, tab_def_dict, where_eq):
        key = []
        for col in tab_def_dict:
            if tab_def_dict[col]['role'] == 'primary-key':
                keyval = where_eq.get(col)
                if keyval is not None:
                    key.append(keyval)
                else:
                    return None
        return "#".join(key)

    # construct query with indexes
    def __index_search(self, table, tab_def_dict, where_eq):
        ids = None
        to_delete = []
        for col_name in where_eq:
            val = where_eq[col_name]
            keys = None
            col = tab_def_dict[col_name]

            # check if indexed
            match col['role']:
                case 'unique':
                    keys = self.idb[f'{table}_uq'].find_one({'_id': {col_name: val}})
                    if keys is not None:
                        keys = [keys["key"]]
                case 'index':
                    keys = self.idb[f'{table}_nq'].find_one({'_id': {col_name: val}})
                    if keys is not None:
                        keys = keys['keys']
                case 'foreign-key':
                    ref_tab, ref_col = col['reference'].split('.')
                    keys = self.idb[f'{ref_tab}_fk'].find_one({'_id': {ref_col: val}})
                    if keys is not None:
                        keys = filter(lambda doc: doc['table'] == table, keys['refs'])
                        keys = map(lambda doc: doc['key'], keys)
                case _:
                    continue

            # index assumed to exist and be valid
            to_delete.append(col_name)

            # no keys to merge with previously found
            if keys is None:
                ids = set()
                continue

            # if first loop then create new set, else intersect with newly found keys
            if ids is None:
                ids = set(keys)
            else:
                ids.intersect_update(keys)

        return (ids, to_delete)

    def __query(self, table, columns, where):
        tab_dict = self.read_table_dict(table)

        # construct where_eq for use in searching by index
        where_eq = {}
        for wh in where:
            # TODO: make this wh['inner'] compatible somehow
            # NOTE: aka, implement hash joins
            # NOTE: not sure that's required, only an optimization
            if wh['op'] == '=' and not wh['inner']:
                where_eq[wh['lhs']] = wh['rhs']

        # query
        key = self.__pk_reconstruction(tab_dict, where_eq)
        if key is not None:
            query_doc = {'_id': key}
        keys, to_delete = self.__index_search(table, tab_dict, where_eq)
        if keys is not None:
            # TODO: delete 'to_delete' lhs-es from where
            query_doc = {'_id': {'$in': list(keys)}}
        else:
            query_doc = {}

        res = self.db[table].find(query_doc)
        res = map(lambda row: self.__reconstruct_row(tab_dict, columns, row), res)
        res = filter(lambda row: self.__where_check(row, where), res)
        res = list(res)

        return res

    # tables: dict[alias, table]
    # columns: list[col]
    # where: list[{lhs, rhs, op, inner}]
    def select(self, tables, columns, where):
        tab_defs = {}
        tab_dicts = {}
        for alias in tables:
            tab = tables[alias]
            tab_defs[alias] = self.read_table(tab)
            tab_dicts[alias] = self.table_def_to_dict(tab_defs[alias])

        # check that projection columns and columns used in filtering actually exist
        where_cols = chain(
            map(lambda wh: wh['lhs'], where),
            map(lambda wh: wh['rhs'],
                filter(lambda wh: wh['inner'], where)))

        if '*' in columns:
            to_check = where_cols
        else:
            to_check = chain(columns, where_cols)

        # figure out which name refers to which column
        col_ref = {}
        for col in to_check:
            if col_ref.get(col) is not None:
                continue

            ref = None
            if '.' in col:
                rtab, rcol = col.split('.')
                rtab_def = tab_dicts.get(rtab)
                if rtab_def is None:
                    raise ServerError(Error.INVALID_REFERENCE, f"ref: {col}: no such table")
                rcol_def = rtab_def.get(rcol)
                if rcol_def is None:
                    raise ServerError(Error.INVALID_REFERENCE, f"ref: {col}: no such column")
                ref = (rtab, rcol)
            else:
                for tab in tab_defs:
                    tab_dict = tab_dicts[tab]
                    if tab_dict.get(col) is not None:
                        if col_ref.get(col) is not None:
                            raise ServerError(Error.AMBIGUOUS_REFERENCE, f'ref: {ref}')
                        else:
                            ref = (tab, col)
            if ref is None:
                raise ServerError(Error.INVALID_REFERENCE, f'ref: {col}')

            # the reference for the column is stored
            col_ref[col] = ref

        # figure out what applies to which subselect
        if '*' in columns:
            col_by_table = None
        else:
            col_by_table = {}
            for col in columns:
                rtab, rcol = col_ref[col]
                if rtab not in col_by_table:
                    col_by_table[rtab] = []
                col_by_table[rtab].append(rcol)

        wh_by_table = {}
        wh_by_join = {}
        for wh in where:
            lref = col_ref.get(wh['lhs'])
            if wh['inner']:
                # if both lhs and rhs refer to columns
                rref = col_ref.get(wh['rhs'])
                if lref[0] == rref[0]:
                    # if they are on the same table
                    key = lref[0]
                    dict = wh_by_table
                    val = wh.copy()
                    val['lhs'] = val['lhs'].split('.')[-1]
                    val['rhs'] = val['rhs'].split('.')[-1]
                else:
                    # if they are on different tables
                    key = frozenset([lref[0], rref[0]])  # frozenset, ignorant of order
                    dict = wh_by_join
                    val = wh
            else:
                key = lref[0]
                dict = wh_by_table
                val = wh.copy()
                val['lhs'] = val['lhs'].split('.')[-1]

            if key not in dict:
                dict[key] = []

            dict[key].append(val)

        # do the joined query
        rows0 = None
        alias0 = None
        for alias1 in tables:
            cols = ['*']
            if col_by_table is not None:
                cols = col_by_table[alias1]

            wh = []
            if wh_by_table.get(alias1) is not None:
                wh = wh_by_table[alias1]

            rows1 = self.__query(tables[alias1], cols, wh)
            if rows0 is None:
                rows0 = rows1
                alias0 = alias1
                continue

            rows0 = self.__join_tables(alias0, alias1, rows0, rows1)
            if (whs := wh_by_join.get(frozenset([alias0, alias1]))) is not None:
                rows0 = filter(lambda row: self.__where_check(row, whs), rows0)
                # TODO: test removing this
                rows0 = list(rows0)
            alias0 = alias1

        return rows0

    def run_command(self, command) -> (int, str):
        '''
            returns (code, message)

            create_database DATABASE
            drop_database DATABASE
            use_database DATABASE
            create_table TABLE
            create_column TABLE CNAME CTYPE [ primary-key, foreign-key=TABLE.COLNAME, unique, index, none ]
            drop_table TABLE
            insert into TABLE values VALUES#..
            delete TABLE [ where VAR=VAL .. ]
            select [ * | COL,.. ] from TABLE[:ALIAS],.. [ where VAR=VAL|TAB1.COL1=&TAB2.COL2 .. ]
        '''

        def parse_where_clause(where_clause):
            if len(where_clause) == 0:
                return []

            if where_clause[0] == 'where':
                where_clause = where_clause[1:]
            ops = ['>=', '<=', '/=', '!=', '<', '>', '=']
            where = []
            for wh in where_clause:
                if wh.lower() == 'and':
                    continue

                op = list(filter(lambda op: op in wh, ops))
                if len(op) > 1 and op[1] in op[0]:
                    op = op[0]
                elif len(op) != 1:
                    raise ServerError(Error.INVALID_COMMAND, f"where clause: {wh}")
                else:
                    op = op[0]

                a = wh.split(op)
                if len(a) != 2:
                    raise ServerError(Error.INVALID_COMMAND, f"where clause: {wh}")

                # set 'inner' if both lhs and rhs is a column reference
                if a[1][0] == '&':
                    a[1] = a[1][1:]
                    inner = True
                else:
                    inner = False

                where.append({
                    'op': op,
                    'lhs': a[0],
                    'rhs': a[1],
                    'inner': inner
                })

            return where

        try:
            match command.split():
                case ["create_database", db]:
                    self.create_database(db)
                case ["drop_database", db]:
                    self.drop_database(db)
                case ["use_database", db]:
                    self.use_database(db)
                case ["create_table", table]:
                    self.create_table(table)
                case ["create_column", table, col_name, col_type, index_type] if index_type.split('=')[0] in [
                        "primary-key", "foreign-key", "unique", "index", "none"]:
                    self.create_column(table, col_name, col_type, index_type)
                case ["drop_table", table]:
                    self.drop_table(table)
                case ["insert", "into", table, "values", values]:
                    values = values.split('#')
                    self.insert(table, values)
                case ["delete", table, *where_clause]:
                    where = parse_where_clause(where_clause)
                    self.delete(table, where)
                case ["select", cols, "from", tables, *where_clause]:
                    cols = cols.split(',')
                    if '*' in cols:
                        cols = ['*']
                    tables_ = {}
                    for tab in tables.split(','):
                        spl = tab.split(':')
                        if len(spl) == 2:
                            table, alias = spl
                            tables_[alias] = table
                        else:
                            tables_[tab] = tab

                    where = parse_where_clause(where_clause)
                    rows = self.select(tables_, cols, where)

                    return int(Error.ROWS), rows
                case _:
                    return int(Error.INVALID_COMMAND), command

        except ServerError as e:
            return e.code, e.message
        except pymongo.errors.DuplicateKeyError:
            return Error.DUPLICATE_KEY, ""
        return int(Error.SUCCESS), ""

    def run(self, io_read, io_write, io_log=None):
        if io_log is None:
            io_log = io_write

        # for line in io_read:
        while True:
            if io_read.seekable():
                cur_pos = io_read.tell()
            line = io_read.readline()[:-1]
            if io_read.seekable() and cur_pos == io_read.tell():
                break

            line = line.strip()
            if len(line) == 0:
                continue

            code, message = self.run_command(line)
            if code != Error.ROWS:
                message = f'[{Error(code).name}] {message}\n'
                io_write.write(message)
                io_write.flush()
            else:
                io_write.write(f'[{Error(code).name}]\n')
                io_write.flush()
                for row in message:
                    io_write.write(f'{row}\n')
                    io_write.flush()
                io_write.write('---\n')
                io_write.flush()

    def listen(self, address=('localhost', 25567)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(address)
        sock.listen(5)
        print(f'Server running on {address}')

        with sock:
            while True:
                s, addr = sock.accept()
                print(f'{addr} connected')
                with s:
                    s_rd = s.makefile('r')
                    s_wr = s.makefile('w')
                    self.run(s_rd, s_wr)
                print(f'{addr} disconnected')


if __name__ == "__main__":
    server = Server(os.getcwd())
    if len(sys.argv) > 1:
        for path in sys.argv[1:]:
            if path == '-':
                server.run(stdin, stdout, stderr)
            else:
                input = open(path, 'r')
                server.run(input, stdout, stderr)
                input.close()
    else:
        server.listen()
