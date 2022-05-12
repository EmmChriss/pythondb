import json
import os
import sys
import shutil
import socket
import parser
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
        table = {}
        for col in tab_def:
            table[col['name']] = col
        return table

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
        tab, col = split
        tab_def = self.read_table_dict(tab)

        col_def = tab_def.get(col)
        if col_def is None:
            raise ServerError(Error.INVALID_REFERENCE, f'ref: {reference}')
        else:
            return tab_def, col_def

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

        # self.db.create_collection(table)

    def create_column(self, table, col_name, col_type, index_type):
        table_def = self.read_table(table)

        if any(map(lambda c: c['name'] == col_name, table_def)):
            raise ServerError(Error.ALREADY_EXISTS)

        if index_type == 'primary-key-unique' and any(map(lambda c: c['role'] == 'primary-key-unique', table_def)):
            raise ServerError(Error.DUPLICATE_KEY,
                "Cannot have more than one unique primary key")

        column = {
            "name": col_name,
            "type": col_type,
            "role": index_type
        }

        if index_type.startswith('foreign-key'):
            reference = index_type.split('=')[1]
            ref_tab, ref_col = self.get_reference(reference)

            if ref_col['role'] not in ['primary-key-unique', 'unique']:
                raise ServerError(Error.INVALID_REFERENCE, f'ref: {reference}: referenced column not unique')

            column['role'] = 'foreign-key'
            column['reference'] = reference

        table_def.append(column)
        self.write_table(table, table_def)

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
        if len(tab_def) != len(values):
            raise ServerError(Error.INVALID_NUMBER_OF_FIELDS)
        if any(map(lambda cv: not parser.parser_input(cv[1], cv[0]['type']), zip(tab_def, values))):
            raise ServerError(Error.INVALID_TYPE)

        keys = []
        vals = []
        for col, val in zip(tab_def, values):
            match col['role']:
                case 'primary-key-unique' | 'primary-key-not-unique': keys.append(val)
                case _: vals.append(val)

        key = "#".join(keys)

        # check for duplicate primary key
        # NOTE: redundant, mongo handles this, but need to synchronize
        # duplicate key exceptions with normal and index inserts
        # TODO: try-except mongo's exception, delete index, then rethrow
        row = self.db[table].find_one({'_id': key})
        if row is not None:
            raise ServerError(Error.DUPLICATE_KEY)

        for col, val in zip(tab_def, values):
            match col['role']:
                case 'foreign-key':
                    # check that referenced key exists
                    ref_tab, ref_col = col['reference'].split('.')
                    ref_rows = self.select(ref_tab, ['*'], {ref_col: val})
                    if len(ref_rows) == 0:
                        raise ServerError(Error.INVALID_REFERENCE, f"ref: {col['reference']}={val}: no such row")
                    # reference is valid, let's index it
                    row = self.idb[f'{ref_tab}_fk'].find_one_and_update(
                        {'_id': {ref_col: val}}, {"$push": {"refs": {'table': table, 'key': key}}})
                    if row is None:
                        self.idb[f'{ref_tab}_fk'].insert_one(
                            {'_id': {ref_col: val}, "refs": [{'table': table, 'key': key}]})
                case 'unique':
                    # unique index
                    row = self.idb[f'{table}_uq']\
                        .find_one({'_id': {col['name']: val}})
                    if row is not None:
                        raise ServerError(Error.DUPLICATE_UNIQUE)
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
        tab_def = self.read_table_dict(table)

        # no need to validate 'where', select handles it
        rows = self.select(table, ['*'], where)

        # check if fk constraints let us delete
        # dont delete anything until then
        for row in rows:
            for col in row:
                res = self.idb[f'{table}_fk']\
                    .find_one({'_id': {col: row[col]}})
                if res is not None and len(res['refs']) != 0:
                    raise ServerError(Error.FOREIGN_KEY_CONSTRAINT)

        # delete
        for row in rows:
            for colname in row:
                key = row['_id']
                val = row[colname]
                col = tab_def[colname]
                if col is None:
                    continue

                # delete fk index
                if col['role'] == 'foreign-key':
                    ref_tab, ref_col = col['reference'].split('.')
                    self.idb[f'{ref_tab}_fk'].find_one_and_update(
                        {'_id': {ref_col: val}}, {"$pull": {"refs": {"table": table, "key": key}}})
                # delete uq index
                elif col['role'] == 'unique':
                    self.idb[f'{table}_uq'].delete_one(
                        {'_id': {col['name']: val}})
                # delete nq index
                elif col['role'] == 'index':
                    self.idb[f'{table}_nq'].find_one_and_update(
                        {'_id': {col['name']: val}}, {"$pull": {"keys": key}})
            # delete row
            self.db[table].delete_one({'_id': key})

    #
    # UTILITY FUNCTIONS FOR SELECT
    #

    def __join_tables(self, name1, name2, rows1, rows2):
        rows = []
        # for all pairs of rows
        for r1 in rows1:
            for r2 in rows2:
                # execute join
                joined = {}
                for k1 in r1:
                    if r2.get(k1) is not None:
                        joined[f'{name1}.{k1}'] = r1[k1]
                        joined[f'{name2}.{k1}'] = r2[k1]
                        del r2[k1]
                    else:
                        joined[k1] = r1[k1]
                for k2 in r2:
                    joined[k2] = r2[k2]
                rows.append(joined)
        return rows

    def __reconstruct_row(tab_def: dict, row):
        keys = row['_id'].split('#')
        vals = row['values'].split('#')

        doc = {'_id': row['_id']}
        for tab in tab_def:
            if tab['role'].startswith('primary-key'):
                doc[tab['name']] = keys.pop(0)
            else:
                doc[tab['name']] = vals.pop(0)
        return doc

    def select(self, table, columns, where):
        table_names = table.split(',')
        tab_defs = {}
        for tab_name in table_names:
            tab_defs[tab_name] = self.read_table_dict(tab_name)

        # check that projection columns and columns used in filtering actually exist
        if '*' in columns:
            to_check = where
        else:
            to_check = chain(columns, where)

        # check if each word refers to a column
        for col in to_check:
            ref = None
            if self.is_reference_valid(col):
                ref = self.get_reference(col)
            else:
                for tab in tab_defs:
                    if tab.get(col) is not None:
                        if ref is not None:
                            raise ServerError(Error.AMBIGUOUS_REFERENCE)
                        else:
                            ref = (tab, col)

            if ref is None:
                raise ServerError(Error.INVALID_REFERENCE, f'ref: {table}.{wh}')

        def where_check(row):
            return all(map(lambda field: str(row[field]) == str(where[field]), where))

        # very inefficient
        # TODO: think about providing a dictionary API to access column defs by name
        def get_col_def(name):
            for col in tab_def:
                if col['name'] == name:
                    return col
            return None

        # check if all primary keys are present in search
        key = []
        complete = True
        for col in tab_def:
            match col['role']:
                case 'primary-key-unique' | 'primary-key-not-unique':
                    keyval = where.get(col['name'])
                    if keyval is not None:
                        key.append(keyval)
                    else:
                        complete = False
                        break
        if complete:
            key = "#".join(key)
        else:
            key = None

        # construct query with indexes
        ids = None
        to_delete = []
        for col_name in where:
            # bail early if we found a complete key
            if key is not None:
                break

            val = where[col_name]
            keys = None
            col = get_col_def(col_name)

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

        # delete fields searched with index
        for field in to_delete:
            del where[field]

        # query
        if key is not None:
            query_doc = {'_id': key}
        if ids is not None:
            query_doc = {'_id': {'$in': list(ids)}}
        else:
            query_doc = {}

        res = self.db[table].find(query_doc)
        res = filter(lambda doc: doc["values"] != "", res)
        res = [reconstruct_row(row) for row in res]

        # filter
        res = list(filter(lambda row: where_check(row), res))

        # ugly projection
        if '*' not in columns:
            for rowi in range(len(res)):
                row = res[rowi]
                keys = list(row.keys())
                for col in keys:
                    if col not in columns:
                        del row[col]
        return res

    def run_command(self, command) -> (int, str):
        '''
            returns (code, message)

            create_database DATABASE
            drop_database DATABASE
            use_database DATABASE
            create_table TABLE
            create_column TABLE CNAME CTYPE [ primary-key-unique,
                primary-key-not-unique, foreign-key=TABLE.COLNAME, unique, index, none ]
            drop_table TABLE
            insert into TABLE values VALUES#..
            delete TABLE [ where VAR=VAL .. ]
            select [ * | COL,.. ] from TABLE [ where VAR=VAL .. ]
        '''
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
                        "primary-key-unique", "primary-key-not-unique",
                        "foreign-key", "unique", "index", "none"]:
                    self.create_column(table, col_name, col_type, index_type)
                case ["drop_table", table]:
                    self.drop_table(table)
                case ["insert", "into", table, "values", values]:
                    values = values.split('#')
                    self.insert(table, values)
                case ["delete", table, *where_clause]:
                    match where_clause:
                        case ["where", *_]:
                            where_clause = where_clause[1:]
                    where = {}
                    for k, v in map(lambda w: w.split('='), where_clause):
                        where[k] = v
                    self.delete(table, where)
                case ["select", cols, "from", table, *where_clause]:
                    match where_clause:
                        case ["where", *_]:
                            where_clause = where_clause[1:]
                    where_clause = filter(lambda st: "and" not in st, where_clause)
                    cols = cols.split(',')
                    if '*' in cols:
                        cols = ['*']

                    where = {}
                    split = map(lambda w: w.split('='), where_clause)
                    for arr in split:
                        k, v = arr
                        where[k] = v

                    rows = self.select(table, cols, where)
                    for row in rows:
                        print(row)
                case _ :
                    # if command == "SECTION":
                    #     breakpoint()
                    return int(Error.INVALID_COMMAND), command

        except ServerError as e:
            return e.code, e.message
        except pymongo.errors.DuplicateKeyError as e:
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
            message = f'[{Error(code).name}] {message}\n'
            if code != Error.SUCCESS:
                io_log.write(message)
                io_log.flush()
            else:
                io_write.write(message)
                io_write.flush()

    def listen(self, address=('localhost', 25565)):
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
