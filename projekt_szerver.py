import json
import os
import sys
import shutil
import socket
import time
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
        raise ServerError(Error.INVALID_TYPE, f'value: {input}: not of type: {type}')

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
        self.idb[f'{table}_uq'].drop()
        self.idb[f'{table}_nq'].drop()

    def insert(self, table, values):
        tab_def = self.read_table(table)

        # check the number of values given
        if len(tab_def) != len(values):
            raise ServerError(Error.INVALID_NUMBER_OF_FIELDS, f"table has {len(tab_def)} columns; {len(values)} values were given")

        # check type of values
        for col, val in zip(tab_def, values):
            if not self.check_type(val, col['type']):
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
                    row = self.idb[f'{rtab}_uq'].find_one({'_id': {rcol: val}})
                    if row is None:
                        raise ServerError(Error.INVALID_REFERENCE, f"ref: {col['reference']}={val}: no such row")
                case 'unique' | 'primary-key':
                    # unique index
                    row = self.idb[f'{table}_uq'].find_one({'_id': {col['name']: val}})
                    if row is not None:
                        raise ServerError(Error.DUPLICATE_UNIQUE, f"col: {col['name']}={val} already exists")

        # fk -> uq
        # uq: { _id: {col1: val1}, key: _id, refs: [{table: table1, key: key1}, ..] }
        # nq: { _id: {col1: val1}, keys: [_id] }

        # everything is valid, let's index it
        for col, val in zip(tab_def, values):
            match col['role']:
                case 'foreign-key':
                    # fk index
                    rtab, rcol = col['reference'].split('.')
                    self.idb[f'{rtab}_uq'].update_one({'_id': {rcol: val}},
                        {"$push": {"refs": {'table': table, 'key': key}}})
                case 'unique' | 'primary-key':
                    # unique index
                    self.idb[f'{table}_uq'].insert_one({'_id': {col['name']: val}, "key": key, "refs": []})
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
        rows = self.__query(table, {'*': '*'}, where)

        # check if fk constraints let us delete
        # dont delete anything until then
        for row in rows:
            for col in row:
                if col == '_id':
                    continue

                res = self.idb[f'{table}_uq'].find_one({'_id': {col: str(row[col])}})
                if res is not None and len(res['refs']) != 0:
                    refs = ', '.join(map(lambda ref: f'{ref["table"]}:{ref["key"]}', res['refs']))
                    raise ServerError(Error.FOREIGN_KEY_CONSTRAINT, f"{table}.{col}:{row['_id']} referenced by {refs}")

        # TODO: build buffer and bulk mongo commands
        # delete
        for row in rows:
            for colname in row:
                key = row['_id']
                val = str(row[colname])
                col = tab_dict.get(colname)
                if col is None:
                    continue

                # delete fk index
                match col['role']:
                    case 'foreign-key':
                        ref_tab, ref_col = col['reference'].split('.')
                        self.idb[f'{ref_tab}_uq'].update_one({'_id': {ref_col: val}},
                            {"$pull": {"refs": {"table": table, "key": key}}})
                    # delete uq index
                    case 'unique' | 'primary-key':
                        self.idb[f'{table}_uq'].delete_one(
                            {'_id': {col['name']: val}})
                    # delete nq index
                    case 'index':
                        self.idb[f'{table}_nq'].update_one({'_id': {col['name']: val}},
                            {"$pull": {"keys": key}})
            # delete row
            self.db[table].delete_one({'_id': key})

    # generator function that joins two sets of rows
    def __join_tables(self, name1, name2, rows1, rows2, wh_eq, hash_join):
        # for all pairs of rows
        for r1 in rows1:
            if hash_join is None:
                idxs = range(len(rows2))
            else:
                vals = tuple(map(lambda w: r1[w], wh_eq.values()))
                idxs = hash_join.get(vals)
                if idxs is None:
                    continue
            for r2 in idxs:
                r2 = rows2[r2]
                # execute join
                joined = r1.copy()
                if '_id' in joined:
                    joined[f'{name1}._id'] = joined['_id']
                    del joined['_id']
                joined.update(r2)
                if '_id' in joined:
                    joined[f'{name2}._id'] = joined['_id']
                    del joined['_id']

                yield joined

    def __reconstruct_row(self, tab_dict, cols, row):
        any_select = '*' in cols
        aliased = type(cols) == dict and not any_select

        keys = row['_id'].split('#')
        vals = row['values'].split('#')

        doc = {'_id': row['_id']}
        for col in tab_dict:
            if tab_dict[col]['role'] == 'primary-key':
                val = keys.pop(0)
            else:
                val = vals.pop(0)

            if not (any_select or col in cols):
                continue

            val = self.cast_to(val, tab_dict[col]['type'])

            if aliased:
                doc[cols[col]] = val
            else:
                doc[col] = val

        return doc

    AGGREGATE_LAMBDA = {
        "avg": {
            'type': 'float',
            'ctx': lambda: (0, 0),
            'each': lambda t, val: (t[0] + 1, t[1] + val),
            'post': lambda t: t[1] / t[0]
        },
        "count": {
            'type': 'int',
            'ctx': lambda: 0,
            'each': lambda t, val: t + 1,
            'post': lambda t: t
        },
        "sum": {
            'ctx': lambda: 0,
            'each': lambda t, val: t + val,
            'post': lambda t: t
        },
        "min": {
            'ctx': lambda: None,
            'each': lambda t, val: t and min(t, val) or val,
            'post': lambda t: t
        },
        "max": {
            'ctx': lambda: None,
            'each': lambda t, val: t and max(t, val) or val,
            'post': lambda t: t
        }
    }

    def __aggregate_rows(self, rows, aggregate, group_by):
        hash_group = {}
        for row in rows:
            tup = tuple(map(lambda k: row[k], group_by))
            if (aggr_row := hash_group.get(tup)) is not None:
                for a in aggregate:
                    aggr_row[a['name']] = Server.AGGREGATE_LAMBDA[a['op']]['each'](aggr_row[a['name']], row[a['row']])
            else:
                initial_row = {c: row[c] for c in group_by}
                for a in aggregate:
                    initial_row[a['name']] = Server.AGGREGATE_LAMBDA[a['op']]['ctx']()
                    initial_row[a['name']] = Server.AGGREGATE_LAMBDA[a['op']]['each'](initial_row[a['name']], row[a['row']])
                hash_group[tup] = initial_row

        # finalize
        for tup in hash_group:
            row = hash_group[tup]
            for a in aggregate:
                row[a['name']] = Server.AGGREGATE_LAMBDA[a['op']]['post'](row[a['name']])
            yield row

    def __project_rows(self, rows, projection):
        for row in rows:
            for col in list(row.keys()):
                if col not in projection:
                    del row[col]

            for pj in projection:
                if pj != projection[pj]:
                    row[projection[pj]] = row[pj]
                    del row[pj]

            yield row

    OP_LAMBDA = {
        "=": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "/=": lambda a, b: a != b,
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b
    }

    def __where_check(self, row, where):
        def wh_check(wh):
            lhs = row[wh['lhs']]
            if wh['inner']:
                rhs = row[wh['rhs']]
            else:
                rhs = wh['rhs']
            return Server.OP_LAMBDA[wh['op']](lhs, rhs)

        return all(map(lambda wh: wh_check(wh), where))

    # check if all primary keys are present in search
    # if so, reconstruct _id by which to search
    def __pk_reconstruction(self, tab_def_dict, where_eq):
        key = []
        for col in tab_def_dict:
            if tab_def_dict[col]['role'] == 'primary-key':
                keyval = where_eq.get(col)
                if keyval is not None:
                    key.append(str(keyval))
                else:
                    return None

        return "#".join(key)

    # construct query with indexes
    def __index_search(self, table, tab_def_dict, where_eq):
        ids = None
        to_delete = []
        for col_name in where_eq:
            val = where_eq[col_name]
            col_name = col_name.split('.')[-1]
            keys = None
            col = tab_def_dict[col_name]

            # check if indexed
            match col['role']:
                case 'unique' | 'primary-key':
                    index = self.idb[f'{table}_uq'].find_one({'_id': {col_name: str(val)}})
                    if index is not None:
                        keys = [index["key"]]
                case 'index':
                    index = self.idb[f'{table}_nq'].find_one({'_id': {col_name: str(val)}})
                    if index is not None:
                        keys = index['keys']
                case 'foreign-key':
                    ref_tab, ref_col = col['reference'].split('.')
                    index = self.idb[f'{ref_tab}_uq'].find_one({'_id': {ref_col: str(val)}})
                    if index is not None:
                        index = filter(lambda doc: doc['table'] == table, index['refs'])
                        keys = map(lambda doc: doc['key'], index)
                case _:
                    continue

            # index assumed to exist and be valid
            to_delete.append(col_name)

            # no keys to merge with previously found
            if keys is None:
                ids = set()
                break

            # if first loop then create new set, else intersect with newly found keys
            if ids is None:
                ids = set(keys)
            else:
                ids.intersection_update(keys)

        return (ids, to_delete)

    def __query(self, table, columns, where):
        any_select = '*' in columns
        tab_dict = self.read_table_dict(table)

        # cast values in where to column types
        for wh in where:
            if wh['inner']:
                continue

            # NOTE: god-awful hack but does the job
            # reverse lookup original column name from alias
            orig_column = None
            for col in columns:
                if columns[col] == wh['lhs']:
                    orig_column = col
                    break

            wh['rhs'] = self.cast_to(wh['rhs'], tab_dict[orig_column or wh['lhs']]['type'])

        # construct where_eq for use in searching by index
        where_eq = {}
        for wh in where:
            # TODO: make this wh['inner'] compatible somehow
            # NOTE: aka, implement hash joins
            if wh['op'] == '=' and not wh['inner']:
                where_eq[wh['lhs']] = wh['rhs']

        # see if a complete _id can be reconstructed from where
        key = self.__pk_reconstruction(tab_dict, where_eq)
        if key is not None:
            query_doc = {'_id': key}

        # normal index search
        keys, to_delete = self.__index_search(table, tab_dict, where_eq)

        # remove where conditions that were indexed
        where = filter(lambda wh: not (wh['op'] == '=' and not wh['inner'] and wh['lhs'] in to_delete), where)
        where = list(where)

        # query
        if keys is not None:
            query_doc = {'_id': {'$in': list(keys)}}
        else:
            query_doc = {}

        res = self.db[table].find(query_doc)
        res = map(lambda row: self.__reconstruct_row(tab_dict, columns, row), res)
        res = filter(lambda row: self.__where_check(row, where), res)
        res = list(res)

        return res

    # tables: dict[alias, table]
    # columns: dict[alias, col]
    # where: list[{lhs, rhs, op, inner}]
    def select(self, tables, columns, where, having):
        any_select = '*' in columns

        group_by = []
        aggregate = []
        to_delete = []
        to_append = []
        for alias in columns:
            col = columns[alias]
            if '(' in col:
                spl = col.split('(')
                if len(spl) != 2:
                    raise ServerError(Error.INVALID_AGGREGATION, f'col: {col}')
                func, spl = spl

                spl = spl.split(')')
                if len(spl) != 2:
                    raise ServerError(Error.INVALID_AGGREGATION, f'col: {col}')
                inner_col, spl = spl

                if spl != '':
                    raise ServerError(Error.INVALID_AGGREGATION, f'col: {col}')

                if func not in Server.AGGREGATE_LAMBDA:
                    raise ServerError(Error.INVALID_AGGREGATION, f'col: {col}: no such aggregation function')

                aggregate.append({
                    'op': func,
                    'row': inner_col,
                    'name': alias,
                    'type': Server.AGGREGATE_LAMBDA[func].get('type') or None
                })
                to_delete.append(alias)
                to_append.append(inner_col)
            else:
                group_by.append(alias)

        for col in to_delete:
            del columns[col]

        for col in to_append:
            columns[col] = col

        with_id = '_id' in columns
        if with_id:
            del columns['_id']

        tab_defs = {}
        tab_dicts = {}
        for alias in tables:
            tab = tables[alias]
            tab_defs[alias] = self.read_table(tab)
            tab_dicts[alias] = self.table_def_to_dict(tab_defs[alias])

        # map all possible names to their reference
        col_ref = {}
        ambiguous_refs = []
        for alias in tab_dicts:
            tab_dict = tab_dicts[alias]
            for col in tab_dict:
                if col_ref.get(col) is not None:
                    del col_ref[col]
                    ambiguous_refs.append(col)
                elif col not in ambiguous_refs:
                    col_ref[col] = (alias, col)
                col_ref[f'{alias}.{col}'] = (alias, col)

        # take aliased columns into account
        alias_ref = {}
        for alias in columns:
            if alias == '*':
                continue

            col = columns[alias]

            # alias references non-existing column
            if (ref := col_ref.get(col)) is None:
                if col in ambiguous_refs:
                    raise ServerError(Error.AMBIGUOUS_REFERENCE, f'ref: {col}; try table.column format')
                else:
                    raise ServerError(Error.INVALID_REFERENCE, f"ref: {col}: no such column")

            # alias collides with existing name
            # if col_ref.get(alias) is not None:
            #     raise ServerError(Error.AMBIGUOUS_REFERENCE, f'alias: {alias}: collides with {".".join(col_ref[alias])}')

            col_ref[alias] = ref
            alias_ref[ref] = alias

        # check that projection columns and columns used in filtering actually exist
        where_cols = chain(
            map(lambda wh: wh['lhs'], where),
            map(lambda wh: wh['rhs'],
                filter(lambda wh: wh['inner'], where)))
        to_check = chain(columns, where_cols)
    
        # collect the minimal amount of columns needed for the query to work
        col_by_table = {}
        for col in to_check:
            if col == '*':
                continue

            if (ref := col_ref.get(col)) is None:
                if col in ambiguous_refs:
                    raise ServerError(Error.AMBIGUOUS_REFERENCE, f'ref: {col}; try table.column format')
                else:
                    raise ServerError(Error.INVALID_REFERENCE, f"ref: {col}: no such column")

            rtab, rcol = ref
            if rtab not in col_by_table:
                col_by_table[rtab] = {}
            col_by_table[rtab][rcol] = alias_ref.get(ref) or f'{rtab}.{rcol}'

        # if any_select, override everything with every column
        if any_select:
            for alias in tab_dicts:
                tab_dict = tab_dicts[alias]
                if alias not in col_by_table:
                    col_by_table[alias] = {}
                for col in tab_dict:
                    ref = col_ref[f'{alias}.{col}']
                    rtab, rcol = ref
                    col_by_table[alias][rcol] = alias_ref.get(ref) or f'{rtab}.{rcol}'

        wh_by_table = {}
        wh_by_join = {}
        for wh in where:
            lref = col_ref[wh['lhs']]

            val = wh.copy()
            val['lhs'] = col_by_table[lref[0]][lref[1]]

            if wh['inner']:
                # if both lhs and rhs refer to columns
                rref = col_ref.get(wh['rhs'])
                val['rhs'] = col_by_table[rref[0]][rref[1]]

                if lref[0] == rref[0]:
                    # if they are on the same table
                    key = lref[0]
                    dict = wh_by_table
                else:
                    # if they are on different tables
                    key = frozenset([lref[0], rref[0]])  # frozenset, ignorant of order
                    dict = wh_by_join
                    val['lhs'] = col_by_table[lref[0]][lref[1]]
                    val['rhs'] = col_by_table[rref[0]][rref[1]]
            else:
                key = lref[0]
                dict = wh_by_table

            if key not in dict:
                dict[key] = []

            dict[key].append(val)

        # do the joined query
        rows0 = None
        alias0 = None
        for alias1 in tables:
            cols = col_by_table.get(alias1) or {'*': '*'}
            wh = wh_by_table.get(alias1) or {}
            rows1 = self.__query(tables[alias1], cols, wh)
            if rows0 is None:
                rows0 = rows1
                alias0 = alias1
                continue

            whs = wh_by_join.get(frozenset([alias0, alias1])) or []

            # construct hash-join table
            whs_eq = {}
            for w in whs:
                if w['op'] == '=':
                    if w['lhs'] in rows1[0]:
                        whs_eq[w['lhs']] = w['rhs']
                    else:
                        whs_eq[w['rhs']] = w['lhs']

            whs = filter(lambda w: w['op'] != '=', whs)
            whs = list(whs)

            if len(whs_eq) == 0:
                hash_join = None
            else:
                hash_join = {}
            for i, row in enumerate(rows1):
                if hash_join is None:
                    break

                vals = tuple(map(lambda k: row[k], whs_eq.keys()))
                if hash_join.get(vals) is None:
                    hash_join[vals] = []
                hash_join[vals].append(i)

            rows0 = self.__join_tables(alias0, alias1, rows0, rows1, whs_eq, hash_join)
            rows0 = filter(lambda row: self.__where_check(row, whs), rows0)
            alias0 = alias1

        if not any_select:
            projection = {}
            if with_id:
                projection['_id'] = '_id'

            for col in columns:
                projection[col] = col

            rows0 = self.__project_rows(rows0, projection)

        if len(aggregate) > 0:
            rows0 = self.__aggregate_rows(rows0, aggregate, group_by)

            # cast having to row's type
            for hv, aggr in zip(having, aggregate):
                if aggr['type'] is not None:
                    type = aggr['type']
                else:
                    rtab, rcol = col_ref[aggr['row']]
                    col_def = tab_dicts[rtab][rcol]
                    type = col_def['type']

                hv['rhs'] = self.cast_to(hv['rhs'], type)

            # filter by having
            rows0 = filter(lambda row: self.__where_check(row, having), rows0)
        return rows0

    def run_command(self, command) -> (int, str):
        def parse_clause(clause):
            if len(clause) == 0:
                return []

            if 'where' in clause:
                clause.remove("where")

            where = []
            for wh in clause:
                op = list(filter(lambda op: op in wh, Server.OP_LAMBDA.keys()))
                if len(op) > 1 and op[1] in op[0]:
                    op = op[0]
                elif len(op) != 1:
                    raise ServerError(Error.INVALID_COMMAND, f"clause: {wh}")
                else:
                    op = op[0]

                a = wh.split(op)
                if len(a) != 2:
                    raise ServerError(Error.INVALID_COMMAND, f"clause: {wh}")

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
                    where = parse_clause(where_clause)
                    self.delete(table, where)
                case ["select", cols, "from", tables, *clauses]:

                    # column aliasing
                    cols = cols.split(',')
                    if '*' in cols:
                        cols_ = {'*': '*'}
                    else:
                        cols_ = {}
                        for col in cols:
                            spl = col.split('=')
                            if len(spl) == 2:
                                col, alias = spl
                                cols_[alias] = col
                            else:
                                cols_[col] = col

                    # table aliasing
                    tables_ = {}
                    for tab in tables.split(','):
                        spl = tab.split('=')
                        if len(spl) == 2:
                            table, alias = spl
                            tables_[alias] = table
                        else:
                            tables_[tab] = tab

                    clause_map = {}
                    clause_map['where'] = []
                    clause_map['having'] = []

                    where = []
                    group_by = []
                    while len(clauses) > 0:
                        match clauses:
                            case ["join", tab, *rest]:
                                clauses = rest

                                spl = tab.split('=')
                                if len(spl) == 2:
                                    table, alias = spl
                                    tables_[alias] = table
                                else:
                                    tables_[tab] = tab
                            case ['on', cond, *rest]:
                                clauses = rest
                                wh = parse_clause(cond.split(','))
                                wh[0]['inner'] = True
                                where.append(wh[0])
                            case ['group_by', cols, *rest]:
                                clauses = rest
                                group_by = cols.split(',')
                            case _:
                                if clauses[0] not in ['where', 'having']:
                                    raise ServerError(Error.INVALID_COMMAND, f'invalid clause: {clauses}')
                                next_clause = next((i+1 for i, cl in enumerate(clauses[1:]) if cl in ['where', 'having', 'group_by']), None)
                                current_clause = clauses[:next_clause]
                                clause_map[clauses[0]].extend(current_clause[1:])
                                if next_clause is None:
                                    clauses = []
                                else:
                                    clauses = clauses[next_clause:]

                    where += parse_clause((clause_map.get('on') or []) + (clause_map.get('where') or []))
                    having = parse_clause(clause_map.get('having') or [])
                    rows = self.select(tables_, cols_, where, having)

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

            start = time.perf_counter()
            code, message = self.run_command(line)
            if code != Error.ROWS:
                message = f'[{Error(code).name}] {message}\n'
                io_write.write(message)
                io_write.flush()
            else:
                io_write.write(f'[{Error(code).name}]\n')
                io_write.flush()
                count = 0
                for row in message:
                    io_write.write(f'{row}\n')
                    io_write.flush()
                    count += 1
                io_write.write(f'--- {count} rows {time.perf_counter() - start} sec\n')
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
                print('> executing from stdin')
                server.run(stdin, stdout, stderr)
            else:
                print(f'> executing {path}')
                input = open(path, 'r')
                server.run(input, stdout, stderr)
                input.close()
    else:
        server.listen()
