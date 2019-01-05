from collections import OrderedDict
from itertools import groupby
import typing as t

from ..base import Query
from piccolo.columns import Column, ForeignKey
from ..mixins import (
    ColumnsMixin, CountMixin, DistinctMixin, LimitMixin, OrderByMixin,
    OutputMixin, WhereMixin
)
if t.TYPE_CHECKING:
    from table import Table  # noqa


class Select(
    Query,
    ColumnsMixin,
    CountMixin,
    DistinctMixin,
    LimitMixin,
    OrderByMixin,
    OutputMixin,
    WhereMixin,
):

    def get_joins(self, columns: t.List[Column]):
        """
        A call chain is a sequence of foreign keys representing joins which
        need to be made to retrieve a column in another table.
        """
        joins: t.List[str] = []
        for column in columns:
            _joins: t.List[str] = []
            for index, key in enumerate(column.call_chain, 1):

                keys = column.call_chain[:index]

                tablename = key.references.Meta.tablename
                key_name = f'{key._name}'

                # Fix the table alias ... then work out join_right ...
                # Print it out to see how close we are ...
                table_alias = (
                    '__'.join([
                        f'{key._table.Meta.tablename}__'
                        f'${key._name}' for i in keys
                    ]) + f'__{keys[-1].references.Meta.tablename}'
                )
                join_left = f'{table_alias}.{key_name}'
                join_right = f'{key_name}.id'

                _joins.append(
                    f'JOIN {tablename} {table_alias} '
                    f'ON {join_left} = {join_right}'
                )

            joins.extend(_joins)
            column.prefix = table_alias

        # loses the order here ...
        return list(OrderedDict.fromkeys(joins))

    def check_valid_call_chain(self, keys: t.List[ForeignKey]):
        for column in keys:
            if column.call_chain:
                # Make sure the call_chain isn't too large

                if len(column.call_chain) > 10:
                    raise Exception(
                        "Joining more than 10 tables isn't supported - "
                        "please restructure your query."
                    )

    def __str__(self):
        joins = []

        if len(self.selected_columns) == 0:
            columns_str = '*'
        else:
            ###################################################################
            # JOIN

            # keys = set()

            self.check_valid_call_chain(self.selected_columns)

            joins = self.get_joins(self.selected_columns)

            # # Group the foreign keys by tablename
            # keys = list(keys)
            # # Groups consecutive items with the same tablename
            # grouped_keys = groupby(
            #     sorted(
            #         keys,
            #         key=lambda k: k.references.Meta.tablename
            #     ),
            #     key=lambda k: k.references.Meta.tablename
            # )

            # for tablename, table_keys in grouped_keys:
            #     table_keys = [i for i in table_keys]
            #     for index, key in enumerate(table_keys):
            #         _index = index + 1
            #         key.index = _index
            #         # Double underscore is just to prevent the likelihood of a
            #         # name clash.
            #         alias = f'{tablename}__{_index}'
            #         key.alias = alias
            #         joins.append(
            #             f' JOIN {tablename} {alias} ON {key._name} = {alias}.id'
            #         )

            ###################################################################

            column_names = []
            for column in self.selected_columns:
                column_name = column._name

                if not column.call_chain:
                    column_names.append(
                        f'{self.table.Meta.tablename}.{column_name}'
                    )
                    continue

                column_name = '$'.join([
                    i._name for i in column.call_chain
                ]) + f'${column_name}'

                alias = f'{column.prefix}.{column._name}'
                column_names.append(
                    f'{alias} AS "{column_name}"'
                )

            columns_str = ', '.join(column_names)

        #######################################################################

        select = 'SELECT DISTINCT' if self.distinct else 'SELECT'
        query = f'{select} {columns_str} FROM "{self.table.Meta.tablename}"'

        for join in joins:
            query += f' {join}'

        #######################################################################

        if self._where:
            query += f' WHERE {self._where.__str__()}'

        if self._order_by:
            query += self._order_by.__str__()

        if self._limit:
            query += self._limit.__str__()

        if self._count:
            query = f'SELECT COUNT(*) FROM ({query}) AS sub_query'

        return query
