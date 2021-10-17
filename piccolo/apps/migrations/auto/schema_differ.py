from __future__ import annotations

import ast
import typing as t
from copy import deepcopy
from dataclasses import dataclass, field

from piccolo.apps.migrations.auto.diffable_table import (
    DiffableTable,
    TableDelta,
)
from piccolo.apps.migrations.auto.operations import RenameColumn, RenameTable
from piccolo.apps.migrations.auto.serialisation import Import, serialise_params
from piccolo.utils.printing import get_fixed_length_string


@dataclass
class RenameTableCollection:
    rename_tables: t.List[RenameTable] = field(default_factory=list)

    def append(self, renamed_table: RenameTable):
        self.rename_tables.append(renamed_table)

    @property
    def old_class_names(self):
        return [i.old_class_name for i in self.rename_tables]

    @property
    def new_class_names(self):
        return [i.new_class_name for i in self.rename_tables]

    def renamed_from(self, new_class_name: str) -> t.Optional[str]:
        """
        Returns the old class name, if it exists.
        """
        rename = [
            i for i in self.rename_tables if i.new_class_name == new_class_name
        ]
        if len(rename) > 0:
            return rename[0].old_class_name
        else:
            return None


@dataclass
class RenameColumnCollection:
    rename_columns: t.List[RenameColumn] = field(default_factory=list)

    def append(self, rename_column: RenameColumn):
        self.rename_columns.append(rename_column)

    def for_table_class_name(
        self, table_class_name: str
    ) -> t.List[RenameColumn]:
        return [
            i
            for i in self.rename_columns
            if i.table_class_name == table_class_name
        ]

    @property
    def old_column_names(self):
        return [i.old_column_name for i in self.rename_columns]

    @property
    def new_column_names(self):
        return [i.new_column_name for i in self.rename_columns]


@dataclass
class AlterStatements:
    statements: t.List[str]
    extra_imports: t.List[Import] = field(default_factory=list)
    extra_definitions: t.List[str] = field(default_factory=list)


@dataclass
class SchemaDiffer:
    """
    Compares two lists of DiffableTables, and returns the list of alter
    statements required to make them match. Asks for user input when it isn't
    sure - for example, whether a column was renamed.
    """

    schema: t.List[DiffableTable]
    schema_snapshot: t.List[DiffableTable]

    # Sometimes the SchemaDiffer requires input from a user - for example,
    # asking if a table was renamed or not. When running in non-interactive
    # mode (like in a unittest), we can set a default to be used instead, like
    # 'y'.
    auto_input: t.Optional[str] = None

    ###########################################################################

    def __post_init__(self):
        self.schema_snapshot_map: t.Dict[str, DiffableTable] = {
            i.class_name: i for i in self.schema_snapshot
        }
        self.rename_tables_collection = self.check_rename_tables()
        self.rename_columns_collection = self.check_renamed_columns()

    def check_rename_tables(self) -> RenameTableCollection:
        """
        Work out whether any of the tables were renamed.
        """
        drop_tables: t.List[DiffableTable] = list(
            set(self.schema_snapshot) - set(self.schema)
        )

        new_tables: t.List[DiffableTable] = list(
            set(self.schema) - set(self.schema_snapshot)
        )

        # A mapping of the old table name (i.e. dropped table) to the new
        # table name.
        collection = RenameTableCollection()

        if len(drop_tables) == 0 or len(new_tables) == 0:
            # There needs to be at least one dropped table and one created
            # table for a rename to make sense.
            return collection

        # A renamed table should have at least one column remaining with the
        # same name.
        for new_table in new_tables:
            new_column_names = [
                i._meta.db_column_name for i in new_table.columns
            ]
            for drop_table in drop_tables:
                drop_column_names = [
                    i._meta.db_column_name for i in new_table.columns
                ]
                same_column_names = set(new_column_names).intersection(
                    drop_column_names
                )
                if len(same_column_names) > 0:
                    if (
                        drop_table.class_name == new_table.class_name
                        and drop_table.tablename != new_table.tablename
                    ):
                        # The class names are the same, but the tablename
                        # has changed - we can assume this is a deliberate
                        # rename.
                        collection.append(
                            RenameTable(
                                old_class_name=drop_table.class_name,
                                old_tablename=drop_table.tablename,
                                new_class_name=new_table.class_name,
                                new_tablename=new_table.tablename,
                            )
                        )
                        continue

                    user_response = (
                        self.auto_input
                        if self.auto_input
                        else input(
                            f"Did you rename {drop_table.class_name} "
                            f"(tablename: {drop_table.tablename}) to "
                            f"{new_table.class_name} "
                            f"(tablename: {new_table.tablename})? (y/N)"
                        )
                    )
                    if user_response.lower() == "y":
                        collection.append(
                            RenameTable(
                                old_class_name=drop_table.class_name,
                                old_tablename=drop_table.tablename,
                                new_class_name=new_table.class_name,
                                new_tablename=new_table.tablename,
                            )
                        )

        return collection

    def check_renamed_columns(self) -> RenameColumnCollection:
        """
        Work out whether any of the columns were renamed.
        """
        collection = RenameColumnCollection()

        for table in self.schema:
            snapshot_table = self.schema_snapshot_map.get(
                table.class_name, None
            )
            if not snapshot_table:
                continue
            delta: TableDelta = table - snapshot_table

            if (not delta.add_columns) and (not delta.drop_columns):
                continue

            # Detecting renamed columns is really tricky.
            # Even if a rename is detected, the column could also have changed
            # type. For now, each time a column is added and removed from a
            # table, ask if it's a rename.

            renamed_column_names: t.List[str] = []

            for add_column in delta.add_columns:
                if add_column.table_class_name in renamed_column_names:
                    continue

                for drop_column in delta.drop_columns:
                    user_response = (
                        self.auto_input
                        if self.auto_input
                        else input(
                            f"Did you rename the `{drop_column.db_column_name}` "  # noqa: E501
                            f"column to `{add_column.db_column_name}` on the "
                            f"`{ add_column.table_class_name }` table? (y/N)"
                        )
                    )
                    if user_response.lower() == "y":
                        renamed_column_names.append(
                            add_column.table_class_name
                        )
                        collection.append(
                            RenameColumn(
                                table_class_name=add_column.table_class_name,
                                tablename=drop_column.tablename,
                                old_column_name=drop_column.column_name,
                                new_column_name=add_column.column_name,
                                old_db_column_name=drop_column.db_column_name,
                                new_db_column_name=add_column.db_column_name,
                            )
                        )

        return collection

    ###########################################################################

    @property
    def create_tables(self) -> AlterStatements:
        new_tables: t.List[DiffableTable] = list(
            set(self.schema) - set(self.schema_snapshot)
        )

        # Remove any which are renames
        new_tables = [
            i
            for i in new_tables
            if i.class_name
            not in self.rename_tables_collection.new_class_names
        ]

        return AlterStatements(
            statements=[
                f"manager.add_table('{i.class_name}', tablename='{i.tablename}')"  # noqa: E501
                for i in new_tables
            ]
        )

    @property
    def drop_tables(self) -> AlterStatements:
        drop_tables: t.List[DiffableTable] = list(
            set(self.schema_snapshot) - set(self.schema)
        )

        # Remove any which are renames
        drop_tables = [
            i
            for i in drop_tables
            if i.class_name
            not in self.rename_tables_collection.old_class_names
        ]

        return AlterStatements(
            statements=[
                f"manager.drop_table(class_name='{i.class_name}', tablename='{i.tablename}')"  # noqa: E501
                for i in drop_tables
            ]
        )

    @property
    def rename_tables(self) -> AlterStatements:
        return AlterStatements(
            statements=[
                f"manager.rename_table(old_class_name='{renamed_table.old_class_name}', old_tablename='{renamed_table.old_tablename}', new_class_name='{renamed_table.new_class_name}', new_tablename='{renamed_table.new_tablename}')"  # noqa
                for renamed_table in self.rename_tables_collection.rename_tables  # noqa: E501
            ]
        )

    ###########################################################################

    def _get_snapshot_table(
        self, table_class_name: str
    ) -> t.Optional[DiffableTable]:
        snapshot_table = self.schema_snapshot_map.get(table_class_name, None)
        if snapshot_table:
            return snapshot_table
        else:
            if (
                table_class_name
                in self.rename_tables_collection.new_class_names
            ):
                class_name = self.rename_tables_collection.renamed_from(
                    table_class_name
                )
                snapshot_table = self.schema_snapshot_map.get(class_name)
                if snapshot_table:
                    snapshot_table.class_name = table_class_name
                    return snapshot_table
        return None

    @property
    def alter_columns(self) -> AlterStatements:
        response: t.List[str] = []
        extra_imports: t.List[Import] = []
        extra_definitions: t.List[str] = []
        for table in self.schema:
            snapshot_table = self._get_snapshot_table(table.class_name)
            if snapshot_table:
                delta: TableDelta = table - snapshot_table
            else:
                continue

            for alter_column in delta.alter_columns:
                new_params = serialise_params(alter_column.params)
                extra_imports.extend(new_params.extra_imports)
                extra_definitions.extend(new_params.extra_definitions)

                old_params = serialise_params(alter_column.old_params)
                extra_imports.extend(old_params.extra_imports)
                extra_definitions.extend(old_params.extra_definitions)

                column_class = (
                    alter_column.column_class.__name__
                    if alter_column.column_class
                    else "None"
                )

                old_column_class = (
                    alter_column.old_column_class.__name__
                    if alter_column.old_column_class
                    else "None"
                )

                if alter_column.column_class is not None:
                    extra_imports.append(
                        Import(
                            module=alter_column.column_class.__module__,
                            target=alter_column.column_class.__name__,
                        )
                    )

                if alter_column.old_column_class is not None:
                    extra_imports.append(
                        Import(
                            module=alter_column.old_column_class.__module__,
                            target=alter_column.old_column_class.__name__,
                        )
                    )

                response.append(
                    f"manager.alter_column(table_class_name='{table.class_name}', tablename='{table.tablename}', column_name='{alter_column.column_name}', params={new_params.params}, old_params={old_params.params}, column_class={column_class}, old_column_class={old_column_class})"  # noqa: E501
                )

        return AlterStatements(
            statements=response,
            extra_imports=extra_imports,
            extra_definitions=extra_definitions,
        )

    @property
    def drop_columns(self) -> AlterStatements:
        response = []
        for table in self.schema:
            snapshot_table = self._get_snapshot_table(table.class_name)
            if snapshot_table:
                delta: TableDelta = table - snapshot_table
            else:
                continue

            for column in delta.drop_columns:
                if (
                    column.column_name
                    in self.rename_columns_collection.old_column_names
                ):
                    continue

                response.append(
                    f"manager.drop_column(table_class_name='{table.class_name}', tablename='{table.tablename}', column_name='{column.column_name}', db_column_name='{column.db_column_name}')"  # noqa: E501
                )
        return AlterStatements(statements=response)

    @property
    def add_columns(self) -> AlterStatements:
        response: t.List[str] = []
        extra_imports: t.List[Import] = []
        extra_definitions: t.List[str] = []
        for table in self.schema:
            snapshot_table = self._get_snapshot_table(table.class_name)
            if snapshot_table:
                delta: TableDelta = table - snapshot_table
            else:
                continue

            for add_column in delta.add_columns:
                if (
                    add_column.column_name
                    in self.rename_columns_collection.new_column_names
                ):
                    continue

                params = serialise_params(add_column.params)
                cleaned_params = params.params
                extra_imports.extend(params.extra_imports)
                extra_definitions.extend(params.extra_definitions)

                column_class = add_column.column_class
                extra_imports.append(
                    Import(
                        module=column_class.__module__,
                        target=column_class.__name__,
                    )
                )

                response.append(
                    f"manager.add_column(table_class_name='{table.class_name}', tablename='{table.tablename}', column_name='{add_column.column_name}', db_column_name='{add_column.db_column_name}', column_class_name='{add_column.column_class_name}', column_class={column_class.__name__}, params={str(cleaned_params)})"  # noqa: E501
                )
        return AlterStatements(
            statements=response,
            extra_imports=extra_imports,
            extra_definitions=extra_definitions,
        )

    @property
    def rename_columns(self) -> AlterStatements:
        return AlterStatements(
            statements=[
                f"manager.rename_column(table_class_name='{i.table_class_name}', tablename='{i.tablename}', old_column_name='{i.old_column_name}', new_column_name='{i.new_column_name}', old_db_column_name='{i.old_db_column_name}', new_db_column_name='{i.new_db_column_name}')"  # noqa: E501
                for i in self.rename_columns_collection.rename_columns
            ]
        )

    ###########################################################################

    @property
    def new_table_columns(self) -> AlterStatements:
        new_tables: t.List[DiffableTable] = list(
            set(self.schema) - set(self.schema_snapshot)
        )

        response: t.List[str] = []
        extra_imports: t.List[Import] = []
        extra_definitions: t.List[str] = []
        for table in new_tables:
            if (
                table.class_name
                in self.rename_tables_collection.new_class_names
            ):
                continue

            for column in table.columns:
                # In case we cause subtle bugs:
                params = deepcopy(column._meta.params)
                _params = serialise_params(params)
                cleaned_params = _params.params
                extra_imports.extend(_params.extra_imports)
                extra_definitions.extend(_params.extra_definitions)

                extra_imports.append(
                    Import(
                        module=column.__class__.__module__,
                        target=column.__class__.__name__,
                    )
                )

                response.append(
                    f"manager.add_column(table_class_name='{table.class_name}', tablename='{table.tablename}', column_name='{column._meta.name}', db_column_name='{column._meta.db_column_name}', column_class_name='{column.__class__.__name__}', column_class={column.__class__.__name__}, params={str(cleaned_params)})"  # noqa: E501
                )
        return AlterStatements(
            statements=response,
            extra_imports=extra_imports,
            extra_definitions=extra_definitions,
        )

    ###########################################################################

    def get_alter_statements(self) -> t.List[AlterStatements]:
        """
        Call to execute the necessary alter commands on the database.
        """
        alter_statements: t.Dict[str, AlterStatements] = {
            "Created tables": self.create_tables,
            "Dropped tables": self.drop_tables,
            "Renamed tables": self.rename_tables,
            "Created table columns": self.new_table_columns,
            "Dropped columns": self.drop_columns,
            "Columns added to existing tables": self.add_columns,
            "Renamed columns": self.rename_columns,
            "Altered columns": self.alter_columns,
        }

        for message, statements in alter_statements.items():
            _message = get_fixed_length_string(message, length=40)
            count = len(statements.statements)
            print(f"{_message} {count}")

        alter_statements_lst = [i for i in alter_statements.values()]

        self._raise_if_alter_statements_have_conflicting_imports(
            alter_statements_lst
        )

        return alter_statements_lst

    @classmethod
    def _raise_if_alter_statements_have_conflicting_imports(
        cls, alter_statements: t.List[AlterStatements]
    ) -> None:
        conflicting_imports = cls._get_conflicting_imports(
            alter_statements, ignore_duplicates=True
        )
        if len(conflicting_imports) != 0:
            raise ConflictingExtraImportsError(
                conflicting_imports=conflicting_imports,
                message_template=(
                    "Alter statements contain conflicting extra imports:\n"
                    "{humanized_conflicting_imports}"
                ),
            )

    @staticmethod
    def _get_conflicting_imports(
        alter_statements: t.List[AlterStatements],
        ignore_duplicates: bool = False,
    ) -> t.List[t.List[str]]:
        import_node_accumulator = _AstImportNodeAccumulator()
        for alter_statement in alter_statements:
            for extra_import in alter_statement.extra_imports:
                import_node_accumulator.add_import_str(repr(extra_import))

        import_name_to_nodes_mapping: t.Dict[
            str, t.List[t.Union[ast.Import, ast.ImportFrom]]
        ] = {}
        for import_node in import_node_accumulator.import_nodes:
            for alias in import_node.names:
                import_name = (
                    alias.name if alias.asname is None else alias.asname
                )

                if import_name in import_name_to_nodes_mapping:
                    import_name_to_nodes_mapping[import_name].append(
                        import_node
                    )
                else:
                    import_name_to_nodes_mapping[import_name] = [import_node]

        conflicting_imports = []
        for import_name, import_nodes in import_name_to_nodes_mapping.items():
            if len(import_nodes) >= 2:
                if ignore_duplicates and all(
                    _are_equal_ast_import_nodes(import_nodes[0], import_node)
                    for import_node in import_nodes[1:]
                ):
                    continue

                conflicting_import_group = [
                    _unparse_ast_import_node(import_node)
                    for import_node in import_nodes
                ]
                conflicting_imports.append(conflicting_import_group)

        return conflicting_imports


class _AstImportNodeAccumulator(ast.NodeVisitor):
    def __init__(self):
        self.import_nodes: t.List[t.Union[ast.Import, ast.ImportFrom]] = []
        super().__init__()

    def add_import_str(self, import_str: str):
        self.visit(ast.parse(import_str))

    def visit_Import(self, node: ast.Import) -> None:
        self.import_nodes.append(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.import_nodes.append(node)


def _are_equal_ast_import_nodes(
    import_node1: t.Union[ast.Import, ast.ImportFrom],
    import_node2: t.Union[ast.Import, ast.ImportFrom],
) -> bool:
    if isinstance(import_node1, ast.Import) and not isinstance(
        import_node2, ast.Import
    ):
        return False

    elif isinstance(import_node1, ast.ImportFrom):
        if not isinstance(import_node2, ast.ImportFrom):
            return False

        if (
            import_node1.module != import_node2.module
            or import_node1.level != import_node2.level
        ):
            return False

    if len(import_node1.names) != len(import_node2.names):
        return False

    if any(
        alias1.name != alias2.name or alias1.asname != alias2.asname
        for alias1, alias2 in zip(import_node1.names, import_node2.names)
    ):
        return False

    return True


def _unparse_ast_import_node(
    import_node: t.Union[ast.Import, ast.ImportFrom]
) -> str:
    aliases_str = ", ".join(
        alias.name if alias.asname is None else alias.asname
        for alias in import_node.names
    )
    if isinstance(import_node, ast.Import):
        return f"import {aliases_str}"
    elif isinstance(import_node, ast.ImportFrom):
        dots = "." * import_node.level
        module = getattr(import_node, "module", "")
        return f"from {dots}{module} " f"import {aliases_str}"
    else:
        raise ValueError(
            "import_node must be an instance of ast.Import or ast.ImportFrom"
        )


class AlterStatementsError(ValueError):
    pass


class ConflictingExtraImportsError(AlterStatementsError):
    def __init__(
        self,
        conflicting_imports: t.List[t.List[str]],
        message_template: str = (
            "Conflicting extra imports:\n{humanized_conflicting_imports}"
        ),
    ):
        error_message = message_template.format(
            humanized_conflicting_imports=self.humanize_conflicting_imports(
                conflicting_imports
            )
        )
        super().__init__(error_message)

    @classmethod
    def humanize_conflicting_imports(
        cls, conflicting_imports: t.List[t.List[str]], indent: str = "    "
    ) -> str:
        separator = f"{indent}- "
        return separator + separator.join(
            cls.humanize_conflicting_import_group(import_group)
            for import_group in conflicting_imports
        )

    @staticmethod
    def humanize_conflicting_import_group(
        conflicting_import_group: t.List[str],
    ) -> str:
        quoted_imports = [
            f'"{import_}"' for import_ in conflicting_import_group
        ]
        return f"{', '.join(quoted_imports[:-1])} and {quoted_imports[-1]}"
