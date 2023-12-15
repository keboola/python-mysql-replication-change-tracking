import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

import sqlparse
from sqlparse.sql import Identifier, Statement, Token, IdentifierList, TokenList, Parenthesis
from sqlparse.tokens import Whitespace, Name

TABLE_NAME_INDEX = 4

FIRST_KEYWORD_INDEX = 8


class TableChangeType(Enum):
    DROP_COLUMN = 'DROP_COLUMN'
    ADD_COLUMN = 'ADD_COLUMN'


@dataclass
class TableSchemaChange:
    type: TableChangeType
    table_name: str
    schema: Optional[str]
    column_name: str
    after_column: str = None
    first_position: bool = False
    data_type: str = None
    collation: str = None
    column_key: str = None
    charset_name: str = None
    query: str = ''


class AlterStatementParser:
    """
    Parse ALTER statements.

    - Case sensitive.
    - Expects valid queries.
    - Multi statements are not supported. Apart from multi statement including USE {schema}; at the beginning.
    """
    # Supported statements - match patterns based on position
    SUPPORTED_ALTER_TABLE_STATEMENTS = ['ALTER TABLE {table_name} DROP COLUMN {col_name}',
                                        'ALTER TABLE {table_name} DROP {col_name}',
                                        'ALTER TABLE {table_name} ADD COLUMN {col_name}',
                                        'ALTER TABLE {table_name} ADD {col_name}']

    # minimal size of a query (ALTER TABLE xx XXX SOMETHING)
    MINIMAL_TOKEN_COUNT = 9

    def _is_column_name(self, statement: Statement, idx: int):
        next_idx, next_token = self._get_element_next_to_position(statement, idx)
        if statement.tokens[idx].normalized.upper() == 'FOREIGN' and next_token.upper() == 'KEY':
            return False
        if statement.tokens[idx].normalized.upper() == 'CONSTRAINT':
            return False
        if statement.tokens[idx].normalized.upper() == 'UNIQUE':
            return False
        if statement.tokens[idx].normalized == 'INDEX':
            return False

        return True

    def _is_matching_pattern(self, statement: Statement, pattern: str):
        match = True
        for idx, value in enumerate(re.split(r'(\s+)', pattern)):
            if value.startswith('{'):
                if value == '{col_name}' and not self._is_column_name(statement, idx):
                    match = False
                continue
            if statement.tokens[idx].normalized != value:
                match = False
                break
        return match

    def _is_supported_alter_table_statement(self, normalized_statement):
        token_count = len(normalized_statement.tokens)
        if not (normalized_statement.get_type() == 'ALTER' and
                token_count >= self.MINIMAL_TOKEN_COUNT and
                normalized_statement.tokens[2].value.upper() == 'TABLE'):
            return False
        else:
            for pattern in self.SUPPORTED_ALTER_TABLE_STATEMENTS:
                match = self._is_matching_pattern(statement=normalized_statement, pattern=pattern)
                if match:
                    return True

            return False

    @staticmethod
    def __is_column_identifier(token: Token) -> bool:
        parent_is_name = (token.ttype == sqlparse.tokens.Name and isinstance(token.parent, Identifier))
        return isinstance(token, Identifier) or parent_is_name

    @staticmethod
    def _split_parenthesis(statement, token, position):
        ddl_type = statement.token_prev(position, skip_cm=True)
        new_tokens = []
        token_flat = []
        for t in list(token)[1:-1]:
            token_flat.extend(t.flatten()) if isinstance(t, IdentifierList) else token_flat.append(t)
        for t in token_flat:

            if t.ttype == sqlparse.tokens.Punctuation and t.normalized == ',':
                new_tokens.append(t)
                new_tokens.append(Token(Whitespace, ' '))
                new_tokens.append(ddl_type[1])
                new_tokens.append(Token(Whitespace, ' '))
            else:
                new_tokens.append(t)
        return new_tokens

    def __ungroup_identifier_lists(self, statement: Statement):
        """
        Dirty fix of a sqlparser bug that falsely groups statements like (FIRST, ADD) in
        ADD COLUMN email VARCHAR(100) NOT NULL FIRST, ADD
        """
        tokens = []
        for idx, t in enumerate(statement):

            if isinstance(t, IdentifierList):
                tokens.extend(t.flatten())
            elif isinstance(t, Parenthesis):
                tokens.extend(self._split_parenthesis(statement, t, idx))
            # exclude schema.table id
            elif isinstance(t, Identifier) and len(t.tokens) > 1 and t.tokens[1].normalized != '.':
                tokens.extend(t.tokens)
            else:
                tokens.append(t)
        return TokenList(tokens)

    @staticmethod
    def _is_column_keyword(statement: Token):
        return statement.ttype == sqlparse.tokens.Keyword and statement.normalized == 'COLUMN'

    def _normalize_identifier(self, identifier: str):
        """
        Remove quotes
        """
        return identifier.replace('`', '')

    def _get_table_name(self, statement: Statement):
        schema = ''
        table_name = statement.tokens[TABLE_NAME_INDEX].value
        split = table_name.split('.')
        if len(split) > 1:
            table_name = split[1]
            schema = split[0]
        return self._normalize_identifier(schema), self._normalize_identifier(table_name)

    @staticmethod
    def _normalize_token_name(token: Token):
        value = token.normalized if token else ''
        if value and token.ttype == Name and value.startswith('`') and value.endswith('`'):
            value = value[1:][:-1]

        return value

    def _get_element_next_to_position(self, statement: TokenList, position):
        index, value = statement.token_next(position, skip_cm=True)
        next_value = self._normalize_token_name(value)

        return index, next_value

    def _process_drop_event(self, table_name, schema,
                            statement: Statement, original_statement_sql) -> List[TableSchemaChange]:
        schema_changes = []

        token_count = len(statement.tokens)
        first_keyword_index = FIRST_KEYWORD_INDEX
        for idx, value in enumerate(statement.tokens[FIRST_KEYWORD_INDEX:], start=FIRST_KEYWORD_INDEX):
            if (idx != token_count - 1) and value.ttype in [sqlparse.tokens.Whitespace, sqlparse.tokens.Newline]:
                # skip empty chars if not end
                continue
                # capture new col name
            elif idx == first_keyword_index:
                next_index = idx
                if self._is_column_keyword(value):
                    next_index, column_name = self._get_element_next_to_position(statement, idx)
                else:
                    column_name = value.normalized
                schema_changes.append(TableSchemaChange(TableChangeType.DROP_COLUMN, table_name, schema,
                                                        self._normalize_identifier(column_name),
                                                        query=original_statement_sql))

            elif value.ttype == sqlparse.tokens.Punctuation and value.normalized == ',':
                # next is always another DROP, if not it may algorithm, lock, etc, so quit
                add_keyword_index, element = self._get_element_next_to_position(statement, idx)
                if element.upper() != 'DROP':
                    continue
                first_keyword_index, element = self._get_element_next_to_position(statement, add_keyword_index)

        return schema_changes

    def _process_add_event(self, table_name, schema,
                           statement: Statement, original_statement_sql) -> List[TableSchemaChange]:
        schema_changes = []
        schema_change = None
        token_count = len(statement.tokens)

        # index of first keyword after ADD statement
        first_keyword_index = FIRST_KEYWORD_INDEX
        for idx, value in enumerate(statement.tokens[FIRST_KEYWORD_INDEX:], start=FIRST_KEYWORD_INDEX):

            if (idx != token_count - 1) and value.ttype in [sqlparse.tokens.Whitespace, sqlparse.tokens.Newline]:
                # skip empty chars if not end
                continue
            # capture new col name
            elif idx == first_keyword_index:
                next_index = idx
                if self._is_column_keyword(value):
                    next_index, column_name = self._get_element_next_to_position(statement, idx)
                else:
                    column_name = value.normalized
                # next one is always datatype
                next_index, data_type = self._get_element_next_to_position(statement, next_index)
                schema_change = TableSchemaChange(TableChangeType.ADD_COLUMN, table_name, schema,
                                                  self._normalize_identifier(column_name),
                                                  data_type=data_type.upper(),
                                                  query=original_statement_sql)

            # AFTER statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'AFTER':
                # next one is always column name
                next_index, schema_change.after_column = self._get_element_next_to_position(statement, idx)

            # CHARACTER SET statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'CHARACTER':
                # next should be SET statement
                next_index, next_element = self._get_element_next_to_position(statement, idx)
                if 'SET' == next_element:
                    schema_change.charset_name = self._get_element_next_to_position(statement, next_index)[1]

            # COLLATE  statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'COLLATE':
                # next one is always column name
                next_index, schema_change.collation = self._get_element_next_to_position(statement, idx)

            # PRIMARY KEY  statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'PRIMARY' \
                    and self._get_element_next_to_position(statement, idx)[1] == 'KEY':
                # next one is always column name
                schema_change.column_key = 'PRI'

            # process FIRST statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'FIRST':
                schema_change.first_position = True

            # is at the end of multiline statement or end of the query
            elif value.ttype == sqlparse.tokens.Punctuation and value.normalized == ',':
                # next is always another ADD, if not it may algorithm, lock, etc, so quit
                add_keyword_index, element = self._get_element_next_to_position(statement, idx)
                if element.upper() != 'ADD':
                    continue
                first_keyword_index, element = self._get_element_next_to_position(statement, add_keyword_index)
                schema_changes.append(schema_change)

            # save schema change on end, should never be empty
            if idx == token_count - 1:
                if schema_change is None:
                    raise RuntimeError(f"Invalid ALTER statement query: {statement.normalized}")
                schema_changes.append(schema_change)

        return schema_changes

    def _get_schema_from_use_statement(self, statement: Statement):
        schema = self._get_element_next_to_position(statement, 0)[1]
        return self._normalize_identifier(schema)

    def _extract_alter_statement_and_schema(self, normalized_statements: Statement):
        use_schema = ''
        normalized_statement = ''
        for statement in normalized_statements:
            first_token = statement.token_first(skip_cm=True)
            if first_token.normalized == 'ALTER':
                normalized_statement = statement
            elif first_token.normalized == 'USE':
                use_schema = self._get_schema_from_use_statement(statement)
        return use_schema, normalized_statement

    def _split_drop_add(self, flattened_tokens, change_type) -> Tuple[Statement, Statement]:
        change_tokens = {'ADD': [], 'DROP': []}
        alter_tokens = flattened_tokens.tokens[:6]
        last_index = len(flattened_tokens.tokens) - 1
        skip_indexes = [-1]
        type_change = False
        for idx, value in enumerate(flattened_tokens.tokens[6:], start=6):
            if idx in skip_indexes:
                continue

            if value.ttype == sqlparse.tokens.Punctuation and value.normalized == ',':
                next_index, element = self._get_element_next_to_position(flattened_tokens, idx)
                if element.upper() in ['DROP', 'ADD']:
                    type_change = change_type != element.upper()
                    change_type = element.upper()
                    skip_indexes = list(range(idx, next_index))

                if type_change or idx == last_index:
                    # reset
                    type_change = False
                    continue

            change_tokens[change_type].append(value)

        add_statement = Statement(alter_tokens + change_tokens['ADD']) if change_tokens['ADD'] else Statement([])
        drop_statement = Statement(alter_tokens + change_tokens['DROP']) if change_tokens['DROP'] else Statement([])
        return add_statement, drop_statement

    def _process_add_drop_changes(self, table_name, schema_name, normalized_statement):
        first_type = normalized_statement.tokens[6].normalized
        table_changes = []
        # because some statements including FIRST were invalidly parsed as identifier groups
        # happens when tokens of type Keyword are separated by comma
        flattened_tokens = self.__ungroup_identifier_lists(normalized_statement)
        add_statement, drop_statement = self._split_drop_add(flattened_tokens,
                                                             change_type=first_type)

        table_changes.extend(self._process_drop_event(table_name,
                                                      schema_name,
                                                      drop_statement,
                                                      normalized_statement.normalized))

        table_changes.extend(self._process_add_event(table_name,
                                                     schema_name,
                                                     add_statement,
                                                     normalized_statement.normalized))
        return table_changes

    def get_table_changes(self, sql: str, schema: str) -> List[TableSchemaChange]:
        normalized_statements = sqlparse.parse(sqlparse.format(sql, strip_comments=True, reindent_aligned=True,
                                                               strip_whitespace=True).replace('\n', ' '))
        use_schema, normalized_statement = self._extract_alter_statement_and_schema(normalized_statements)

        # normalized / formatted by now, should be safe to use fixed index
        if not normalized_statement or not self._is_supported_alter_table_statement(normalized_statement):
            return []

        query_schema, table_name = self._get_table_name(normalized_statement)

        schema_name = schema or query_schema or use_schema

        return self._process_add_drop_changes(table_name, schema_name, normalized_statement)
