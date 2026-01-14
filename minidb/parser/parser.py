"""
SQL Parser - Converts tokens into an Abstract Syntax Tree (AST)

Uses recursive descent parsing to build a tree structure
that represents SQL statements.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Any, Union
from enum import Enum, auto

from .lexer import Lexer, Token, TokenType


# ============================================================================
# AST Node Types
# ============================================================================

class JoinType(Enum):
    INNER = auto()
    LEFT = auto()
    RIGHT = auto()
    CROSS = auto()


class OrderDirection(Enum):
    ASC = auto()
    DESC = auto()


@dataclass
class ColumnDef:
    """Column definition for CREATE TABLE"""
    name: str
    data_type: str
    primary_key: bool = False
    unique: bool = False
    not_null: bool = False
    default: Any = None


@dataclass
class ColumnRef:
    """Reference to a column, optionally with table alias"""
    column: str
    table: Optional[str] = None
    alias: Optional[str] = None


@dataclass
class Literal:
    """A literal value"""
    value: Any


@dataclass
class BinaryOp:
    """Binary operation (e.g., a = b, a AND b)"""
    left: Any
    operator: str
    right: Any


@dataclass
class UnaryOp:
    """Unary operation (e.g., NOT x, -5)"""
    operator: str
    operand: Any


@dataclass
class FunctionCall:
    """Aggregate or scalar function call"""
    name: str
    args: List[Any]
    distinct: bool = False


@dataclass
class TableRef:
    """Reference to a table"""
    name: str
    alias: Optional[str] = None


@dataclass 
class JoinClause:
    """JOIN clause"""
    table: TableRef
    join_type: JoinType
    condition: Optional[Any] = None


@dataclass
class OrderByItem:
    """ORDER BY item"""
    expr: Any
    direction: OrderDirection = OrderDirection.ASC


@dataclass
class SelectStatement:
    """SELECT statement"""
    columns: List[Any]  # List of ColumnRef, Literal, FunctionCall, or '*'
    from_table: Optional[TableRef] = None
    joins: List[JoinClause] = field(default_factory=list)
    where: Optional[Any] = None
    group_by: List[Any] = field(default_factory=list)
    having: Optional[Any] = None
    order_by: List[OrderByItem] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    distinct: bool = False


@dataclass
class InsertStatement:
    """INSERT statement"""
    table: str
    columns: Optional[List[str]] = None
    values: List[List[Any]] = field(default_factory=list)


@dataclass
class UpdateStatement:
    """UPDATE statement"""
    table: str
    assignments: List[tuple] = field(default_factory=list)  # [(col, value), ...]
    where: Optional[Any] = None


@dataclass
class DeleteStatement:
    """DELETE statement"""
    table: str
    where: Optional[Any] = None


@dataclass
class CreateTableStatement:
    """CREATE TABLE statement"""
    table: str
    columns: List[ColumnDef] = field(default_factory=list)
    if_not_exists: bool = False


@dataclass
class DropTableStatement:
    """DROP TABLE statement"""
    table: str
    if_exists: bool = False


@dataclass
class CreateIndexStatement:
    """CREATE INDEX statement"""
    name: str
    table: str
    column: str
    unique: bool = False


@dataclass
class DropIndexStatement:
    """DROP INDEX statement"""
    name: str
    table: str


@dataclass
class ShowTablesStatement:
    """SHOW TABLES statement"""
    pass


@dataclass
class DescribeTableStatement:
    """DESCRIBE TABLE statement"""
    table: str


@dataclass
class TruncateTableStatement:
    """TRUNCATE TABLE statement"""
    table: str


# ============================================================================
# Parser
# ============================================================================

class ParseError(Exception):
    """Parser error with position information"""
    def __init__(self, message: str, token: Token):
        self.token = token
        super().__init__(f"{message} at line {token.line}, column {token.column}")


class Parser:
    """
    Recursive descent SQL parser.
    
    Parses SQL statements into AST nodes.
    """
    
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0
    
    def _current(self) -> Token:
        """Get current token"""
        if self.pos >= len(self.tokens):
            return self.tokens[-1]  # EOF
        return self.tokens[self.pos]
    
    def _peek(self, offset: int = 1) -> Token:
        """Peek at token ahead"""
        pos = self.pos + offset
        if pos >= len(self.tokens):
            return self.tokens[-1]
        return self.tokens[pos]
    
    def _advance(self) -> Token:
        """Advance and return current token"""
        token = self._current()
        self.pos += 1
        return token
    
    def _match(self, *types: TokenType) -> bool:
        """Check if current token matches any of the types"""
        return self._current().type in types
    
    def _expect(self, token_type: TokenType, message: str = None) -> Token:
        """Expect a specific token type"""
        if not self._match(token_type):
            msg = message or f"Expected {token_type.name}"
            raise ParseError(msg, self._current())
        return self._advance()
    
    def _consume_if(self, token_type: TokenType) -> bool:
        """Consume token if it matches"""
        if self._match(token_type):
            self._advance()
            return True
        return False
    
    def parse(self) -> Any:
        """Parse a single statement"""
        if self._match(TokenType.SELECT):
            return self._parse_select()
        elif self._match(TokenType.INSERT):
            return self._parse_insert()
        elif self._match(TokenType.UPDATE):
            return self._parse_update()
        elif self._match(TokenType.DELETE):
            return self._parse_delete()
        elif self._match(TokenType.CREATE):
            return self._parse_create()
        elif self._match(TokenType.DROP):
            return self._parse_drop()
        elif self._match(TokenType.SHOW):
            return self._parse_show()
        elif self._match(TokenType.DESCRIBE):
            return self._parse_describe()
        elif self._match(TokenType.TRUNCATE):
            return self._parse_truncate()
        else:
            raise ParseError(f"Unexpected token: {self._current().value}", self._current())
    
    def _parse_select(self) -> SelectStatement:
        """Parse SELECT statement"""
        self._expect(TokenType.SELECT)
        
        stmt = SelectStatement(columns=[])
        
        # DISTINCT
        if self._consume_if(TokenType.DISTINCT):
            stmt.distinct = True
        
        # Column list
        stmt.columns = self._parse_select_columns()
        
        # FROM clause
        if self._consume_if(TokenType.FROM):
            stmt.from_table = self._parse_table_ref()
            
            # JOIN clauses
            while self._match(TokenType.JOIN, TokenType.INNER, TokenType.LEFT, 
                             TokenType.RIGHT, TokenType.CROSS):
                stmt.joins.append(self._parse_join())
        
        # WHERE clause
        if self._consume_if(TokenType.WHERE):
            stmt.where = self._parse_expression()
        
        # GROUP BY clause
        if self._consume_if(TokenType.GROUP):
            self._expect(TokenType.BY)
            stmt.group_by = self._parse_expression_list()
            
            # HAVING clause
            if self._consume_if(TokenType.HAVING):
                stmt.having = self._parse_expression()
        
        # ORDER BY clause
        if self._consume_if(TokenType.ORDER):
            self._expect(TokenType.BY)
            stmt.order_by = self._parse_order_by()
        
        # LIMIT clause
        if self._consume_if(TokenType.LIMIT):
            stmt.limit = self._expect(TokenType.INTEGER).value
            
            # OFFSET clause
            if self._consume_if(TokenType.OFFSET):
                stmt.offset = self._expect(TokenType.INTEGER).value
        
        self._consume_if(TokenType.SEMICOLON)
        
        return stmt
    
    def _parse_select_columns(self) -> List[Any]:
        """Parse SELECT column list"""
        columns = []
        
        while True:
            if self._match(TokenType.STAR):
                self._advance()
                columns.append('*')
            else:
                expr = self._parse_expression()
                
                # Check for alias
                alias = None
                if self._consume_if(TokenType.AS):
                    alias = self._expect(TokenType.IDENTIFIER).value
                elif self._match(TokenType.IDENTIFIER) and not self._match(
                    TokenType.FROM, TokenType.WHERE, TokenType.JOIN, 
                    TokenType.ORDER, TokenType.GROUP, TokenType.LIMIT):
                    # Implicit alias
                    next_token = self._current()
                    if next_token.type == TokenType.IDENTIFIER:
                        alias = self._advance().value
                
                if isinstance(expr, ColumnRef):
                    expr.alias = alias
                    columns.append(expr)
                elif alias:
                    columns.append((expr, alias))
                else:
                    columns.append(expr)
            
            if not self._consume_if(TokenType.COMMA):
                break
        
        return columns
    
    def _parse_table_ref(self) -> TableRef:
        """Parse table reference with optional alias"""
        name = self._expect(TokenType.IDENTIFIER).value
        
        alias = None
        if self._consume_if(TokenType.AS):
            alias = self._expect(TokenType.IDENTIFIER).value
        elif self._match(TokenType.IDENTIFIER):
            # Check it's not a keyword
            if self._current().type == TokenType.IDENTIFIER:
                alias = self._advance().value
        
        return TableRef(name=name, alias=alias)
    
    def _parse_join(self) -> JoinClause:
        """Parse JOIN clause"""
        join_type = JoinType.INNER
        
        if self._consume_if(TokenType.INNER):
            join_type = JoinType.INNER
        elif self._consume_if(TokenType.LEFT):
            self._consume_if(TokenType.OUTER)
            join_type = JoinType.LEFT
        elif self._consume_if(TokenType.RIGHT):
            self._consume_if(TokenType.OUTER)
            join_type = JoinType.RIGHT
        elif self._consume_if(TokenType.CROSS):
            join_type = JoinType.CROSS
        
        self._expect(TokenType.JOIN)
        
        table = self._parse_table_ref()
        
        condition = None
        if self._consume_if(TokenType.ON):
            condition = self._parse_expression()
        
        return JoinClause(table=table, join_type=join_type, condition=condition)
    
    def _parse_order_by(self) -> List[OrderByItem]:
        """Parse ORDER BY clause"""
        items = []
        
        while True:
            expr = self._parse_expression()
            direction = OrderDirection.ASC
            
            if self._consume_if(TokenType.DESC):
                direction = OrderDirection.DESC
            else:
                self._consume_if(TokenType.ASC)
            
            items.append(OrderByItem(expr=expr, direction=direction))
            
            if not self._consume_if(TokenType.COMMA):
                break
        
        return items
    
    def _parse_insert(self) -> InsertStatement:
        """Parse INSERT statement"""
        self._expect(TokenType.INSERT)
        self._expect(TokenType.INTO)
        
        table = self._expect(TokenType.IDENTIFIER).value
        
        # Optional column list
        columns = None
        if self._consume_if(TokenType.LPAREN):
            columns = []
            while True:
                columns.append(self._expect(TokenType.IDENTIFIER).value)
                if not self._consume_if(TokenType.COMMA):
                    break
            self._expect(TokenType.RPAREN)
        
        # VALUES clause
        self._expect(TokenType.VALUES)
        
        values = []
        while True:
            self._expect(TokenType.LPAREN)
            row = []
            while True:
                row.append(self._parse_value())
                if not self._consume_if(TokenType.COMMA):
                    break
            self._expect(TokenType.RPAREN)
            values.append(row)
            
            if not self._consume_if(TokenType.COMMA):
                break
        
        self._consume_if(TokenType.SEMICOLON)
        
        return InsertStatement(table=table, columns=columns, values=values)
    
    def _parse_value(self) -> Any:
        """Parse a literal value"""
        if self._match(TokenType.INTEGER):
            return Literal(self._advance().value)
        elif self._match(TokenType.FLOAT):
            return Literal(self._advance().value)
        elif self._match(TokenType.STRING):
            return Literal(self._advance().value)
        elif self._match(TokenType.TRUE):
            self._advance()
            return Literal(True)
        elif self._match(TokenType.FALSE):
            self._advance()
            return Literal(False)
        elif self._match(TokenType.NULL):
            self._advance()
            return Literal(None)
        else:
            raise ParseError(f"Expected value, got {self._current().type.name}", self._current())
    
    def _parse_update(self) -> UpdateStatement:
        """Parse UPDATE statement"""
        self._expect(TokenType.UPDATE)
        
        table = self._expect(TokenType.IDENTIFIER).value
        
        self._expect(TokenType.SET)
        
        # Assignments
        assignments = []
        while True:
            col = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.EQUALS)
            value = self._parse_expression()
            assignments.append((col, value))
            
            if not self._consume_if(TokenType.COMMA):
                break
        
        # WHERE clause
        where = None
        if self._consume_if(TokenType.WHERE):
            where = self._parse_expression()
        
        self._consume_if(TokenType.SEMICOLON)
        
        return UpdateStatement(table=table, assignments=assignments, where=where)
    
    def _parse_delete(self) -> DeleteStatement:
        """Parse DELETE statement"""
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM)
        
        table = self._expect(TokenType.IDENTIFIER).value
        
        # WHERE clause
        where = None
        if self._consume_if(TokenType.WHERE):
            where = self._parse_expression()
        
        self._consume_if(TokenType.SEMICOLON)
        
        return DeleteStatement(table=table, where=where)
    
    def _parse_create(self) -> Union[CreateTableStatement, CreateIndexStatement]:
        """Parse CREATE statement"""
        self._expect(TokenType.CREATE)
        
        if self._consume_if(TokenType.UNIQUE):
            # CREATE UNIQUE INDEX
            self._expect(TokenType.INDEX)
            return self._parse_create_index(unique=True)
        elif self._consume_if(TokenType.INDEX):
            return self._parse_create_index(unique=False)
        elif self._consume_if(TokenType.TABLE):
            return self._parse_create_table()
        else:
            raise ParseError("Expected TABLE or INDEX after CREATE", self._current())
    
    def _parse_create_table(self) -> CreateTableStatement:
        """Parse CREATE TABLE statement"""
        table = self._expect(TokenType.IDENTIFIER).value
        
        self._expect(TokenType.LPAREN)
        
        columns = []
        while True:
            columns.append(self._parse_column_def())
            if not self._consume_if(TokenType.COMMA):
                break
        
        self._expect(TokenType.RPAREN)
        self._consume_if(TokenType.SEMICOLON)
        
        return CreateTableStatement(table=table, columns=columns)
    
    def _parse_column_def(self) -> ColumnDef:
        """Parse column definition"""
        name = self._expect(TokenType.IDENTIFIER).value
        
        # Parse data type
        type_parts = [self._expect(TokenType.IDENTIFIER).value]
        
        # Check for VARCHAR(n) style types
        if self._consume_if(TokenType.LPAREN):
            type_parts.append('(')
            type_parts.append(str(self._expect(TokenType.INTEGER).value))
            type_parts.append(')')
            self._expect(TokenType.RPAREN)
        
        data_type = ''.join(type_parts)
        
        col_def = ColumnDef(name=name, data_type=data_type)
        
        # Parse constraints
        while True:
            if self._consume_if(TokenType.PRIMARY):
                self._expect(TokenType.KEY)
                col_def.primary_key = True
            elif self._consume_if(TokenType.UNIQUE):
                col_def.unique = True
            elif self._consume_if(TokenType.NOT):
                self._expect(TokenType.NULL)
                col_def.not_null = True
            elif self._consume_if(TokenType.DEFAULT):
                col_def.default = self._parse_value().value
            else:
                break
        
        return col_def
    
    def _parse_create_index(self, unique: bool) -> CreateIndexStatement:
        """Parse CREATE INDEX statement"""
        name = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.ON)
        table = self._expect(TokenType.IDENTIFIER).value
        
        self._expect(TokenType.LPAREN)
        column = self._expect(TokenType.IDENTIFIER).value
        self._expect(TokenType.RPAREN)
        
        self._consume_if(TokenType.SEMICOLON)
        
        return CreateIndexStatement(name=name, table=table, column=column, unique=unique)
    
    def _parse_drop(self) -> Union[DropTableStatement, DropIndexStatement]:
        """Parse DROP statement"""
        self._expect(TokenType.DROP)
        
        if self._consume_if(TokenType.TABLE):
            table = self._expect(TokenType.IDENTIFIER).value
            self._consume_if(TokenType.SEMICOLON)
            return DropTableStatement(table=table)
        elif self._consume_if(TokenType.INDEX):
            name = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.ON)
            table = self._expect(TokenType.IDENTIFIER).value
            self._consume_if(TokenType.SEMICOLON)
            return DropIndexStatement(name=name, table=table)
        else:
            raise ParseError("Expected TABLE or INDEX after DROP", self._current())
    
    def _parse_show(self) -> ShowTablesStatement:
        """Parse SHOW TABLES statement"""
        self._expect(TokenType.SHOW)
        self._expect(TokenType.TABLES)
        self._consume_if(TokenType.SEMICOLON)
        return ShowTablesStatement()
    
    def _parse_describe(self) -> DescribeTableStatement:
        """Parse DESCRIBE statement"""
        self._expect(TokenType.DESCRIBE)
        table = self._expect(TokenType.IDENTIFIER).value
        self._consume_if(TokenType.SEMICOLON)
        return DescribeTableStatement(table=table)
    
    def _parse_truncate(self) -> TruncateTableStatement:
        """Parse TRUNCATE TABLE statement"""
        self._expect(TokenType.TRUNCATE)
        self._consume_if(TokenType.TABLE)
        table = self._expect(TokenType.IDENTIFIER).value
        self._consume_if(TokenType.SEMICOLON)
        return TruncateTableStatement(table=table)
    
    def _parse_expression(self) -> Any:
        """Parse expression (entry point)"""
        return self._parse_or_expression()
    
    def _parse_or_expression(self) -> Any:
        """Parse OR expression"""
        left = self._parse_and_expression()
        
        while self._consume_if(TokenType.OR):
            right = self._parse_and_expression()
            left = BinaryOp(left=left, operator='OR', right=right)
        
        return left
    
    def _parse_and_expression(self) -> Any:
        """Parse AND expression"""
        left = self._parse_not_expression()
        
        while self._consume_if(TokenType.AND):
            right = self._parse_not_expression()
            left = BinaryOp(left=left, operator='AND', right=right)
        
        return left
    
    def _parse_not_expression(self) -> Any:
        """Parse NOT expression"""
        if self._consume_if(TokenType.NOT):
            return UnaryOp(operator='NOT', operand=self._parse_not_expression())
        return self._parse_comparison()
    
    def _parse_comparison(self) -> Any:
        """Parse comparison expression"""
        left = self._parse_additive()
        
        # IS NULL / IS NOT NULL
        if self._consume_if(TokenType.IS):
            if self._consume_if(TokenType.NOT):
                self._expect(TokenType.NULL)
                return BinaryOp(left=left, operator='IS NOT NULL', right=None)
            self._expect(TokenType.NULL)
            return BinaryOp(left=left, operator='IS NULL', right=None)
        
        # BETWEEN
        if self._consume_if(TokenType.BETWEEN):
            low = self._parse_additive()
            self._expect(TokenType.AND)
            high = self._parse_additive()
            return BinaryOp(
                left=BinaryOp(left=left, operator='>=', right=low),
                operator='AND',
                right=BinaryOp(left=left, operator='<=', right=high)
            )
        
        # LIKE
        if self._consume_if(TokenType.LIKE):
            pattern = self._parse_additive()
            return BinaryOp(left=left, operator='LIKE', right=pattern)
        
        # IN
        if self._consume_if(TokenType.IN):
            self._expect(TokenType.LPAREN)
            values = self._parse_expression_list()
            self._expect(TokenType.RPAREN)
            return BinaryOp(left=left, operator='IN', right=values)
        
        # Standard comparison operators
        if self._match(TokenType.EQUALS):
            self._advance()
            return BinaryOp(left=left, operator='=', right=self._parse_additive())
        elif self._match(TokenType.NOT_EQUALS):
            self._advance()
            return BinaryOp(left=left, operator='!=', right=self._parse_additive())
        elif self._match(TokenType.LESS_THAN):
            self._advance()
            return BinaryOp(left=left, operator='<', right=self._parse_additive())
        elif self._match(TokenType.GREATER_THAN):
            self._advance()
            return BinaryOp(left=left, operator='>', right=self._parse_additive())
        elif self._match(TokenType.LESS_EQUALS):
            self._advance()
            return BinaryOp(left=left, operator='<=', right=self._parse_additive())
        elif self._match(TokenType.GREATER_EQUALS):
            self._advance()
            return BinaryOp(left=left, operator='>=', right=self._parse_additive())
        
        return left
    
    def _parse_additive(self) -> Any:
        """Parse additive expression (+, -)"""
        left = self._parse_multiplicative()
        
        while self._match(TokenType.PLUS, TokenType.MINUS):
            op = '+' if self._advance().type == TokenType.PLUS else '-'
            right = self._parse_multiplicative()
            left = BinaryOp(left=left, operator=op, right=right)
        
        return left
    
    def _parse_multiplicative(self) -> Any:
        """Parse multiplicative expression (*, /)"""
        left = self._parse_unary()
        
        while self._match(TokenType.STAR, TokenType.DIVIDE):
            op = '*' if self._advance().type == TokenType.STAR else '/'
            right = self._parse_unary()
            left = BinaryOp(left=left, operator=op, right=right)
        
        return left
    
    def _parse_unary(self) -> Any:
        """Parse unary expression"""
        if self._match(TokenType.MINUS):
            self._advance()
            return UnaryOp(operator='-', operand=self._parse_unary())
        return self._parse_primary()
    
    def _parse_primary(self) -> Any:
        """Parse primary expression"""
        # Parenthesized expression
        if self._consume_if(TokenType.LPAREN):
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN)
            return expr
        
        # Literals
        if self._match(TokenType.INTEGER, TokenType.FLOAT, TokenType.STRING):
            return Literal(self._advance().value)
        
        if self._match(TokenType.TRUE):
            self._advance()
            return Literal(True)
        
        if self._match(TokenType.FALSE):
            self._advance()
            return Literal(False)
        
        if self._match(TokenType.NULL):
            self._advance()
            return Literal(None)
        
        # Aggregate functions
        if self._match(TokenType.COUNT, TokenType.SUM, TokenType.AVG, 
                      TokenType.MIN, TokenType.MAX):
            func_name = self._advance().value
            self._expect(TokenType.LPAREN)
            
            distinct = self._consume_if(TokenType.DISTINCT)
            
            if self._match(TokenType.STAR):
                self._advance()
                args = ['*']
            else:
                args = self._parse_expression_list()
            
            self._expect(TokenType.RPAREN)
            return FunctionCall(name=func_name, args=args, distinct=distinct)
        
        # Identifier (column reference or table.column)
        if self._match(TokenType.IDENTIFIER):
            name = self._advance().value
            
            # Check for table.column
            if self._consume_if(TokenType.DOT):
                column = self._expect(TokenType.IDENTIFIER).value
                return ColumnRef(column=column, table=name)
            
            return ColumnRef(column=name)
        
        raise ParseError(f"Unexpected token: {self._current().value}", self._current())
    
    def _parse_expression_list(self) -> List[Any]:
        """Parse comma-separated expression list"""
        exprs = []
        while True:
            exprs.append(self._parse_expression())
            if not self._consume_if(TokenType.COMMA):
                break
        return exprs


def parse_sql(sql: str) -> Any:
    """Parse a SQL string into an AST"""
    lexer = Lexer(sql)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    return parser.parse()
