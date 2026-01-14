"""
SQL Lexer - Tokenizes SQL statements

Converts raw SQL strings into a stream of tokens for the parser.
"""

import re
from enum import Enum, auto
from dataclasses import dataclass
from typing import List, Optional, Iterator


class TokenType(Enum):
    """Types of tokens in SQL"""
    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    UPDATE = auto()
    SET = auto()
    DELETE = auto()
    CREATE = auto()
    DROP = auto()
    TABLE = auto()
    INDEX = auto()
    ON = auto()
    PRIMARY = auto()
    KEY = auto()
    UNIQUE = auto()
    NOT = auto()
    NULL = auto()
    AND = auto()
    OR = auto()
    JOIN = auto()
    INNER = auto()
    LEFT = auto()
    RIGHT = auto()
    OUTER = auto()
    CROSS = auto()
    ORDER = auto()
    BY = auto()
    ASC = auto()
    DESC = auto()
    LIMIT = auto()
    OFFSET = auto()
    AS = auto()
    DISTINCT = auto()
    COUNT = auto()
    SUM = auto()
    AVG = auto()
    MIN = auto()
    MAX = auto()
    GROUP = auto()
    HAVING = auto()
    LIKE = auto()
    IN = auto()
    BETWEEN = auto()
    IS = auto()
    TRUE = auto()
    FALSE = auto()
    DEFAULT = auto()
    SHOW = auto()
    TABLES = auto()
    DESCRIBE = auto()
    TRUNCATE = auto()
    
    # Operators
    EQUALS = auto()
    NOT_EQUALS = auto()
    LESS_THAN = auto()
    GREATER_THAN = auto()
    LESS_EQUALS = auto()
    GREATER_EQUALS = auto()
    PLUS = auto()
    MINUS = auto()
    MULTIPLY = auto()
    DIVIDE = auto()
    
    # Punctuation
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    SEMICOLON = auto()
    DOT = auto()
    STAR = auto()
    
    # Literals
    INTEGER = auto()
    FLOAT = auto()
    STRING = auto()
    IDENTIFIER = auto()
    
    # Special
    EOF = auto()
    NEWLINE = auto()


@dataclass
class Token:
    """A single token"""
    type: TokenType
    value: any
    line: int
    column: int
    
    def __repr__(self):
        return f"Token({self.type.name}, {self.value!r})"


class Lexer:
    """SQL Lexer - converts SQL text to tokens"""
    
    # Keywords mapping
    KEYWORDS = {
        'SELECT': TokenType.SELECT,
        'FROM': TokenType.FROM,
        'WHERE': TokenType.WHERE,
        'INSERT': TokenType.INSERT,
        'INTO': TokenType.INTO,
        'VALUES': TokenType.VALUES,
        'UPDATE': TokenType.UPDATE,
        'SET': TokenType.SET,
        'DELETE': TokenType.DELETE,
        'CREATE': TokenType.CREATE,
        'DROP': TokenType.DROP,
        'TABLE': TokenType.TABLE,
        'INDEX': TokenType.INDEX,
        'ON': TokenType.ON,
        'PRIMARY': TokenType.PRIMARY,
        'KEY': TokenType.KEY,
        'UNIQUE': TokenType.UNIQUE,
        'NOT': TokenType.NOT,
        'NULL': TokenType.NULL,
        'AND': TokenType.AND,
        'OR': TokenType.OR,
        'JOIN': TokenType.JOIN,
        'INNER': TokenType.INNER,
        'LEFT': TokenType.LEFT,
        'RIGHT': TokenType.RIGHT,
        'OUTER': TokenType.OUTER,
        'CROSS': TokenType.CROSS,
        'ORDER': TokenType.ORDER,
        'BY': TokenType.BY,
        'ASC': TokenType.ASC,
        'DESC': TokenType.DESC,
        'LIMIT': TokenType.LIMIT,
        'OFFSET': TokenType.OFFSET,
        'AS': TokenType.AS,
        'DISTINCT': TokenType.DISTINCT,
        'COUNT': TokenType.COUNT,
        'SUM': TokenType.SUM,
        'AVG': TokenType.AVG,
        'MIN': TokenType.MIN,
        'MAX': TokenType.MAX,
        'GROUP': TokenType.GROUP,
        'HAVING': TokenType.HAVING,
        'LIKE': TokenType.LIKE,
        'IN': TokenType.IN,
        'BETWEEN': TokenType.BETWEEN,
        'IS': TokenType.IS,
        'TRUE': TokenType.TRUE,
        'FALSE': TokenType.FALSE,
        'DEFAULT': TokenType.DEFAULT,
        'SHOW': TokenType.SHOW,
        'TABLES': TokenType.TABLES,
        'DESCRIBE': TokenType.DESCRIBE,
        'TRUNCATE': TokenType.TRUNCATE,
    }
    
    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1
    
    def _current_char(self) -> Optional[str]:
        """Get current character or None if at end"""
        if self.pos >= len(self.text):
            return None
        return self.text[self.pos]
    
    def _peek(self, offset: int = 1) -> Optional[str]:
        """Peek at character ahead"""
        pos = self.pos + offset
        if pos >= len(self.text):
            return None
        return self.text[pos]
    
    def _advance(self) -> str:
        """Advance position and return current char"""
        char = self._current_char()
        self.pos += 1
        if char == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        return char
    
    def _skip_whitespace(self) -> None:
        """Skip whitespace characters"""
        while self._current_char() and self._current_char() in ' \t\r':
            self._advance()
    
    def _skip_comment(self) -> None:
        """Skip SQL comments"""
        if self._current_char() == '-' and self._peek() == '-':
            # Single line comment
            while self._current_char() and self._current_char() != '\n':
                self._advance()
        elif self._current_char() == '/' and self._peek() == '*':
            # Multi-line comment
            self._advance()  # /
            self._advance()  # *
            while self._current_char():
                if self._current_char() == '*' and self._peek() == '/':
                    self._advance()  # *
                    self._advance()  # /
                    break
                self._advance()
    
    def _read_string(self, quote_char: str) -> Token:
        """Read a string literal"""
        start_line = self.line
        start_col = self.column
        self._advance()  # Opening quote
        
        value = []
        while self._current_char() and self._current_char() != quote_char:
            if self._current_char() == '\\' and self._peek() == quote_char:
                self._advance()  # Skip escape
            value.append(self._advance())
        
        if self._current_char() == quote_char:
            self._advance()  # Closing quote
        
        return Token(TokenType.STRING, ''.join(value), start_line, start_col)
    
    def _read_number(self) -> Token:
        """Read a numeric literal"""
        start_line = self.line
        start_col = self.column
        
        value = []
        has_dot = False
        
        while self._current_char() and (self._current_char().isdigit() or 
                                        self._current_char() == '.'):
            if self._current_char() == '.':
                if has_dot:
                    break
                has_dot = True
            value.append(self._advance())
        
        num_str = ''.join(value)
        if has_dot:
            return Token(TokenType.FLOAT, float(num_str), start_line, start_col)
        return Token(TokenType.INTEGER, int(num_str), start_line, start_col)
    
    def _read_identifier(self) -> Token:
        """Read an identifier or keyword"""
        start_line = self.line
        start_col = self.column
        
        value = []
        while self._current_char() and (self._current_char().isalnum() or 
                                        self._current_char() == '_'):
            value.append(self._advance())
        
        identifier = ''.join(value)
        upper_id = identifier.upper()
        
        # Check if it's a keyword
        if upper_id in self.KEYWORDS:
            return Token(self.KEYWORDS[upper_id], upper_id, start_line, start_col)
        
        return Token(TokenType.IDENTIFIER, identifier, start_line, start_col)
    
    def tokenize(self) -> List[Token]:
        """Tokenize the entire input"""
        tokens = []
        
        while self._current_char() is not None:
            self._skip_whitespace()
            
            if self._current_char() is None:
                break
            
            # Skip comments
            if (self._current_char() == '-' and self._peek() == '-') or \
               (self._current_char() == '/' and self._peek() == '*'):
                self._skip_comment()
                continue
            
            char = self._current_char()
            start_line = self.line
            start_col = self.column
            
            # Newline
            if char == '\n':
                self._advance()
                continue
            
            # String literals
            if char in '"\'':
                tokens.append(self._read_string(char))
                continue
            
            # Numbers
            if char.isdigit():
                tokens.append(self._read_number())
                continue
            
            # Identifiers and keywords
            if char.isalpha() or char == '_':
                tokens.append(self._read_identifier())
                continue
            
            # Two-character operators
            if char == '!' and self._peek() == '=':
                self._advance()
                self._advance()
                tokens.append(Token(TokenType.NOT_EQUALS, '!=', start_line, start_col))
                continue
            
            if char == '<' and self._peek() == '=':
                self._advance()
                self._advance()
                tokens.append(Token(TokenType.LESS_EQUALS, '<=', start_line, start_col))
                continue
            
            if char == '>' and self._peek() == '=':
                self._advance()
                self._advance()
                tokens.append(Token(TokenType.GREATER_EQUALS, '>=', start_line, start_col))
                continue
            
            if char == '<' and self._peek() == '>':
                self._advance()
                self._advance()
                tokens.append(Token(TokenType.NOT_EQUALS, '<>', start_line, start_col))
                continue
            
            # Single-character tokens
            single_char_tokens = {
                '=': TokenType.EQUALS,
                '<': TokenType.LESS_THAN,
                '>': TokenType.GREATER_THAN,
                '+': TokenType.PLUS,
                '-': TokenType.MINUS,
                '*': TokenType.STAR,
                '/': TokenType.DIVIDE,
                '(': TokenType.LPAREN,
                ')': TokenType.RPAREN,
                ',': TokenType.COMMA,
                ';': TokenType.SEMICOLON,
                '.': TokenType.DOT,
            }
            
            if char in single_char_tokens:
                self._advance()
                tokens.append(Token(single_char_tokens[char], char, start_line, start_col))
                continue
            
            # Unknown character - skip
            self._advance()
        
        # Add EOF token
        tokens.append(Token(TokenType.EOF, None, self.line, self.column))
        
        return tokens
