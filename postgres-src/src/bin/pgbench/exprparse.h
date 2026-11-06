/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_EXPR_YY_EXPRPARSE_H_INCLUDED
# define YY_EXPR_YY_EXPRPARSE_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int expr_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    NULL_CONST = 258,              /* NULL_CONST  */
    INTEGER_CONST = 259,           /* INTEGER_CONST  */
    MAXINT_PLUS_ONE_CONST = 260,   /* MAXINT_PLUS_ONE_CONST  */
    DOUBLE_CONST = 261,            /* DOUBLE_CONST  */
    BOOLEAN_CONST = 262,           /* BOOLEAN_CONST  */
    VARIABLE = 263,                /* VARIABLE  */
    FUNCTION = 264,                /* FUNCTION  */
    AND_OP = 265,                  /* AND_OP  */
    OR_OP = 266,                   /* OR_OP  */
    NOT_OP = 267,                  /* NOT_OP  */
    NE_OP = 268,                   /* NE_OP  */
    LE_OP = 269,                   /* LE_OP  */
    GE_OP = 270,                   /* GE_OP  */
    LS_OP = 271,                   /* LS_OP  */
    RS_OP = 272,                   /* RS_OP  */
    IS_OP = 273,                   /* IS_OP  */
    CASE_KW = 274,                 /* CASE_KW  */
    WHEN_KW = 275,                 /* WHEN_KW  */
    THEN_KW = 276,                 /* THEN_KW  */
    ELSE_KW = 277,                 /* ELSE_KW  */
    END_KW = 278,                  /* END_KW  */
    ISNULL_OP = 279,               /* ISNULL_OP  */
    NOTNULL_OP = 280,              /* NOTNULL_OP  */
    UNARY = 281                    /* UNARY  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 49 "exprparse.y"

	int64		ival;
	double		dval;
	bool		bval;
	char	   *str;
	PgBenchExpr *expr;
	PgBenchExprList *elist;

#line 99 "exprparse.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif




int expr_yyparse (yyscan_t yyscanner);


#endif /* !YY_EXPR_YY_EXPRPARSE_H_INCLUDED  */
