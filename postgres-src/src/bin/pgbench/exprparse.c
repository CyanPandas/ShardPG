/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         expr_yyparse
#define yylex           expr_yylex
#define yyerror         expr_yyerror
#define yydebug         expr_yydebug
#define yynerrs         expr_yynerrs

/* First part of user prologue.  */
#line 1 "exprparse.y"

/*-------------------------------------------------------------------------
 *
 * exprparse.y
 *	  bison grammar for a simple expression syntax
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pgbench/exprparse.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "pgbench.h"

#define PGBENCH_NARGS_VARIABLE	(-1)
#define PGBENCH_NARGS_CASE		(-2)
#define PGBENCH_NARGS_HASH		(-3)
#define PGBENCH_NARGS_PERMUTE	(-4)

PgBenchExpr *expr_parse_result;

static PgBenchExprList *make_elist(PgBenchExpr *expr, PgBenchExprList *list);
static PgBenchExpr *make_null_constant(void);
static PgBenchExpr *make_boolean_constant(bool bval);
static PgBenchExpr *make_integer_constant(int64 ival);
static PgBenchExpr *make_double_constant(double dval);
static PgBenchExpr *make_variable(char *varname);
static PgBenchExpr *make_op(yyscan_t yyscanner, const char *operator,
							PgBenchExpr *lexpr, PgBenchExpr *rexpr);
static PgBenchExpr *make_uop(yyscan_t yyscanner, const char *operator, PgBenchExpr *expr);
static int	find_func(yyscan_t yyscanner, const char *fname);
static PgBenchExpr *make_func(yyscan_t yyscanner, int fnumber, PgBenchExprList *args);
static PgBenchExpr *make_case(yyscan_t yyscanner, PgBenchExprList *when_then_list, PgBenchExpr *else_part);


#line 116 "exprparse.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "exprparse.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_NULL_CONST = 3,                 /* NULL_CONST  */
  YYSYMBOL_INTEGER_CONST = 4,              /* INTEGER_CONST  */
  YYSYMBOL_MAXINT_PLUS_ONE_CONST = 5,      /* MAXINT_PLUS_ONE_CONST  */
  YYSYMBOL_DOUBLE_CONST = 6,               /* DOUBLE_CONST  */
  YYSYMBOL_BOOLEAN_CONST = 7,              /* BOOLEAN_CONST  */
  YYSYMBOL_VARIABLE = 8,                   /* VARIABLE  */
  YYSYMBOL_FUNCTION = 9,                   /* FUNCTION  */
  YYSYMBOL_AND_OP = 10,                    /* AND_OP  */
  YYSYMBOL_OR_OP = 11,                     /* OR_OP  */
  YYSYMBOL_NOT_OP = 12,                    /* NOT_OP  */
  YYSYMBOL_NE_OP = 13,                     /* NE_OP  */
  YYSYMBOL_LE_OP = 14,                     /* LE_OP  */
  YYSYMBOL_GE_OP = 15,                     /* GE_OP  */
  YYSYMBOL_LS_OP = 16,                     /* LS_OP  */
  YYSYMBOL_RS_OP = 17,                     /* RS_OP  */
  YYSYMBOL_IS_OP = 18,                     /* IS_OP  */
  YYSYMBOL_CASE_KW = 19,                   /* CASE_KW  */
  YYSYMBOL_WHEN_KW = 20,                   /* WHEN_KW  */
  YYSYMBOL_THEN_KW = 21,                   /* THEN_KW  */
  YYSYMBOL_ELSE_KW = 22,                   /* ELSE_KW  */
  YYSYMBOL_END_KW = 23,                    /* END_KW  */
  YYSYMBOL_ISNULL_OP = 24,                 /* ISNULL_OP  */
  YYSYMBOL_NOTNULL_OP = 25,                /* NOTNULL_OP  */
  YYSYMBOL_26_ = 26,                       /* '<'  */
  YYSYMBOL_27_ = 27,                       /* '>'  */
  YYSYMBOL_28_ = 28,                       /* '='  */
  YYSYMBOL_29_ = 29,                       /* '|'  */
  YYSYMBOL_30_ = 30,                       /* '#'  */
  YYSYMBOL_31_ = 31,                       /* '&'  */
  YYSYMBOL_32_ = 32,                       /* '~'  */
  YYSYMBOL_33_ = 33,                       /* '+'  */
  YYSYMBOL_34_ = 34,                       /* '-'  */
  YYSYMBOL_35_ = 35,                       /* '*'  */
  YYSYMBOL_36_ = 36,                       /* '/'  */
  YYSYMBOL_37_ = 37,                       /* '%'  */
  YYSYMBOL_UNARY = 38,                     /* UNARY  */
  YYSYMBOL_39_ = 39,                       /* ','  */
  YYSYMBOL_40_ = 40,                       /* '('  */
  YYSYMBOL_41_ = 41,                       /* ')'  */
  YYSYMBOL_YYACCEPT = 42,                  /* $accept  */
  YYSYMBOL_result = 43,                    /* result  */
  YYSYMBOL_elist = 44,                     /* elist  */
  YYSYMBOL_expr = 45,                      /* expr  */
  YYSYMBOL_when_then_list = 46,            /* when_then_list  */
  YYSYMBOL_case_control = 47,              /* case_control  */
  YYSYMBOL_function = 48                   /* function  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_int8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if !defined yyoverflow

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  25
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   320

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  42
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  7
/* YYNRULES -- Number of rules.  */
#define YYNRULES  47
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  88

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   281


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,    30,     2,    37,    31,     2,
      40,    41,    35,    33,    39,    34,     2,    36,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      26,    28,    27,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    29,     2,    32,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    38
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint8 yyrline[] =
{
       0,    83,    83,    88,    89,    90,    93,    94,    96,    99,
     102,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   115,   116,   117,   118,   119,   120,   121,   122,
     124,   125,   129,   130,   135,   139,   145,   146,   147,   148,
     150,   151,   152,   156,   157,   160,   161,   163
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "NULL_CONST",
  "INTEGER_CONST", "MAXINT_PLUS_ONE_CONST", "DOUBLE_CONST",
  "BOOLEAN_CONST", "VARIABLE", "FUNCTION", "AND_OP", "OR_OP", "NOT_OP",
  "NE_OP", "LE_OP", "GE_OP", "LS_OP", "RS_OP", "IS_OP", "CASE_KW",
  "WHEN_KW", "THEN_KW", "ELSE_KW", "END_KW", "ISNULL_OP", "NOTNULL_OP",
  "'<'", "'>'", "'='", "'|'", "'#'", "'&'", "'~'", "'+'", "'-'", "'*'",
  "'/'", "'%'", "UNARY", "','", "'('", "')'", "$accept", "result", "elist",
  "expr", "when_then_list", "case_control", "function", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-33)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  ((Yyn) == YYTABLE_NINF)

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      64,   -33,   -33,   -33,   -33,   -33,   -33,    64,   -19,    64,
      64,    46,    64,    13,   205,   -33,   -22,   258,    64,    -6,
      11,   -33,   -33,   -33,    92,   -33,    64,    64,    64,    64,
      64,    64,    64,     3,   -33,   -33,    64,    64,    64,    64,
      64,    64,    64,    64,    64,    64,    64,    64,   121,    64,
      64,   -33,   -33,   258,   233,   283,   283,   283,    11,    11,
     -33,   -33,     5,   283,   283,   283,    11,    11,    11,    -9,
      -9,   -33,   -33,   -33,   -32,   205,    64,   149,   177,   -33,
     -33,    64,   -33,   205,    64,   -33,   205,   205
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,    36,    38,    39,    37,    40,    47,     0,     0,     0,
       0,     0,     0,     0,     2,    42,     0,    11,     0,     0,
      10,     7,     9,     8,     0,     1,     0,     0,     0,     0,
       0,     0,     0,     0,    30,    31,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     3,     0,     0,
       0,    45,     6,    28,    29,    22,    18,    20,    26,    27,
      32,    34,     0,    17,    19,    21,    24,    25,    23,    12,
      13,    14,    15,    16,     0,     4,     0,     0,     0,    33,
      35,     0,    41,    44,     0,    46,     5,    43
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -33,   -33,   -33,    -7,   -33,   -33,   -33
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,    13,    74,    14,    19,    15,    16
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      17,    18,    20,    21,    23,    24,    60,    81,    79,    82,
      61,    48,    80,    25,    49,    62,    50,    51,    47,    53,
      54,    55,    56,    57,    58,    59,    44,    45,    46,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    72,    73,
      75,     0,    77,    78,    42,    43,    44,    45,    46,     1,
       2,    22,     3,     4,     5,     6,     0,     0,     7,     0,
       0,     0,     0,     0,     0,     8,     0,     1,     2,    83,
       3,     4,     5,     6,    86,     0,     7,    87,     9,    10,
      11,     0,     0,     8,     0,     0,    12,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     9,    10,    11,     0,
       0,     0,    26,    27,    12,    28,    29,    30,    31,    32,
      33,     0,     0,     0,     0,     0,    34,    35,    36,    37,
      38,    39,    40,    41,     0,    42,    43,    44,    45,    46,
       0,    26,    27,    52,    28,    29,    30,    31,    32,    33,
       0,     0,    76,     0,     0,    34,    35,    36,    37,    38,
      39,    40,    41,     0,    42,    43,    44,    45,    46,    26,
      27,     0,    28,    29,    30,    31,    32,    33,     0,     0,
      84,     0,     0,    34,    35,    36,    37,    38,    39,    40,
      41,     0,    42,    43,    44,    45,    46,    26,    27,     0,
      28,    29,    30,    31,    32,    33,     0,     0,     0,     0,
      85,    34,    35,    36,    37,    38,    39,    40,    41,     0,
      42,    43,    44,    45,    46,    26,    27,     0,    28,    29,
      30,    31,    32,    33,     0,     0,     0,     0,     0,    34,
      35,    36,    37,    38,    39,    40,    41,     0,    42,    43,
      44,    45,    46,    26,     0,     0,    28,    29,    30,    31,
      32,    33,     0,     0,     0,     0,     0,    34,    35,    36,
      37,    38,    39,    40,    41,     0,    42,    43,    44,    45,
      46,    28,    29,    30,    31,    32,    33,     0,     0,     0,
       0,     0,    34,    35,    36,    37,    38,    39,    40,    41,
       0,    42,    43,    44,    45,    46,    -1,    -1,    -1,    31,
      32,     0,     0,     0,     0,     0,     0,     0,     0,    -1,
      -1,    -1,    39,    40,    41,     0,    42,    43,    44,    45,
      46
};

static const yytype_int8 yycheck[] =
{
       7,    20,     9,    10,    11,    12,     3,    39,     3,    41,
       7,    18,     7,     0,    20,    12,    22,    23,    40,    26,
      27,    28,    29,    30,    31,    32,    35,    36,    37,    36,
      37,    38,    39,    40,    41,    42,    43,    44,    45,    46,
      47,    -1,    49,    50,    33,    34,    35,    36,    37,     3,
       4,     5,     6,     7,     8,     9,    -1,    -1,    12,    -1,
      -1,    -1,    -1,    -1,    -1,    19,    -1,     3,     4,    76,
       6,     7,     8,     9,    81,    -1,    12,    84,    32,    33,
      34,    -1,    -1,    19,    -1,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    34,    -1,
      -1,    -1,    10,    11,    40,    13,    14,    15,    16,    17,
      18,    -1,    -1,    -1,    -1,    -1,    24,    25,    26,    27,
      28,    29,    30,    31,    -1,    33,    34,    35,    36,    37,
      -1,    10,    11,    41,    13,    14,    15,    16,    17,    18,
      -1,    -1,    21,    -1,    -1,    24,    25,    26,    27,    28,
      29,    30,    31,    -1,    33,    34,    35,    36,    37,    10,
      11,    -1,    13,    14,    15,    16,    17,    18,    -1,    -1,
      21,    -1,    -1,    24,    25,    26,    27,    28,    29,    30,
      31,    -1,    33,    34,    35,    36,    37,    10,    11,    -1,
      13,    14,    15,    16,    17,    18,    -1,    -1,    -1,    -1,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    -1,
      33,    34,    35,    36,    37,    10,    11,    -1,    13,    14,
      15,    16,    17,    18,    -1,    -1,    -1,    -1,    -1,    24,
      25,    26,    27,    28,    29,    30,    31,    -1,    33,    34,
      35,    36,    37,    10,    -1,    -1,    13,    14,    15,    16,
      17,    18,    -1,    -1,    -1,    -1,    -1,    24,    25,    26,
      27,    28,    29,    30,    31,    -1,    33,    34,    35,    36,
      37,    13,    14,    15,    16,    17,    18,    -1,    -1,    -1,
      -1,    -1,    24,    25,    26,    27,    28,    29,    30,    31,
      -1,    33,    34,    35,    36,    37,    13,    14,    15,    16,
      17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    26,
      27,    28,    29,    30,    31,    -1,    33,    34,    35,    36,
      37
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     4,     6,     7,     8,     9,    12,    19,    32,
      33,    34,    40,    43,    45,    47,    48,    45,    20,    46,
      45,    45,     5,    45,    45,     0,    10,    11,    13,    14,
      15,    16,    17,    18,    24,    25,    26,    27,    28,    29,
      30,    31,    33,    34,    35,    36,    37,    40,    45,    20,
      22,    23,    41,    45,    45,    45,    45,    45,    45,    45,
       3,     7,    12,    45,    45,    45,    45,    45,    45,    45,
      45,    45,    45,    45,    44,    45,    21,    45,    45,     3,
       7,    39,    41,    45,    21,    23,    45,    45
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    42,    43,    44,    44,    44,    45,    45,    45,    45,
      45,    45,    45,    45,    45,    45,    45,    45,    45,    45,
      45,    45,    45,    45,    45,    45,    45,    45,    45,    45,
      45,    45,    45,    45,    45,    45,    45,    45,    45,    45,
      45,    45,    45,    46,    46,    47,    47,    48
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     0,     1,     3,     3,     2,     2,     2,
       2,     2,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       2,     2,     3,     4,     3,     4,     1,     1,     1,     1,
       1,     4,     1,     5,     4,     3,     5,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (yyscanner, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, yyscanner); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, yyscan_t yyscanner)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yyscanner);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, yyscan_t yyscanner)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, yyscanner);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule, yyscan_t yyscanner)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)], yyscanner);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, yyscanner); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, yyscan_t yyscanner)
{
  YY_USE (yyvaluep);
  YY_USE (yyscanner);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (yyscan_t yyscanner)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, yyscanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* result: expr  */
#line 83 "exprparse.y"
                                        {
								expr_parse_result = (yyvsp[0].expr);
								(void) yynerrs; /* suppress compiler warning */
							}
#line 1257 "exprparse.c"
    break;

  case 3: /* elist: %empty  */
#line 88 "exprparse.y"
                                                { (yyval.elist) = NULL; }
#line 1263 "exprparse.c"
    break;

  case 4: /* elist: expr  */
#line 89 "exprparse.y"
                                                { (yyval.elist) = make_elist((yyvsp[0].expr), NULL); }
#line 1269 "exprparse.c"
    break;

  case 5: /* elist: elist ',' expr  */
#line 90 "exprparse.y"
                                        { (yyval.elist) = make_elist((yyvsp[0].expr), (yyvsp[-2].elist)); }
#line 1275 "exprparse.c"
    break;

  case 6: /* expr: '(' expr ')'  */
#line 93 "exprparse.y"
                                        { (yyval.expr) = (yyvsp[-1].expr); }
#line 1281 "exprparse.c"
    break;

  case 7: /* expr: '+' expr  */
#line 94 "exprparse.y"
                                { (yyval.expr) = (yyvsp[0].expr); }
#line 1287 "exprparse.c"
    break;

  case 8: /* expr: '-' expr  */
#line 96 "exprparse.y"
                                { (yyval.expr) = make_op(yyscanner, "-",
										   make_integer_constant(0), (yyvsp[0].expr)); }
#line 1294 "exprparse.c"
    break;

  case 9: /* expr: '-' MAXINT_PLUS_ONE_CONST  */
#line 100 "exprparse.y"
                                                        { (yyval.expr) = make_integer_constant(PG_INT64_MIN); }
#line 1300 "exprparse.c"
    break;

  case 10: /* expr: '~' expr  */
#line 102 "exprparse.y"
                                                { (yyval.expr) = make_op(yyscanner, "#",
										   make_integer_constant(~INT64CONST(0)), (yyvsp[0].expr)); }
#line 1307 "exprparse.c"
    break;

  case 11: /* expr: NOT_OP expr  */
#line 104 "exprparse.y"
                                        { (yyval.expr) = make_uop(yyscanner, "!not", (yyvsp[0].expr)); }
#line 1313 "exprparse.c"
    break;

  case 12: /* expr: expr '+' expr  */
#line 105 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "+", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1319 "exprparse.c"
    break;

  case 13: /* expr: expr '-' expr  */
#line 106 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "-", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1325 "exprparse.c"
    break;

  case 14: /* expr: expr '*' expr  */
#line 107 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "*", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1331 "exprparse.c"
    break;

  case 15: /* expr: expr '/' expr  */
#line 108 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "/", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1337 "exprparse.c"
    break;

  case 16: /* expr: expr '%' expr  */
#line 109 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "mod", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1343 "exprparse.c"
    break;

  case 17: /* expr: expr '<' expr  */
#line 110 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "<", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1349 "exprparse.c"
    break;

  case 18: /* expr: expr LE_OP expr  */
#line 111 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "<=", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1355 "exprparse.c"
    break;

  case 19: /* expr: expr '>' expr  */
#line 112 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "<", (yyvsp[0].expr), (yyvsp[-2].expr)); }
#line 1361 "exprparse.c"
    break;

  case 20: /* expr: expr GE_OP expr  */
#line 113 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "<=", (yyvsp[0].expr), (yyvsp[-2].expr)); }
#line 1367 "exprparse.c"
    break;

  case 21: /* expr: expr '=' expr  */
#line 114 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "=", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1373 "exprparse.c"
    break;

  case 22: /* expr: expr NE_OP expr  */
#line 115 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "<>", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1379 "exprparse.c"
    break;

  case 23: /* expr: expr '&' expr  */
#line 116 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "&", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1385 "exprparse.c"
    break;

  case 24: /* expr: expr '|' expr  */
#line 117 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "|", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1391 "exprparse.c"
    break;

  case 25: /* expr: expr '#' expr  */
#line 118 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "#", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1397 "exprparse.c"
    break;

  case 26: /* expr: expr LS_OP expr  */
#line 119 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "<<", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1403 "exprparse.c"
    break;

  case 27: /* expr: expr RS_OP expr  */
#line 120 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, ">>", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1409 "exprparse.c"
    break;

  case 28: /* expr: expr AND_OP expr  */
#line 121 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "!and", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1415 "exprparse.c"
    break;

  case 29: /* expr: expr OR_OP expr  */
#line 122 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "!or", (yyvsp[-2].expr), (yyvsp[0].expr)); }
#line 1421 "exprparse.c"
    break;

  case 30: /* expr: expr ISNULL_OP  */
#line 124 "exprparse.y"
                                        { (yyval.expr) = make_op(yyscanner, "!is", (yyvsp[-1].expr), make_null_constant()); }
#line 1427 "exprparse.c"
    break;

  case 31: /* expr: expr NOTNULL_OP  */
#line 125 "exprparse.y"
                                        {
								(yyval.expr) = make_uop(yyscanner, "!not",
											  make_op(yyscanner, "!is", (yyvsp[-1].expr), make_null_constant()));
							}
#line 1436 "exprparse.c"
    break;

  case 32: /* expr: expr IS_OP NULL_CONST  */
#line 129 "exprparse.y"
                                { (yyval.expr) = make_op(yyscanner, "!is", (yyvsp[-2].expr), make_null_constant()); }
#line 1442 "exprparse.c"
    break;

  case 33: /* expr: expr IS_OP NOT_OP NULL_CONST  */
#line 131 "exprparse.y"
                                                        {
								(yyval.expr) = make_uop(yyscanner, "!not",
											  make_op(yyscanner, "!is", (yyvsp[-3].expr), make_null_constant()));
							}
#line 1451 "exprparse.c"
    break;

  case 34: /* expr: expr IS_OP BOOLEAN_CONST  */
#line 136 "exprparse.y"
                                                        {
								(yyval.expr) = make_op(yyscanner, "!is", (yyvsp[-2].expr), make_boolean_constant((yyvsp[0].bval)));
							}
#line 1459 "exprparse.c"
    break;

  case 35: /* expr: expr IS_OP NOT_OP BOOLEAN_CONST  */
#line 140 "exprparse.y"
                                                        {
								(yyval.expr) = make_uop(yyscanner, "!not",
											  make_op(yyscanner, "!is", (yyvsp[-3].expr), make_boolean_constant((yyvsp[0].bval))));
							}
#line 1468 "exprparse.c"
    break;

  case 36: /* expr: NULL_CONST  */
#line 145 "exprparse.y"
                                        { (yyval.expr) = make_null_constant(); }
#line 1474 "exprparse.c"
    break;

  case 37: /* expr: BOOLEAN_CONST  */
#line 146 "exprparse.y"
                                        { (yyval.expr) = make_boolean_constant((yyvsp[0].bval)); }
#line 1480 "exprparse.c"
    break;

  case 38: /* expr: INTEGER_CONST  */
#line 147 "exprparse.y"
                                        { (yyval.expr) = make_integer_constant((yyvsp[0].ival)); }
#line 1486 "exprparse.c"
    break;

  case 39: /* expr: DOUBLE_CONST  */
#line 148 "exprparse.y"
                                        { (yyval.expr) = make_double_constant((yyvsp[0].dval)); }
#line 1492 "exprparse.c"
    break;

  case 40: /* expr: VARIABLE  */
#line 150 "exprparse.y"
                                                { (yyval.expr) = make_variable((yyvsp[0].str)); }
#line 1498 "exprparse.c"
    break;

  case 41: /* expr: function '(' elist ')'  */
#line 151 "exprparse.y"
                                 { (yyval.expr) = make_func(yyscanner, (yyvsp[-3].ival), (yyvsp[-1].elist)); }
#line 1504 "exprparse.c"
    break;

  case 42: /* expr: case_control  */
#line 152 "exprparse.y"
                                        { (yyval.expr) = (yyvsp[0].expr); }
#line 1510 "exprparse.c"
    break;

  case 43: /* when_then_list: when_then_list WHEN_KW expr THEN_KW expr  */
#line 156 "exprparse.y"
                                                   { (yyval.elist) = make_elist((yyvsp[0].expr), make_elist((yyvsp[-2].expr), (yyvsp[-4].elist))); }
#line 1516 "exprparse.c"
    break;

  case 44: /* when_then_list: WHEN_KW expr THEN_KW expr  */
#line 157 "exprparse.y"
                                    { (yyval.elist) = make_elist((yyvsp[0].expr), make_elist((yyvsp[-2].expr), NULL)); }
#line 1522 "exprparse.c"
    break;

  case 45: /* case_control: CASE_KW when_then_list END_KW  */
#line 160 "exprparse.y"
                                        { (yyval.expr) = make_case(yyscanner, (yyvsp[-1].elist), make_null_constant()); }
#line 1528 "exprparse.c"
    break;

  case 46: /* case_control: CASE_KW when_then_list ELSE_KW expr END_KW  */
#line 161 "exprparse.y"
                                                     { (yyval.expr) = make_case(yyscanner, (yyvsp[-3].elist), (yyvsp[-1].expr)); }
#line 1534 "exprparse.c"
    break;

  case 47: /* function: FUNCTION  */
#line 163 "exprparse.y"
                                        { (yyval.ival) = find_func(yyscanner, (yyvsp[0].str)); pg_free((yyvsp[0].str)); }
#line 1540 "exprparse.c"
    break;


#line 1544 "exprparse.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (yyscanner, YY_("syntax error"));
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, yyscanner);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yyscanner);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (yyscanner, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, yyscanner);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yyscanner);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 166 "exprparse.y"


static PgBenchExpr *
make_null_constant(void)
{
	PgBenchExpr *expr = pg_malloc(sizeof(PgBenchExpr));

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_NULL;
	expr->u.constant.u.ival = 0;
	return expr;
}

static PgBenchExpr *
make_integer_constant(int64 ival)
{
	PgBenchExpr *expr = pg_malloc(sizeof(PgBenchExpr));

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_INT;
	expr->u.constant.u.ival = ival;
	return expr;
}

static PgBenchExpr *
make_double_constant(double dval)
{
	PgBenchExpr *expr = pg_malloc(sizeof(PgBenchExpr));

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_DOUBLE;
	expr->u.constant.u.dval = dval;
	return expr;
}

static PgBenchExpr *
make_boolean_constant(bool bval)
{
	PgBenchExpr *expr = pg_malloc(sizeof(PgBenchExpr));

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_BOOLEAN;
	expr->u.constant.u.bval = bval;
	return expr;
}

static PgBenchExpr *
make_variable(char *varname)
{
	PgBenchExpr *expr = pg_malloc(sizeof(PgBenchExpr));

	expr->etype = ENODE_VARIABLE;
	expr->u.variable.varname = varname;
	return expr;
}

/* binary operators */
static PgBenchExpr *
make_op(yyscan_t yyscanner, const char *operator,
		PgBenchExpr *lexpr, PgBenchExpr *rexpr)
{
	return make_func(yyscanner, find_func(yyscanner, operator),
					 make_elist(rexpr, make_elist(lexpr, NULL)));
}

/* unary operator */
static PgBenchExpr *
make_uop(yyscan_t yyscanner, const char *operator, PgBenchExpr *expr)
{
	return make_func(yyscanner, find_func(yyscanner, operator), make_elist(expr, NULL));
}

/*
 * List of available functions:
 * - fname: function name, "!..." for special internal functions
 * - nargs: number of arguments. Special cases:
 *			- PGBENCH_NARGS_VARIABLE is a special value for least & greatest
 *			  meaning #args >= 1;
 *			- PGBENCH_NARGS_CASE is for the "CASE WHEN ..." function, which
 *			  has #args >= 3 and odd;
 *			- PGBENCH_NARGS_HASH is for hash functions, which have one required
 *			  and one optional argument;
 * - tag: function identifier from PgBenchFunction enum
 */
static const struct
{
	const char *fname;
	int			nargs;
	PgBenchFunction tag;
}	PGBENCH_FUNCTIONS[] =
{
	/* parsed as operators, executed as functions */
	{
		"+", 2, PGBENCH_ADD
	},
	{
		"-", 2, PGBENCH_SUB
	},
	{
		"*", 2, PGBENCH_MUL
	},
	{
		"/", 2, PGBENCH_DIV
	},
	{
		"mod", 2, PGBENCH_MOD
	},
	/* actual functions */
	{
		"abs", 1, PGBENCH_ABS
	},
	{
		"least", PGBENCH_NARGS_VARIABLE, PGBENCH_LEAST
	},
	{
		"greatest", PGBENCH_NARGS_VARIABLE, PGBENCH_GREATEST
	},
	{
		"debug", 1, PGBENCH_DEBUG
	},
	{
		"pi", 0, PGBENCH_PI
	},
	{
		"sqrt", 1, PGBENCH_SQRT
	},
	{
		"ln", 1, PGBENCH_LN
	},
	{
		"exp", 1, PGBENCH_EXP
	},
	{
		"int", 1, PGBENCH_INT
	},
	{
		"double", 1, PGBENCH_DOUBLE
	},
	{
		"random", 2, PGBENCH_RANDOM
	},
	{
		"random_gaussian", 3, PGBENCH_RANDOM_GAUSSIAN
	},
	{
		"random_exponential", 3, PGBENCH_RANDOM_EXPONENTIAL
	},
	{
		"random_zipfian", 3, PGBENCH_RANDOM_ZIPFIAN
	},
	{
		"pow", 2, PGBENCH_POW
	},
	{
		"power", 2, PGBENCH_POW
	},
	/* logical operators */
	{
		"!and", 2, PGBENCH_AND
	},
	{
		"!or", 2, PGBENCH_OR
	},
	{
		"!not", 1, PGBENCH_NOT
	},
	/* bitwise integer operators */
	{
		"&", 2, PGBENCH_BITAND
	},
	{
		"|", 2, PGBENCH_BITOR
	},
	{
		"#", 2, PGBENCH_BITXOR
	},
	{
		"<<", 2, PGBENCH_LSHIFT
	},
	{
		">>", 2, PGBENCH_RSHIFT
	},
	/* comparison operators */
	{
		"=", 2, PGBENCH_EQ
	},
	{
		"<>", 2, PGBENCH_NE
	},
	{
		"<=", 2, PGBENCH_LE
	},
	{
		"<", 2, PGBENCH_LT
	},
	{
		"!is", 2, PGBENCH_IS
	},
	/* "case when ... then ... else ... end" construction */
	{
		"!case_end", PGBENCH_NARGS_CASE, PGBENCH_CASE
	},
	{
		"hash", PGBENCH_NARGS_HASH, PGBENCH_HASH_MURMUR2
	},
	{
		"hash_murmur2", PGBENCH_NARGS_HASH, PGBENCH_HASH_MURMUR2
	},
	{
		"hash_fnv1a", PGBENCH_NARGS_HASH, PGBENCH_HASH_FNV1A
	},
	{
		"permute", PGBENCH_NARGS_PERMUTE, PGBENCH_PERMUTE
	},
	/* keep as last array element */
	{
		NULL, 0, 0
	}
};

/*
 * Find a function from its name
 *
 * return the index of the function from the PGBENCH_FUNCTIONS array
 * or fail if the function is unknown.
 */
static int
find_func(yyscan_t yyscanner, const char *fname)
{
	int			i = 0;

	while (PGBENCH_FUNCTIONS[i].fname)
	{
		if (pg_strcasecmp(fname, PGBENCH_FUNCTIONS[i].fname) == 0)
			return i;
		i++;
	}

	expr_yyerror_more(yyscanner, "unexpected function name", fname);

	/* not reached */
	return -1;
}

/* Expression linked list builder */
static PgBenchExprList *
make_elist(PgBenchExpr *expr, PgBenchExprList *list)
{
	PgBenchExprLink *cons;

	if (list == NULL)
	{
		list = pg_malloc(sizeof(PgBenchExprList));
		list->head = NULL;
		list->tail = NULL;
	}

	cons = pg_malloc(sizeof(PgBenchExprLink));
	cons->expr = expr;
	cons->next = NULL;

	if (list->head == NULL)
		list->head = cons;
	else
		list->tail->next = cons;

	list->tail = cons;

	return list;
}

/* Return the length of an expression list */
static int
elist_length(PgBenchExprList *list)
{
	PgBenchExprLink *link = list != NULL ? list->head : NULL;
	int			len = 0;

	for (; link != NULL; link = link->next)
		len++;

	return len;
}

/* Build function call expression */
static PgBenchExpr *
make_func(yyscan_t yyscanner, int fnumber, PgBenchExprList *args)
{
	int len = elist_length(args);

	PgBenchExpr *expr = pg_malloc(sizeof(PgBenchExpr));

	Assert(fnumber >= 0);

	/* validate arguments number including few special cases */
	switch (PGBENCH_FUNCTIONS[fnumber].nargs)
	{
		/* check at least one arg for least & greatest */
		case PGBENCH_NARGS_VARIABLE:
			if (len == 0)
				expr_yyerror_more(yyscanner, "at least one argument expected",
								  PGBENCH_FUNCTIONS[fnumber].fname);
			break;

		/* case (when ... then ...)+ (else ...)? end */
		case PGBENCH_NARGS_CASE:
			/* 'else' branch is always present, but could be a NULL-constant */
			if (len < 3 || len % 2 != 1)
				expr_yyerror_more(yyscanner,
								  "odd and >= 3 number of arguments expected",
								  "case control structure");
			break;

		/* hash functions with optional seed argument */
		case PGBENCH_NARGS_HASH:
			if (len < 1 || len > 2)
				expr_yyerror_more(yyscanner, "unexpected number of arguments",
								  PGBENCH_FUNCTIONS[fnumber].fname);

			if (len == 1)
			{
				PgBenchExpr *var = make_variable("default_seed");
				args = make_elist(var, args);
			}
			break;

		/* pseudorandom permutation function with optional seed argument */
		case PGBENCH_NARGS_PERMUTE:
			if (len < 2 || len > 3)
				expr_yyerror_more(yyscanner, "unexpected number of arguments",
								  PGBENCH_FUNCTIONS[fnumber].fname);

			if (len == 2)
			{
				PgBenchExpr *var = make_variable("default_seed");
				args = make_elist(var, args);
			}
			break;

		/* common case: positive arguments number */
		default:
			Assert(PGBENCH_FUNCTIONS[fnumber].nargs >= 0);

			if (PGBENCH_FUNCTIONS[fnumber].nargs != len)
				expr_yyerror_more(yyscanner, "unexpected number of arguments",
								  PGBENCH_FUNCTIONS[fnumber].fname);
	}

	expr->etype = ENODE_FUNCTION;
	expr->u.function.function = PGBENCH_FUNCTIONS[fnumber].tag;

	/* only the link is used, the head/tail is not useful anymore */
	expr->u.function.args = args != NULL ? args->head : NULL;
	if (args)
		pg_free(args);

	return expr;
}

static PgBenchExpr *
make_case(yyscan_t yyscanner, PgBenchExprList *when_then_list, PgBenchExpr *else_part)
{
	return make_func(yyscanner,
					 find_func(yyscanner, "!case_end"),
					 make_elist(else_part, when_then_list));
}
