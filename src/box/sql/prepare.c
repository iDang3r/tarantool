/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * This file contains the implementation of the sql_prepare()
 * interface, and routines that contribute to loading the database schema
 * from disk.
 */
#include "sqlInt.h"
#include "tarantoolInt.h"
#include "box/space.h"
#include "box/session.h"

/*
 * Compile the UTF-8 encoded SQL statement zSql into a statement handle.
 */
static int
sqlPrepare(sql * db,	/* Database handle. */
	       const char *zSql,	/* UTF-8 encoded SQL statement. */
	       int nBytes,	/* Length of zSql in bytes. */
	       Vdbe * pReprepare,	/* VM being reprepared */
	       sql_stmt ** ppStmt,	/* OUT: A pointer to the prepared statement */
	       const char **pzTail	/* OUT: End of parsed string */
    )
{
	int rc = 0;	/* Result code */
	Parse sParse;		/* Parsing context */
	sql_parser_create(&sParse, db, current_session()->sql_flags);
	sParse.pReprepare = pReprepare;
	*ppStmt = NULL;
	/* assert( !db->mallocFailed ); // not true with SQL_USE_ALLOCA */

	/* Check to verify that it is possible to get a read lock on all
	 * database schemas.  The inability to get a read lock indicates that
	 * some other database connection is holding a write-lock, which in
	 * turn means that the other connection has made uncommitted changes
	 * to the schema.
	 *
	 * Were we to proceed and prepare the statement against the uncommitted
	 * schema changes and if those schema changes are subsequently rolled
	 * back and different changes are made in their place, then when this
	 * prepared statement goes to run the schema cookie would fail to detect
	 * the schema change.  Disaster would follow.
	 *
	 * Note that setting READ_UNCOMMITTED overrides most lock detection,
	 * but it does *not* override schema lock detection, so this all still
	 * works even if READ_UNCOMMITTED is set.
	 */
	if (nBytes >= 0 && (nBytes == 0 || zSql[nBytes - 1] != 0)) {
		char *zSqlCopy;
		int mxLen = db->aLimit[SQL_LIMIT_SQL_LENGTH];
		testcase(nBytes == mxLen);
		testcase(nBytes == mxLen + 1);
		if (nBytes > mxLen) {
			diag_set(ClientError, ER_SQL_PARSER_LIMIT,
				 "SQL command length", nBytes, mxLen);
			rc = -1;
			goto end_prepare;
		}
		zSqlCopy = sqlDbStrNDup(db, zSql, nBytes);
		if (zSqlCopy) {
			sqlRunParser(&sParse, zSqlCopy);
			sParse.zTail = &zSql[sParse.zTail - zSqlCopy];
			sqlDbFree(db, zSqlCopy);
		} else {
			sParse.zTail = &zSql[nBytes];
		}
	} else {
		sqlRunParser(&sParse, zSql);
	}
	assert(0 == sParse.nQueryLoop);

	if (db->mallocFailed)
		sParse.is_aborted = true;
	if (pzTail) {
		*pzTail = sParse.zTail;
	}
	if (sParse.is_aborted)
		rc = -1;

	if (rc == 0 && sParse.pVdbe != NULL && sParse.explain) {
		static const char *const azColName[] = {
			/*  0 */ "addr",
			/*  1 */ "INTEGER",
			/*  2 */ "opcode",
			/*  3 */ "TEXT",
			/*  4 */ "p1",
			/*  5 */ "INTEGER",
			/*  6 */ "p2",
			/*  7 */ "INTEGER",
			/*  8 */ "p3",
			/*  9 */ "INTEGER",
			/* 10 */ "p4",
			/* 11 */ "TEXT",
			/* 12 */ "p5",
			/* 13 */ "TEXT",
			/* 14 */ "comment",
			/* 15 */ "TEXT",
			/* 16 */ "selectid",
			/* 17 */ "INTEGER",
			/* 18 */ "order",
			/* 19 */ "INTEGER",
			/* 20 */ "from",
			/* 21 */ "INTEGER",
			/* 22 */ "detail",
			/* 23 */ "TEXT",
		};

		int name_first, name_count;
		if (sParse.explain == 2) {
			name_first = 16;
			name_count = 4;
		} else {
			name_first = 0;
			name_count = 8;
		}
		sqlVdbeSetNumCols(sParse.pVdbe, name_count);
		for (int i = 0; i < name_count; i++) {
			int name_index = 2 * i + name_first;
			sqlVdbeSetColName(sParse.pVdbe, i, COLNAME_NAME,
					  azColName[name_index], SQL_STATIC);
			sqlVdbeSetColName(sParse.pVdbe, i, COLNAME_DECLTYPE,
					  azColName[name_index + 1],
					  SQL_STATIC);
		}
	}

	if (db->init.busy == 0) {
		Vdbe *pVdbe = sParse.pVdbe;
		sqlVdbeSetSql(pVdbe, zSql, (int)(sParse.zTail - zSql));
	}
	if (sParse.pVdbe != NULL && (rc != 0 || db->mallocFailed)) {
		sqlVdbeFinalize(sParse.pVdbe);
		assert(!(*ppStmt));
	} else {
		*ppStmt = (sql_stmt *) sParse.pVdbe;
	}

	/* Delete any TriggerPrg structures allocated while parsing this statement. */
	while (sParse.pTriggerPrg) {
		TriggerPrg *pT = sParse.pTriggerPrg;
		sParse.pTriggerPrg = pT->pNext;
		sqlDbFree(db, pT);
	}

 end_prepare:

	sql_parser_destroy(&sParse);
	return rc;
}

/*
 * Rerun the compilation of a statement after a schema change.
 */
int
sqlReprepare(Vdbe * p)
{
	sql_stmt *pNew;
	const char *zSql;
	sql *db;

	zSql = sql_sql((sql_stmt *) p);
	assert(zSql != 0);	/* Reprepare only called for prepare_v2() statements */
	db = sqlVdbeDb(p);
	if (sqlPrepare(db, zSql, -1, p, &pNew, 0) != 0) {
		assert(pNew == 0);
		return -1;
	}
	assert(pNew != 0);
	sqlVdbeSwap((Vdbe *) pNew, p);
	sqlTransferBindings(pNew, (sql_stmt *) p);
	sqlVdbeResetStepResult((Vdbe *) pNew);
	sqlVdbeFinalize((Vdbe *) pNew);
	return 0;
}

int
sql_prepare(const char *sql, int length, struct sql_stmt **stmt,
	    const char **sql_tail)
{
	int rc = sqlPrepare(sql_get(), sql, length, 0, stmt, sql_tail);
	assert(rc == 0 || stmt == NULL || *stmt == NULL);
	return rc;
}

void
sql_parser_create(struct Parse *parser, struct sql *db, uint32_t sql_flags)
{
	memset(parser, 0, sizeof(struct Parse));
	parser->db = db;
	parser->sql_flags = sql_flags;
	region_create(&parser->region, &cord()->slabc);
}

void
sql_parser_destroy(Parse *parser)
{
	assert(parser != NULL);
	assert(!parser->parse_only || parser->pVdbe == NULL);
	sql *db = parser->db;
	sqlDbFree(db, parser->aLabel);
	sql_expr_list_delete(db, parser->pConstExpr);
	create_table_def_destroy(&parser->create_table_def);
	if (db != NULL) {
		assert(db->lookaside.bDisable >=
		       parser->disableLookaside);
		db->lookaside.bDisable -= parser->disableLookaside;
	}
	parser->disableLookaside = 0;
	switch (parser->parsed_ast_type) {
	case AST_TYPE_SELECT:
		sql_select_delete(db, parser->parsed_ast.select);
		break;
	case AST_TYPE_EXPR:
		sql_expr_delete(db, parser->parsed_ast.expr, false);
		break;
	case AST_TYPE_TRIGGER:
		sql_trigger_delete(db, parser->parsed_ast.trigger);
		break;
	default:
		assert(parser->parsed_ast_type == AST_TYPE_UNDEFINED);
	}
	region_destroy(&parser->region);
}
