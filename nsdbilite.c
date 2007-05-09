/*
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://mozilla.org/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) 2006 Stephen Deasey <sdeasey@gmail.com>
 *
 * Alternatively, the contents of this file may be used under the terms
 * of the GNU General Public License (the "GPL"), in which case the
 * provisions of GPL are applicable instead of those above.  If you wish
 * to allow use of your version of this file only under the terms of the
 * GPL and not to allow others to use your version of this file under the
 * License, indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the GPL.
 * If you do not delete the provisions above, a recipient may use your
 * version of this file under either the License or the GPL.
 */

/*
 * nsdbilite.c --
 *
 *      A NaviServer nsdbi database driver for the sqlite3 database.
 */

#include <nsdbidrv.h>
#include <sqlite3.h>


NS_EXPORT int Ns_ModuleVersion = 1;


/*
 * The following structure manages per-pool configuration.
 */

typedef struct LiteConfig {
    CONST char  *module;
    CONST char  *datasource; /* The file containing the database. */
    int          retries;    /* Number of times to retry a busy op. */
} LiteConfig;

/*
 * The following struct manages a single db handle.
 */

typedef struct LiteHandle {
    LiteConfig  *ltCfg;
    sqlite3     *conn;
    Dbi_Handle  *handle;
} LiteHandle;

/*
 * Static functions defined in this file.
 */

static Dbi_OpenProc         Open;
static Dbi_CloseProc        Close;
static Dbi_ConnectedProc    Connected;
static Dbi_BindVarProc      Bind;
static Dbi_PrepareProc      Prepare;
static Dbi_PrepareCloseProc PrepareClose;
static Dbi_ExecProc         Exec;
static Dbi_NextValueProc    NextValue;
static Dbi_ColumnNameProc   ColumnName;
static Dbi_TransactionProc  Transaction;
static Dbi_FlushProc        Flush;
static Dbi_ResetProc        Reset;

static void ReportException(LiteHandle *ltHandle);


/*
 * Static variables defined in this file.
 */

static Dbi_DriverProc procs[] = {
    {Dbi_OpenProcId,         Open},
    {Dbi_CloseProcId,        Close},
    {Dbi_ConnectedProcId,    Connected},
    {Dbi_BindVarProcId,      Bind},
    {Dbi_PrepareProcId,      Prepare},
    {Dbi_PrepareCloseProcId, PrepareClose},
    {Dbi_ExecProcId,         Exec},
    {Dbi_NextValueProcId,    NextValue},
    {Dbi_ColumnNameProcId,   ColumnName},
    {Dbi_TransactionProcId,  Transaction},
    {Dbi_FlushProcId,        Flush},
    {Dbi_ResetProcId,        Reset},
    {0, NULL}
};


/*
 *----------------------------------------------------------------------
 *
 * Dbi_ModuleInit --
 *
 *      Register the driver callbacks.
 *
 * Results:
 *      NS_OK / NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

NS_EXPORT int
Ns_ModuleInit(CONST char *server, CONST char *module)
{
    LiteConfig *ltCfg;
    char       *path;
    CONST char *drivername = "sqlite";
    CONST char *database   = "sqlite3";

    path = Ns_ConfigGetPath(server, module, NULL);

    ltCfg = ns_calloc(1, sizeof(LiteConfig));
    ltCfg->module     = ns_strdup(module);
    ltCfg->datasource = Ns_ConfigString(path, "datasource", ":memory:");
    ltCfg->retries    = Ns_ConfigIntRange(path, "sqlitebusyretries", 100, 0, INT_MAX);

    return Dbi_RegisterDriver(server, module,
                              drivername, database,
                              procs, ltCfg);
}


/*
 *----------------------------------------------------------------------
 *
 * Open --
 *
 *      Open a connection to the configured database.
 *
 * Results:
 *      NS_OK / NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Open(ClientData configData, Dbi_Handle *handle)
{
    LiteConfig *ltCfg = configData;
    LiteHandle *ltHandle;
    sqlite3    *conn;

    if (sqlite3_open(ltCfg->datasource, &conn) != SQLITE_OK) {
        Dbi_SetException(handle, "SQLIT", sqlite3_errmsg(conn));
        (void) sqlite3_close(conn);
        return NS_ERROR;
    }
    ltHandle = ns_calloc(1, sizeof(LiteHandle));
    ltHandle->ltCfg = ltCfg;
    ltHandle->conn = conn;
    ltHandle->handle = handle;
    handle->driverData = ltHandle;

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Close --
 *
 *      Close a handle to the database.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
Close(Dbi_Handle *handle)
{
    LiteHandle *ltHandle = handle->driverData;

    assert(ltHandle);

    if (sqlite3_close(ltHandle->conn) != SQLITE_OK) {
        Ns_Log(Error, "dbilite: error closing db handle: %s",
               sqlite3_errmsg(ltHandle->conn));
    }
    ns_free(ltHandle);
    handle->driverData = NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * Connected --
 *
 *      Is the given handle currently connected?
 *
 * Results:
 *      NS_TRUE or NS_FALSE.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Connected(Dbi_Handle *handle)
{
    LiteHandle *ltHandle = handle->driverData;

    if (ltHandle != NULL) {
        return NS_TRUE;
    }
    return NS_FALSE;
}


/*
 *----------------------------------------------------------------------
 *
 * Bind --
 *
 *      Even though sqlite handles :var notation natively, use simple
 *      '?' notation as it's easier to handle in the driver.
 *
 * Results:
 *      NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
Bind(Ns_DString *ds, CONST char *name, int bindIdx)
{
    Ns_DStringAppend(ds, "?");
}


/*
 *----------------------------------------------------------------------
 *
 * Prepare --
 *
 *      Prepare a statement if one doesn't already exist for this query..
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Prepare(Dbi_Handle *handle, Dbi_Statement *stmt,
        unsigned int *numVarsPtr, unsigned int *numColsPtr)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st;
    const char   *tail;

    if (stmt->driverData == NULL) {
        if (sqlite3_prepare(ltHandle->conn, stmt->sql, stmt->length, &st, &tail)
                != SQLITE_OK) {
            ReportException(ltHandle);
            return NS_ERROR;
        }
        *numVarsPtr = sqlite3_bind_parameter_count(st);
        *numColsPtr = sqlite3_column_count(st);
        stmt->driverData = st;
    }

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * PrepareClose --
 *
 *      Finalize a prepared statement.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static void
PrepareClose(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    sqlite3_stmt *st = stmt->driverData;

    assert(st);

    (void) sqlite3_finalize(st);
    stmt->driverData = NULL;
}


/*
 *----------------------------------------------------------------------
 *
 * Exec --
 *
 *      Bind values to the prepared statement.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      The state machine is run for DML statements.
 *
 *----------------------------------------------------------------------
 */

static int
Exec(Dbi_Handle *handle, Dbi_Statement *stmt,
     CONST char **values, unsigned int *lengths, unsigned int nvalues)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st = stmt->driverData;
    const char   *tail;
    int           rc, i, retries, status;

    /*
     * NB: sqlite indexes variables from 1, nsdbi from 0.
     */

    for (i = 0; i < nvalues; i++) {
        if (sqlite3_bind_text(st, i+1, values[i], lengths[i],
                              SQLITE_STATIC) != SQLITE_OK) {
            ReportException(ltHandle);
            return NS_ERROR;
        }
    }

    /*
     * Step the state machine for DML commands as callers are not
     * expecting any rows and will not call NextValue.
     */

    if (Dbi_NumColumns(handle)) {
        return NS_OK;
    }

    retries = ltHandle->ltCfg->retries;
    status = NS_ERROR;

    do {
        if ((rc = sqlite3_step(st)) == SQLITE_BUSY) {
            Ns_ThreadYield();
        }
    } while (rc == SQLITE_BUSY && --retries > 0);

    switch (rc) {

    case SQLITE_BUSY:
        Dbi_SetException(handle, "SQLIT", "dbisqlite: error executing statement: "
            "database still busy after %d retries.", ltHandle->ltCfg->retries);
        break;

    case SQLITE_ROW:
        Dbi_SetException(handle, "SQLIT",
            "statement with 0 columns returned rows");
        break;

    case SQLITE_ERROR:
        if (sqlite3_finalize(st) == SQLITE_SCHEMA) {
            stmt->driverData = NULL;
            if (sqlite3_prepare(ltHandle->conn, stmt->sql, stmt->length, &st, &tail)
                    != SQLITE_OK) {
                Dbi_SetException(handle, "SQLIT",
                                 "schema change, reprepare failed: %s",
                                 sqlite3_errmsg(ltHandle->conn));
                /* FIXME: what now? statement invalid. */
                break;
            }
            stmt->driverData = st;
        } else {
            ReportException(ltHandle);
        }
        break;

    case SQLITE_MISUSE:
        Dbi_SetException(handle, "SQLIT", "dbilite: Exec: Bug: SQLITE_MISUSE");
        break;

    case SQLITE_DONE:
        status = NS_OK;
        break;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * NextValue --
 *
 *      Fetch the value of the given row and column.
 *
 * Results:
 *      DBI_VALUE, DBI_DONE or DBI_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
NextValue(Dbi_Handle *handle, Dbi_Statement *stmt,
          unsigned int colIdx, unsigned int rowIdx, Dbi_Value *value)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st       = stmt->driverData;
    const char   *tail;
    int           rc;
    int           retries = ltHandle->ltCfg->retries;

    if (colIdx == 0) {

        /*
         * Fetch a new row.
         */

        do {
            if ((rc = sqlite3_step(st)) == SQLITE_BUSY) {
                Ns_ThreadYield();
            }
        } while (rc == SQLITE_BUSY && --retries > 0);

        switch (rc) {

        case SQLITE_BUSY:
            Dbi_SetException(handle, "SQLIT", "dbilite: error executing statement: "
                             "database still busy after %d retries.",
                             ltHandle->ltCfg->retries);
            return DBI_ERROR;

        case SQLITE_ERROR:
            if (sqlite3_finalize(st) == SQLITE_SCHEMA) {
                stmt->driverData = NULL;
                if (sqlite3_prepare(ltHandle->conn, stmt->sql, stmt->length, &st, &tail)
                        != SQLITE_OK) {
                    Dbi_SetException(handle, "SQLIT",
                                     "schema change, reprepare failed: %s",
                                     sqlite3_errmsg(ltHandle->conn));
                    /* FIXME: what now? statement invalid. */
                    return DBI_ERROR;
                }
                stmt->driverData = st;
            } else {
                ReportException(ltHandle);
            }
            return DBI_ERROR;

        case SQLITE_MISUSE:
            Dbi_SetException(handle, "SQLIT", "dbilite: NextValue: Bug: SQLITE_MISUSE");
            return DBI_ERROR;

        case SQLITE_DONE:
            return DBI_DONE;

        case SQLITE_ROW:
            /* NB: handled below. */
            break;
        }
    }

    /*
     * Handle data for current valid row.
     */

    switch (sqlite3_column_type(st, (int) colIdx)) {

    case SQLITE_NULL:
        value->data   = NULL;
        value->length = 0;
        value->binary = 0;
        break;

    case SQLITE_BLOB:
        value->data   = sqlite3_column_blob(st, (int) colIdx);
        value->length = sqlite3_column_bytes(st, (int) colIdx);
        value->binary = 1;
        break;

    default:
        /* SQLITE_TEXT (and all other types, which we return as text) */
        value->data   = sqlite3_column_text(st, (int) colIdx);
        value->length = sqlite3_column_bytes(st, (int) colIdx);
        value->binary = 0;
    }

    return DBI_VALUE;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnName --
 *
 *      Fetch the UTF8 column name for the current statement.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
ColumnName(Dbi_Handle *handle, Dbi_Statement *stmt,
           unsigned int index, CONST char **column)
{
    sqlite3_stmt *st = stmt->driverData;

    *column = sqlite3_column_name(st, index);
    if (*column == NULL) {
        return NS_ERROR;
    }
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Transaction --
 *
 *      Begin, commit and rollback transactions.
 *
 * Results:
 *      NS_OK or NS_ERROR
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Transaction(Dbi_Handle *handle, unsigned int depth,
            Dbi_TransactionCmd cmd, Dbi_Isolation isolation)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st;
    CONST char   *sql;
    int           status;

    if (depth) {
        Dbi_SetException(handle, "SQLIT",
            "dbilite does not support nested transactions");
        return NS_ERROR;
    }

    switch(cmd) {
    case Dbi_TransactionBegin:
        sql = (isolation == Dbi_Serializable)
            ? "begin exclusive" : "begin";
        break;
    case Dbi_TransactionCommit:
        sql = "commit";
        break;
    case Dbi_TransactionRollback:
        sql = "rollback";
        break;
    default:
        Ns_Fatal("dbilite: Transaction: unhandled cmd: %d", (int) cmd);
    }

    status = NS_OK;

    if (sqlite3_prepare(ltHandle->conn, sql, -1, &st, NULL) != SQLITE_OK
            || sqlite3_step(st) != SQLITE_DONE) {
        ReportException(ltHandle);
        status = NS_ERROR;
    }
    (void) sqlite3_finalize(st);

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * Flush --
 *
 *      Reset the statement state machine ready to be executed again.
 *
 * Results:
 *      NS_OK or NS_ERROR.
 *
 * Side effects:
 *      Also clears the variable bindings as nsdbi has no way to
 *      selectively re-bind indiviual variables.
 *
 *----------------------------------------------------------------------
 */

static int
Flush(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st       = stmt->driverData;

    assert(st);

    if (sqlite3_reset(st) != SQLITE_OK) {
        ReportException(ltHandle);
        return NS_ERROR;
    }
/*     if (sqlite3_clear_bindings(st) != SQLITE_OK) { */
/*         Dbi_SetException(handle, "SQLIT", sqlite3_errmsg(sq->conn)); */
/*         return NS_ERROR; */
/*     } */
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Reset --
 *
 *      Nothing to do?
 *
 * Results:
 *      Always NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
Reset(Dbi_Handle *handle)
{
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ReportException --
 *
 *      Set the dbi handle excetion to the latest error message.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      Abort if out of memory.
 *
 *----------------------------------------------------------------------
 */

static void
ReportException(LiteHandle *ltHandle)
{
    if (sqlite3_errcode(ltHandle->conn) == SQLITE_NOMEM) {
        Ns_Fatal("dbilite: SQLITE_NOMEM: %s",
                 sqlite3_errmsg(ltHandle->conn));
    }
    Dbi_SetException(ltHandle->handle, "SQLIT", sqlite3_errmsg(ltHandle->conn));
}
