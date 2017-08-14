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
static Dbi_NextRowProc      NextRow;
static Dbi_ColumnLengthProc ColumnLength;
static Dbi_ColumnValueProc  ColumnValue;
static Dbi_ColumnNameProc   ColumnName;
static Dbi_TransactionProc  Transaction;
static Dbi_FlushProc        Flush;
static Dbi_ResetProc        Reset;

NS_EXPORT Ns_ModuleInitProc Ns_ModuleInit;

static int Step(Dbi_Handle *handle, Dbi_Statement *stmt);
static void ReportException(LiteHandle *ltHandle);


/*
 * Static variables defined in this file.
 */

static Dbi_DriverProc procs[] = {
    {Dbi_OpenProcId,         (Ns_Callback *)Open},
    {Dbi_CloseProcId,        (Ns_Callback *)Close},
    {Dbi_ConnectedProcId,    (Ns_Callback *)Connected},
    {Dbi_BindVarProcId,      (Ns_Callback *)Bind},
    {Dbi_PrepareProcId,      (Ns_Callback *)Prepare},
    {Dbi_PrepareCloseProcId, (Ns_Callback *)PrepareClose},
    {Dbi_ExecProcId,         (Ns_Callback *)Exec},
    {Dbi_NextRowProcId,      (Ns_Callback *)NextRow},
    {Dbi_ColumnLengthProcId, (Ns_Callback *)ColumnLength},
    {Dbi_ColumnValueProcId,  (Ns_Callback *)ColumnValue},
    {Dbi_ColumnNameProcId,   (Ns_Callback *)ColumnName},
    {Dbi_TransactionProcId,  (Ns_Callback *)Transaction},
    {Dbi_FlushProcId,        (Ns_Callback *)Flush},
    {Dbi_ResetProcId,        (Ns_Callback *)Reset},
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
    const char *path;
    CONST char *drivername = "sqlite";
    CONST char *database   = "sqlite3";

    Dbi_LibInit();

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
        Dbi_SetException(handle, "SQLIT", "%s", sqlite3_errmsg(conn));
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
Bind(Ns_DString *ds, const char *UNUSED(name), int UNUSED(bindIdx))
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
        if (sqlite3_prepare_v2(ltHandle->conn, stmt->sql, stmt->length, &st, &tail)
                != SQLITE_OK) {
            ReportException(ltHandle);
            return NS_ERROR;
        }
        *numVarsPtr = (unsigned int)sqlite3_bind_parameter_count(st);
        *numColsPtr = (unsigned int)sqlite3_column_count(st);
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
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st = stmt->driverData;

    assert(st);

    if (sqlite3_finalize(st) != SQLITE_OK) {
        ReportException(ltHandle);
    }
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
     Dbi_Value *values, unsigned int numValues)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st = stmt->driverData;
    int           rc;
    unsigned int  i;

    assert(st);

    /*
     * NB: sqlite indexes variables from 1, nsdbi from 0.
     */

    for (i = 0u; i < numValues; i++) {
        if (values[i].data == NULL) {
            rc = sqlite3_bind_null(st, (int)i+1);
        } else if (values[i].binary) {
            rc = sqlite3_bind_blob(st, (int)i+1, values[i].data, (int)values[i].length,
                                   SQLITE_STATIC);
        } else {
            rc = sqlite3_bind_text(st, (int)i+1, values[i].data, (int)values[i].length,
                                   SQLITE_STATIC);
        }
        if (rc != SQLITE_OK) {
            ReportException(ltHandle);
            return NS_ERROR;
        }
    }

    if (Dbi_NumColumns(handle) > 0) {
        return NS_OK;
    }

    /*
     * Step the state machine for DML commands as callers are not
     * expecting any rows and will not call NextValue.
     */

    rc = Step(handle, stmt);

    if (rc == SQLITE_ROW) {
        Dbi_SetException(handle, "SQLIT",
            "dbilite: Exec: Bug: DML statement returned rows");
        return NS_ERROR;
    }

    return (rc == SQLITE_DONE) ? NS_OK : NS_ERROR;
}


/*
 *----------------------------------------------------------------------
 *
 * NextRow --
 *
 *      Fetch the next row of the result set.
 *
 * Results:
 *      NS_OK or NS_ERROR, endPtr set to 1 after last row fetched.
 *
 * Side effects:
 *      The fetch may be retried if busy; see: Step().
 *
 *----------------------------------------------------------------------
 */

static int
NextRow(Dbi_Handle *handle, Dbi_Statement *stmt, int *endPtr)
{
    int  status;

    switch (Step(handle, stmt)) {
    case SQLITE_ROW:
        status = NS_OK;
        break;
    case SQLITE_DONE:
        *endPtr = 1;
        status = NS_OK;
        break;
    case SQLITE_ERROR:
        status = NS_ERROR;
        break;
    }

    return status;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnLength --
 *
 *      Return the length of the column value and it's text/binary
 *      type after a NextRow(). Null values are 0 length.
 *
 * Results:
 *      NS_OK;
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
ColumnLength(Dbi_Handle *UNUSED(handle), Dbi_Statement *stmt, unsigned int index,
             size_t *lengthPtr, int *binaryPtr)
{
    sqlite3_stmt *st = stmt->driverData;

    *lengthPtr = (size_t)sqlite3_column_bytes(st, (int) index);
    *binaryPtr = (sqlite3_column_type(st, (int) index) == SQLITE_BLOB);

    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * ColumnValue --
 *
 *      Fetch the indicated value from the current row.
 *
 * Results:
 *      NS_OK.
 *
 * Side effects:
 *      None.
 *
 *----------------------------------------------------------------------
 */

static int
ColumnValue(Dbi_Handle *UNUSED(handle), Dbi_Statement *stmt, unsigned int index,
            char *value, size_t length)
{
    sqlite3_stmt *st = stmt->driverData;
    const char   *src;

    if (sqlite3_column_type(st, (int) index) == SQLITE_BLOB) {
        src = sqlite3_column_blob(st, (int) index);
    } else {
        src = (const char *) sqlite3_column_text(st, (int) index);
    }
    memcpy(value, src, MIN((int)length, sqlite3_column_bytes(st, (int) index)));

    return NS_OK;
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
ColumnName(Dbi_Handle *UNUSED(handle), Dbi_Statement *stmt,
           unsigned int index, const char **column)
{
    sqlite3_stmt *st = stmt->driverData;

    *column = sqlite3_column_name(st, (int)index);
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
/*         Dbi_SetException(handle, "SQLIT", "%s", sqlite3_errmsg(sq->conn)); */
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
Reset(Dbi_Handle *UNUSED(handle))
{
    return NS_OK;
}


/*
 *----------------------------------------------------------------------
 *
 * Step --
 *
 *      Step the sqlite state machine for a statement.
 *
 * Results:
 *      An sqlite result code: SQLITE_ROW, SQLITE_DONE, SQLITE_ERROR.
 *
 * Side effects:
 *      May retry the step if busy or recompile the statement on
 *      schema change.
 *
 *----------------------------------------------------------------------
 */

static int
Step(Dbi_Handle *handle, Dbi_Statement *stmt)
{
    LiteHandle   *ltHandle = handle->driverData;
    sqlite3_stmt *st = stmt->driverData;
    int           rc, retries;

    retries = ltHandle->ltCfg->retries;

    do {
        if ((rc = sqlite3_step(st)) == SQLITE_BUSY) {
            Ns_ThreadYield();
        }
    } while (rc == SQLITE_BUSY && --retries > 0);

    switch (rc) {

    case SQLITE_ROW:
    case SQLITE_DONE:
        /* NB: handled by caller. */
        break;

    case SQLITE_BUSY:
        Dbi_SetException(handle, "SQLIT", "dbisqlite: error executing statement: "
            "database still busy after %d retries.", ltHandle->ltCfg->retries);
        rc = SQLITE_ERROR;
        break;

    case SQLITE_MISUSE:
        Dbi_SetException(handle, "SQLIT", "dbilite: Bug: SQLITE_MISUSE");
        rc = SQLITE_ERROR;
        break;

    case SQLITE_ERROR:
    default:
        ReportException(ltHandle);
        rc = SQLITE_ERROR;
        break;
    }

    return rc;
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
    Dbi_SetException(ltHandle->handle, "SQLIT", "%s", sqlite3_errmsg(ltHandle->conn));
}

/*
 * Local Variables:
 * mode: c
 * c-basic-offset: 4
 * fill-column: 78
 * indent-tabs-mode: nil
 * End:
 */
