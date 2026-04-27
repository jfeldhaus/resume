/* dst.c */

/* This is the implementation for the DST library. */



/*

  The DST library is composed of the following groups of functions:

  1. Connection Management Functions
  2. Statement Management Functions
  3. Result Set and Data Transfer Functions
  4. Metadata Functions
  5. Oracle Metadata Functions
  6. TEST_DATA Data Source Management Functions
  7. Utility Functions

*/



/* ---------------------------------------------------------------- */
/* INCLUDES */

#include <common.h>


/* ---------------------------------------------------------------- */
/* DEFINES */


#define DST_MSSG_MALLOC_FAILURE "\n[DST_INTERNAL_ERROR_MALLOC_FAILED]\n"
#define DST_LINE_BUFFER_LENGTH 16384
#define DST_CONN_STR_OUT_LENGTH 2048
#define DST_MAX_AGENT_CONTROL_ATTEMPTS 5



#define DST_SQL_KEYWORDS "ABS ACTIVE ADDMONTHS ADMIN ASYNCHRONOUS AUTHID " \
  "AUTOREFRESH BIGINT BINARY BULK CACHE CACHEONLY CALL COMPRESS CONCAT CONFLICTS " \
  "CS CURRENT_SCHEMA CURRVAL CYCLE DATASTORE DATASTORE_OWNER DDL DEBUG DELETE_FT " \
  "DESTROY DISABLE DURABLE DURATION ELEMENT ENCRYPTED EVERY EXCLUDE EXIT EXTERNALLY " \
  "FAILTHRESHOLD FIRST FLUSH GETDATE HASH ID IDENTIFIED INCREMENT INCREMENTAL " \
  "INLINE INSERTONLY INSTANCE INSTR INTEGER LATENCY LENGTH LIMIT LOAD LOAD_FT LONG MASTER " \
  "MATERIALIZED MAXVALUE MILLISECONDS MINUTES MINVALUE MOD MODE MODIFY MULTI NAME " \
  "NEXTVAL NONDURABLE NUMTODSINTERVAL NUMTOYMINTERVAL NVARCHAR NVL OFF ORACLE PAGES " \
  "PAIR PAUSED PORT PRIMARY PRIVATE PROPAGATE PROPAGATOR PUBLIC PUBLICREAD PUBLICROW QUIT RC " \
  "READONLY RECEIPT REFRESH REFRESH_FT RELEASE RENAME REPLICATION REPORT REQUEST " \
  "REQUIRED RESUME RETURN ROW RR RTRIM RU SECONDS SELF SEQCACHE SEQUENCE SERVICES " \
  "STANDBY START STATE STOPPED STORE SUBSCRIBER SUBSTR SYNCHRONOUS SYSDATE SYSTEM " \
  "TIMEOUT TIMESTAMP TINYINT TO_CHAR TO_DATE TRAFFIC TRANSMIT TRUNCATE TWOSAFE " \
  "UNLOAD USERMANAGED VARBINARY WAIT WRITETHROUGH "



#define DST_OBJECT_TYPE_QUERY \
    /* TABLES */ \
    "SELECT " \
      "CASE " \
        "WHEN SYS25 = 136 OR SYS25 = 72 THEN 'TABLE_GLOBAL_TEMPORARY' " \
        "ELSE 'TABLE' " \
      "END AS TYPE, " \
      "TBLOWNER AS OWNER, " \
      "TBLNAME AS NAME, " \
      "NULL AS QUALIFIER, " \
      "1 AS PRECEDENCE "\
    "FROM SYS.TABLES " \
    "WHERE MVID = 0 " \
 \
    "UNION " \
 \
    /* VIEWS */ \
    "SELECT " \
       "CASE " \
          "WHEN TBLID IS NOT NULL AND (SYS4 & 8192 != 0 OR SYS4 & 16384 <> 0) " \
            "THEN 'TABLE_VIEW_MATERIALIZED_ASYNCHRONOUS' " \
          "WHEN TBLID IS NOT NULL THEN 'TABLE_VIEW_MATERIALIZED_SYNCHRONOUS' " \
        "ELSE 'TABLE_VIEW_LOGICAL' " \
      "END AS TYPE, " \
      "OWNER AS OWNER, " \
      "NAME AS NAME, " \
      "NULL AS QUALIFIER, " \
      "2 AS PRECEDENCE "\
    "FROM SYS.VIEWS " \
 \
    "UNION " \
 \
    /* INDEXES */ \
    "SELECT " \
      "CASE " \
        "WHEN SYS.INDEXES.ISPRIMARY = 0x01 THEN 'INDEX_PRIMARY_KEY' " \
        "WHEN SYS.INDEXES.SYS9 = 256 THEN 'INDEX_UNIQUE_COLUMN' " \
        "WHEN SYS.INDEXES.NLSSORTSTR IS NOT NULL THEN 'INDEX_NLSSORT' " \
        "ELSE 'INDEX'  " \
      "END AS TYPE, " \
      "IXOWNER AS OWNER, " \
      "IXNAME AS NAME, " \
      "TBLNAME AS QUALIFIER, " \
      "3 AS PRECEDENCE "\
    "FROM SYS.INDEXES, SYS.TABLES " \
    "WHERE SYS.INDEXES.TBLID = SYS.TABLES.TBLID " \
 \
    "UNION " \
 \
    /* SEQUENCES */ \
    "SELECT " \
      "'SEQUENCE' AS TYPE, " \
      "OWNER AS OWNER, " \
      "NAME AS NAME, " \
      "NULL AS QUALIFIER, " \
      "4 AS PRECEDENCE "\
    "FROM SYS.SEQUENCES " \
 \
    "UNION " \
 \
    /* CACHE GROUPS */ \
    "SELECT " \
      "'CACHE GROUP' AS TYPE, " \
      "CGOWNER AS OWNER, " \
      "CGNAME AS NAME, " \
      "NULL AS QUALIFIER, " \
      "5 AS PRECEDENCE "\
    "FROM SYS.CACHE_GROUP " \
 \
    "UNION " \
 \
    /* REPLICATION SCHEMES */ \
    "SELECT " \
      "'REPLICATION SCHEME' AS TYPE, " \
      "REPLICATION_OWNER AS OWNER, " \
      "REPLICATION_NAME AS NAME, " \
      "NULL AS QUALIFIER, " \
      "6 AS PRECEDENCE "\
    "FROM TTREP.REPLICATIONS " \
 \
    "UNION " \
 \
    /* SYNONYMS */ \
    "SELECT " \
      "CASE " \
        "WHEN OWNER = 'PUBLIC' THEN 'SYNONYM_PUBLIC' " \
        "ELSE 'SYNONYM' " \
      "END AS TYPE, " \
      "OWNER AS OWNER, " \
      "SYNONYM_NAME AS NAME, " \
      "NULL AS QUALIFIER, " \
      "7 AS PRECEDENCE "\
    "FROM SYS.ALL_SYNONYMS " 


/* ---------------------------------------------------------------- */
/* GLOBALS */

/* used by DSTSetParamAsNull () to set a NULL value at statement execution time */
static SQLLEN giNullValue = SQL_NULL_DATA;


/* used by DSTDoubleQuoteIdentifier () to determine whether the */
/* first character in an identifier is valid - if the first character */
/* in an identifier is not a member of the character set below then */
/* the identifier must be double quoted */
static char gszSqlIdentifierBeginChar [] =
  "abcdefghijklmnopqrstuvwxyz"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ";


/* this is the global environment handle managed by */
/* the DSTInitConn() and DSTFreeConn() functions */
/* there is one global environment handle which allows */
/* driver manager connection pooling to be used */
static HENV ghenv = SQL_NULL_HENV;


/* TEST_DATA data source connection handles */
static HENV ghenvTD = SQL_NULL_HENV;
static HDBC ghdbcTD = SQL_NULL_HDBC;



/* ---------------------------------------------------------------- */
/* FUNCTIONS */



/* ---------------------------------------------------------------- */
/* Connection Management Functions */



/*
* Function: DSTInitConn ()
*
* Description: Allocates the ODBC environment and calls SQLDriverConnect () 
* using the specified ODBC connection string. If the connection is successful 
* then TRUE is returned, otherwise, FALSE is returned.
*
* If the connection string uses Oracle syntax instead of ODBC syntax then 
* Pro*C is used to connect to the target (if the executable is linked to the
* Pro*C libraries).
*
* If tracing is enabled for the DSN by the DST_TRACE_CMDS and DST_TRACE_DSNS 
* properties and the connection is a direct ODBC connection then tracing is 
* automatically configured via the the DSTInitTracing() function.
*
* In: pszConnStrIn
*
* Out: phenv, phdbc
*
* In/Out:
*
* Global properties:
*
*/
BOOL DSTInitConn (HENV *phenv, HDBC *phdbc,
                  const char *pszConnStrIn)
{

#ifdef DST_PRC

  if (DSTIsOraConnStr (pszConnStrIn))
    return PRCInitConn (phenv, phdbc, pszConnStrIn);

#endif

  {
    /* locals */
    BOOL bStatus = TRUE;


#ifdef DTP_H

    /* should XA calls be used in place of ODBC calls? */
    if (DSTUseXATxns ())
    {
      bStatus = DTPOpen (phenv, phdbc, pszConnStrIn, TMNOFLAGS);

      if (bStatus)
      {
        /* turn on autocommit to mimic ODBC behavior */
        DSTSetAutoCommit (*phdbc, SQL_AUTOCOMMIT_ON);
      }

      return bStatus;
    }

#endif


    /* report the call */
    {
      /* locals */
      SQLRETURN rc;
      DSTRING message, connStrIn, connName;
      char szConnStrOut [DST_CONN_STR_OUT_LENGTH] = "";

      DST_LOG_REPORT_ME ("DSTInitConn");

      /* validate the parameters */
      assert (pszConnStrIn != NULL && phenv != NULL && phdbc != NULL);

      /* init. dynamic strings */
      STRInit (&message, "");

      /* Configure the connection string to include ConnectionName=<dsn>. */
      /* This allows a connection to query the name of the DSN (via the */
      /* ttConfiguration() built-in) used to connect to the database. This */
      /* is required by some test functionality. */
      STRInit (&connName, "");
      STRInit (&connStrIn, pszConnStrIn);

      /* This will happen *only* if ConnectionName has not been defined */
      /* directly in the input connection string. */
      DSTGetConnStrAttr (pszConnStrIn, "ConnectionName", &connName);

      if (STRC_LENGTH (connName) == 0)
      {
        DSTGetConnStrAttr (pszConnStrIn, "DSN", &connName);
        DSTSetConnStrAttr (&connStrIn, "ConnectionName", STRC (connName));
      }


      /* allocate the environment - only if it has not already */
      /* been allocated by an existing ODBC connection */

      /* Note: SQLFreeEnv() is never called on the global ghenv variable */
      /* after the variable is initially allocated. */
      if (ghenv == SQL_NULL_HENV)
      {
        rc = SQLAllocEnv (&ghenv);

        /* did it succeed? */
        if (!DSTIsSuccess (rc))
        {
          DST_LOG_ERROR ("SQLAllocEnv () failed.");
          bStatus = FALSE;
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (*phenv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
            "SQLAllocEnv () info.");
        }
      }

      /* set the environment handle */
      if (bStatus)
      {
        assert (ghenv != SQL_NULL_HENV);
        *phenv = ghenv;
      }


      /* allocate the connection handle */
      if (bStatus)
      {
        rc = SQLAllocConnect (*phenv, phdbc);

        if (!DSTIsSuccess (rc))
        {
          DST_LOG_SQL_ERROR (*phenv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
            "SQLAllocConnect () failed.");

          bStatus = FALSE;
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (*phenv, *phdbc, SQL_NULL_HSTMT,
            "SQLAllocConnect () info.");
        }
      }



      /* connect */
      if (bStatus)
      {
        /* report the connection attempt */
        STRCopyCString (&message, "Connecting: %s");
        STRInsertCString (&message, pszConnStrIn);
        DST_LOG (STRC (message));

        /* connect to the data source */
        rc = SQLDriverConnect (*phdbc, NULL,
          (SQLCHAR*) STRC (connStrIn), SQL_NTS,
          (SQLCHAR*) szConnStrOut, DST_CONN_STR_OUT_LENGTH, NULL,
          SQL_DRIVER_NOPROMPT);


        /* was the connection successful? */
        if (!DSTIsSuccess (rc))
        {
          DST_LOG_SQL_ERROR (*phenv, *phdbc, SQL_NULL_HSTMT,
            "SQLDriverConnect () failed.");

          /* clean up - deallocate the connection handle */
          SQLFreeConnect (*phdbc);

          bStatus = FALSE;
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (*phenv, *phdbc, SQL_NULL_HSTMT,
            "SQLDriverConnect () info.");
        }


        /* report the out connection string */
        if (bStatus)
        {
          STRCopyCString (&message, "Connection successful: %s");
          STRInsertCString (&message, szConnStrOut);
          DST_LOG (STRC (message));

          /* print the connected element ID if this is a grid connection */
          if (DSTIsGrid (*phdbc) && !DSTIsOraConn (*phdbc))
          {
            UDWORD iElementId = 0;
            
            if (DSTGetThisElementId (*phdbc, &iElementId))
            {
              STRCopyCString (&message, "Element ID: %u");
              STRInsertUnsignedInt (&message, iElementId);
              DST_LOG (STRC (message));
            }
          }


          /* track the connection */
          if (!DSTTrackConn (*phdbc, pszConnStrIn))
          {
            bStatus = FALSE;
            DSTFreeConn (*phenv, *phdbc);
          }

          /* enable tracing */
          if (bStatus)
            DSTInitTracing (*phdbc, pszConnStrIn);
        }
      }


      /* if the connection failed then be sure that the HDBC */
      /* is set to NULL */
      if (!bStatus)
      {
        *phdbc = SQL_NULL_HDBC;
      }
      else
      {
        /* report the handles */
        DSTLogODBCHandle ("HENV", *phenv);
        DSTLogODBCHandle ("HDBC", *phdbc);
      }


      /* free dynamic strings */
      STRFree (&connName);
      STRFree (&connStrIn);
      STRFree (&message);

      DST_LOG_UNREPORT_ME;
    }

    return bStatus;
  }
}


/*
* Function: DSTInitTracing ()
*
* Description: Initializes database tracing via the
* ttTraceMon command. Trace command output is sent to
* a file named <ds_path>/<ds_name>.tracemon. Tracing is
* available only to ODBC direct driver connections.
* The connection's DSN name must be in the space delimited
* list defined by the DST_TRACE_DSNS property.
* A semicolon delimited list of ttTraceMon commands should
* be defined by the DST_TRACE_CMDS property.
* Tracing is not enabled if the DST_TRACE_CMDS
* property is undefined or if the DSN name does not
* appear in the DST_TRACE_DSNS property. If tracing
* is enabled successfully then TRUE is returned.
*
* In: hdbc, pszConnStr
*
* Out:
*
* In/Out:
*
* Global properties:
* DST_TRACE_CMDS
* DST_TRACE_DSNS
*
*/
BOOL DSTInitTracing (HDBC hdbc, const char* pszConnStr)
{
  BOOL bStatus = FALSE;

#ifdef DST_DIRECT

  DSTRING cmd, traceCmds, traceDSNs;
  DSTRING thisDSN, dsPath, connStr;

  DST_LOG_QUIET_ME ("DSTInitTracing");

  STRInit (&cmd, "");
  STRInit (&traceCmds, "");
  STRInit (&traceDSNs, "");
  STRInit (&thisDSN, "");
  STRInit (&dsPath, "");
  STRInit (&connStr, pszConnStr);


  /* get global properties */
  PROPGetAsString ("DST_TRACE_CMDS", &traceCmds);
  PROPGetAsString ("DST_TRACE_DSNS", &traceDSNs);

  /* get the name of the DSN in the conn. string */
  DSTGetConnStrAttr (pszConnStr, "DSN", &thisDSN);

  /* is this DSN name in the DST_TRACE_DSNS list and */
  /* is the DST_TRACE_CMDS property defined? */
  if (PROPIsDefined ("DST_TRACE_CMDS") &&
    STRHasSequence (traceDSNs, STRC (thisDSN), NULL))
  {
    /* execute ttTraceMon ... */
    STRCopyCString (&cmd,
      "ttTraceMon -e \"outfile %s.tracemon; %s;\" -connStr \"%s\"");

    /* construct the output file */
    DSTGetDataStorePath (hdbc, &dsPath);
    STRInsertString (&cmd, dsPath);

    /* insert the trace commands */
    STRInsertString (&cmd, traceCmds);

    /* insert the conn. str. - remove the OverWrite attr. */
    DSTRemConnStrAttr (&connStr, "OverWrite");
    STRInsertString (&cmd, connStr);

    /* execute */
    DST_LOG_VERBOSE;
    bStatus = DSTSpawnProcess (STRC (cmd), TRUE);
    DST_LOG_RESTORE;
  }


  STRFree (&cmd);
  STRFree (&traceCmds);
  STRFree (&traceDSNs);
  STRFree (&thisDSN);
  STRFree (&dsPath);
  STRFree (&connStr);

  DST_LOG_UNREPORT_ME;

#endif  /* DST_DIRECT */
  return bStatus;
}


/*
* Function: DSTInitConnRetryWait ()
*
* Description: Allocates the ODBC environment and calls
* SQLDriverConnect () using the specified ODBC connection
* string. If the connection is successful then TRUE is returned,
* otherwise, FALSE is returned. A connection that initially
* fails will be retried for up to iRetryDuration seconds.
*
* In: pszConnStrIn, iRetryDuration
*
* Out: phenv, phdbc
*
* In/Out:
*
*/
BOOL DSTInitConnRetryWait (HENV *phenv, HDBC *phdbc,
                           const char *pszConnStrIn,
                           SDWORD iRetryDuration)
{
  BOOL bStatus;
  time_t iStartTime;

  DST_LOG_REPORT_ME ("DSTInitConnRetryWait");


  /* validate the retry duration */
  assert (iRetryDuration >= 0);

  /* get the current time */
  iStartTime = DSTGetTicks ();

  do
  {
    /* try connect */
    bStatus = DSTInitConn (phenv, phdbc, pszConnStrIn);

    /* has the retry duration expired? */
    if (!bStatus &&
      (DSTGetTicks () - iStartTime > iRetryDuration))
    {
      DST_LOG_ERROR ("Timeout has expired.");
      break;
    }
    else if (!bStatus)
    {
      /* wait DST_ASYNC_RETRY_INTERVAL seconds before trying again */
      DST_LOG ("Retrying ... ");
      DSTSleep (DST_ASYNC_RETRY_INTERVAL);
    }
    else
    {
      /* the connection succeeded - but is it valid? */
      if (DSTIsInvalid (*phdbc))
      {
        bStatus = FALSE;
        DSTFreeConn (*phenv, *phdbc);
      }
    }

  }
  while (!bStatus);


  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTInitConnOverwrite ()
*
* Description: This is the same as DSTInitConn () except
* that Overwrite=1 is added to the connection string.
*
* In: pszConnStrIn
*
* Out: phenv, phdbc
*
* In/Out:
*
*/
BOOL DSTInitConnOverwrite (HENV *phenv, HDBC *phdbc,
                           const char *pszConnStrIn)
{
  BOOL bStatus;
  DSTRING connStrOverwrite;

  STRInit (&connStrOverwrite, pszConnStrIn);

  /* it is not currently possible to overwrite databases */
  /* accessed via Pro*C connection strings */
  if (!DSTIsOraConnStr (pszConnStrIn))
    DSTSetConnStrAttr (&connStrOverwrite, "Overwrite", "1");

  bStatus = DSTInitConnAsUser (phenv, phdbc,
    STRC (connStrOverwrite), NULL, NULL);

  STRFree (&connStrOverwrite);
  return bStatus;
}

/*
* Function: DSTInitConnForce ()
*
* Description: This is the same as DSTInitConn () except
* that ForceConnect=1 is added to the connection string.
*
* In: pszConnStrIn
*
* Out: phenv, phdbc
*
* In/Out:
*
*/
BOOL DSTInitConnForce (HENV *phenv, HDBC *phdbc,
                       const char *pszConnStrIn)
{
  BOOL bStatus;
  DSTRING connStrForce;

  STRInit (&connStrForce, pszConnStrIn);

  /* it is not currently possible to overwrite databases */
  /* accessed via Pro*C connection strings */
  if (!DSTIsOraConnStr (pszConnStrIn))
  {
    DSTSetConnStrAttr (&connStrForce, "ForceConnect", "1");
    DSTConfigConnStrAsUser (&connStrForce, NULL, NULL);
  }

  bStatus = DSTInitConn (phenv, phdbc, STRC (connStrForce));

  STRFree (&connStrForce);
  return bStatus;
}


/*
* Function: DSTInitConnAsUser ()
*
* Description: This is the same as DSTInitConn () except that
* the given UID/PWD is specified in the connection string.
*
* In: pszConnStrIn, pszUid, pszPwd
*
* Out: phenv, phdbc
*
* In/Out:
*
*/
BOOL DSTInitConnAsUser (HENV *phenv, HDBC *phdbc,
                        const char *pszConnStrIn,
                        const char* pszUid, const char* pszPwd)
{
  BOOL bStatus;
  DSTRING connStr;

  /* configure the connection string */
  STRInit (&connStr, pszConnStrIn);
  DSTConfigConnStrAsUser (&connStr, pszUid, pszPwd);

  /* connect */
  bStatus = DSTInitConn (phenv, phdbc, STRC (connStr));

  STRFree (&connStr);
  return bStatus;
}



/*
* Function: DSTFreeConn ()
*
* Description: Calls SQLDisconnect () and deallocates the
* ODBC environment. If the disconnection is successful then
* TRUE is returned. If the connection is being tracked then
* DSTUntrackConn () is called automatically.
*
* In: henv, hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTFreeConn (HENV henv, HDBC hdbc)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;

#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCFreeConn (henv, hdbc);
#endif

#ifdef DTP_H

  /* should XA calls be used in place of ODBC calls? */
  if (DSTUseXATxns ())
  {

    /* if using XA commit any pending global txn before disconnecting */
    if (DSTIsTransactionPending (hdbc))
    {
      XID xid;
      DTPCreateXIDFromString (DST_XID_STRING, &xid);

      /* end, prepare and commit the current XA txn. */
      DTPEnd (&xid, hdbc, TMSUCCESS);

      if (DTPPrepare (&xid, hdbc, TMNOFLAGS) != XA_RDONLY)
        DTPCommit (&xid, hdbc, TMNOFLAGS);
    }

    bStatus = DTPClose (hdbc, TMNOFLAGS);
    return bStatus;
  }

#endif


  /* report the call */
  {
    DST_LOG_REPORT_ME ("DSTFreeConn");


    /* validate the parameters */
    assert (henv != SQL_NULL_HENV && hdbc != SQL_NULL_HDBC);
    DSTLogODBCHandle ("HENV", henv);
    DSTLogODBCHandle ("HDBC", hdbc);

    /* if a transaction is pending then roll it back */
    if (DSTIsTransactionPending (hdbc))
    {
      /* transaction rollback will fail with 'no logging' */
      /* connections - so try commit if rollback fails */
      if (!DSTIsSuccess (DSTTransact (hdbc, SQL_ROLLBACK)))
        DSTTransact (hdbc, SQL_COMMIT);
    }


    /* disconnect */
    rc = SQLDisconnect (hdbc);

    /* was it successful? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (henv, hdbc, SQL_NULL_HSTMT,
        "SQLDisconnect () failed.");
      bStatus = FALSE;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (henv, hdbc, SQL_NULL_HSTMT,
        "SQLDisconnect () info.");
    }


    /* free the allocated connection */
    rc = SQLFreeConnect (hdbc);

    /* was it successful? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (henv, hdbc, SQL_NULL_HSTMT,
        "SQLFreeConnect () failed.");
      bStatus = FALSE;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
        "SQLFreeConnect () info.");
    }

    /* Note: SQLFreeEnv() is never called on the global ghenv variable after */
    /* the variable is initially allocated. */


    /* disable tracking? */
    if (DSTIsTrackedConn (hdbc))
      DSTUntrackConn (hdbc);


    DST_LOG_UNREPORT_ME;
  }

  return bStatus;
}


/*
* Function: DSTIsOraConnStr ()
*
* Description: Returns TRUE if the connection string uses
* Oracle connection syntax.
*
* In: pszConnStrIn
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsOraConnStr (const char *pszConnStrIn)
{
  BOOL bIsOracle = FALSE;
  DSTRING connStrTest;

  /* is this a Pro*C connection string? */
  STRInit (&connStrTest, pszConnStrIn);
  STRToUpper (connStrTest);

  /* if the string does not have the DSN and/or Driver */
  /* connection attributes then assume it is an Oracle connection */
  if (!STRHasSequence (connStrTest, "DSN=", NULL) &&
    !STRHasSequence (connStrTest, "DRIVER=", NULL))
  {
    bIsOracle = TRUE;
  }

  STRFree (&connStrTest);

  return bIsOracle;
}

/*
* Function: DSTIsProCConn ()
*
* Description: Returns TRUE if the given connection or
* statement handle is associated with a Pro*C connection.
*
* In: hdbc/hstmt
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsProCConn (HDBC hdbc)
{
  BOOL bIsProC = FALSE;

#ifdef DST_PRC
  bIsProC = PRCIsConn (hdbc);
#endif

  return bIsProC;
}

BOOL DSTIsProCStmt (HSTMT hstmt)
{
  BOOL bIsProC = FALSE;

#ifdef DST_PRC
  bIsProC = PRCIsStmt (hstmt);
#endif

  return bIsProC;
}



/* ---------------------------------------------------------------- */
/* Statement Management Functions */



/*
* Function: DSTAllocStmt ()
*
* Description: Allocates a statement handle. A record is inserted into the
* ALLOC_STATEMENT table of the test results repository. This record maps
* the HDBC in the TEST_RUN table to the allocated HSTMT.
*
* In: hdbc
*
* Out:
*
* In/Out: phstmt
*
*/
SQLRETURN DSTAllocStmt (HDBC hdbc, HSTMT *phstmt)
{

#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCAllocStmt (hdbc, phstmt);
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTAllocStmt");

    rc = SQLAllocStmt (hdbc, phstmt);

    /* error ? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLAllocStmt () failed.");

      /* make sure a NULL statement handle is returned by the driver */
      /* when the pointer is valid: this is an ODBC requirement */
       if (phstmt != NULL)
      {
        assert (*phstmt == SQL_NULL_HSTMT);
      }

    }
    /* success with info ? */
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, *phstmt,
        "SQLAllocStmt () info.");
    }

    /* record the allocated statement */
    if (DSTIsSuccess (rc))
      DSTPostAllocStmt (hdbc, *phstmt);

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}



/*
* Function: DSTPrepareStmt ()
*
* Description: Prepares a SQL statement.
*
* In: hstmt, pszSql
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTPrepareStmt (HSTMT hstmt, const char *pszSql)
{

#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCPrepareStmt (hstmt, pszSql);
#endif

  {
    /* locals */
    SQLRETURN rc;
    DSTRING mssg;
    ttTime ttStartTime;
    ttTime ttEndTime;
    float fElapsedTime;
    char szFormattedTime [32] = "";

    DST_LOG_REPORT_ME ("DSTPrepareStmt");

    /* validate parms. */
    assert (hstmt != SQL_NULL_HSTMT &&
      pszSql != NULL);

    DSTLogODBCHandle ("HSTMT", hstmt);


    /* init. dynamic strings */
    STRInit (&mssg, "");


    STRCopyCString (&mssg, "Preparing: %s");
    STRInsertCString (&mssg, pszSql);
    DST_LOG (STRC (mssg));


    /* prepare the statement and measure the */
    /* time required for it to return */
    ttGetTime (&ttStartTime);

    rc = SQLPrepare (hstmt, (SQLCHAR*) pszSql, SQL_NTS);

    ttGetTime (&ttEndTime);


    /* get the elapsed time in milliseconds */
    ttDiffTimeMilli (&ttStartTime,
      &ttEndTime, &fElapsedTime);

    /* convert from milliseconds to seconds */
    fElapsedTime = fElapsedTime / 1000;

    sprintf (szFormattedTime, "%.3f", fElapsedTime);
    STRCopyCString (&mssg, "Elapsed prepare time: %s seconds");
    STRInsertCString (&mssg, szFormattedTime);
    DST_LOG (STRC (mssg));


    /* record the statement */
    DSTPostPrepareStmt (hstmt, pszSql, fElapsedTime, rc);


    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLPrepare () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLPrepare () info.");
    }

    if (DSTIsSuccess (rc))
      DST_LOG ("Prepare successful.");



    STRFree (&mssg);

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}



/*
* Function: DSTExecPreparedStmt ()
*
* Description: Executes a previously prepared SQL statement.
*
* In: hstmt
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecPreparedStmt (HSTMT hstmt)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCExecPreparedStmt (hstmt);
#endif

  {
    /* locals */
    SQLRETURN rc;
    SDWORD iNumRowsAffected = 0;
    DSTRING message;
    ttTime ttStartTime;
    ttTime ttEndTime;
    float fElapsedTime;
    char szFormattedTime [32] = "";

    /* report the function call */
    DST_LOG_REPORT_ME ("DSTExecPreparedStmt");

    assert (hstmt != SQL_NULL_HSTMT);
    DSTLogODBCHandle ("HSTMT", hstmt);


    /* init. dynamic strings */
    STRInit (&message, "");


    /* record the intention to execute the statement in the */
    /* test results repository */
    DSTPostPreExecStmt (hstmt, NULL, 1);


    /* execute the prepared statement and measure the */
    /* time required for it to return */
    ttGetTime (&ttStartTime);

    rc = SQLExecute (hstmt);

    ttGetTime (&ttEndTime);


    /* get the elapsed time in milliseconds */
    ttDiffTimeMilli (&ttStartTime, &ttEndTime, &fElapsedTime);

    /* convert from milliseconds to seconds */
    fElapsedTime = fElapsedTime / 1000;

    sprintf (szFormattedTime, "%.3f", fElapsedTime);
    STRCopyCString (&message, "Elapsed execution time: %s seconds");
    STRInsertCString (&message, szFormattedTime);
    DST_LOG (STRC (message));


    /* did the statement fail or generate any info? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLExecute () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLExecute () info.");
    }


    if (DSTIsSuccess (rc) && rc != SQL_NEED_DATA)
    {
      /* did the statement operate on any rows? */
      iNumRowsAffected = DSTGetRowsAffectedCount (hstmt);

      /* were any rows affected? if true then report the number. */
      if (iNumRowsAffected > 0)
      {
        STRCopyCString (&message, "Rows affected: %d");
        STRInsertInt (&message, iNumRowsAffected);
        DST_LOG (STRC (message));
      }

      DST_LOG ("Execution successful.");
    }


    /* record the statement */
    DSTPostExecStmt (hstmt, NULL, fElapsedTime,
      rc, iNumRowsAffected);


    /* free dynamic strings */
    STRFree (&message);

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}





/*
* Function: DSTExecDirectStmt ()
*
* Description: Executes the specified SQL statement.
*
* In: hstmt, pszSql
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecDirectStmt (HSTMT hstmt, const char *pszSql)
{

#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
  {
    if (DSTIsSuccess (DSTPrepareStmt (hstmt, pszSql)))
      return DSTExecPreparedStmt (hstmt);

    return SQL_ERROR;
  }
#endif

  {
    /* locals */
    SQLRETURN rc;
    SDWORD iNumRowsAffected = 0;
    ttTime ttStartTime;
    ttTime ttEndTime;
    float fElapsedTime;
    DSTRING message;
    char szFormattedTime [32] = "";


    /* report the function call */
    DST_LOG_REPORT_ME ("DSTExecDirectStmt");

    assert (hstmt != SQL_NULL_HSTMT && pszSql != NULL);
    DSTLogODBCHandle ("HSTMT", hstmt);


    /* init. dynamic strings */
    STRInit (&message, "");


    STRCopyCString (&message, "Executing: %s");
    STRInsertCString (&message, pszSql);
    DST_LOG (STRC (message));


    /* record the intention to execute the statement in the */
    /* test results repository */
    DSTPostPreExecStmt (hstmt, pszSql, 1);


    /* execute the statement and measure the time required for it to return */
    /* get the elapsed time in milliseconds */
    ttGetTime (&ttStartTime);

    rc = SQLExecDirect (hstmt, (SQLCHAR*) pszSql, SQL_NTS);

    ttGetTime (&ttEndTime);
    ttDiffTimeMilli (&ttStartTime, &ttEndTime, &fElapsedTime);

    /* convert from milliseconds to seconds */
    fElapsedTime = fElapsedTime / 1000;

    sprintf (szFormattedTime, "%.3f", fElapsedTime);
    STRCopyCString (&message, "Elapsed execution time: %s seconds");
    STRInsertCString (&message, szFormattedTime);
    DST_LOG (STRC (message));



    /* did statement execution generate any errors or info ? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLExecDirect () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLExecDirect () info.");
    }


    if (DSTIsSuccess (rc) && rc != SQL_NEED_DATA)
    {
      /* did the statement operate on any rows? */
      iNumRowsAffected = DSTGetRowsAffectedCount (hstmt);

      /* were any rows affected? if true then report the number. */
      if (iNumRowsAffected > 0)
      {
        STRCopyCString (&message, "Rows affected: %d");
        STRInsertInt (&message,  iNumRowsAffected);
        DST_LOG (STRC (message));
      }

      DST_LOG ("Execution successful.");
    }


    /* record the statement's execution results in the */
    /* test results repository */
    DSTPostExecStmt (hstmt, pszSql, fElapsedTime, rc, iNumRowsAffected);


    /* free dynamic strings */
    STRFree (&message);


    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTExecuteBatch ()
*
* Description: Executes a SQL statement repeatedly iBatchSize times.
* If the output parameter piBatchSizeProcessed is not NULL then the
* number of processed elements in the batch is returned.
*
* In: hdbc, pszSql, iBatchSize
*
* Out: piBatchSizeProcessed
*
* In/Out:
*
*/
SQLRETURN DSTExecuteBatch (HDBC hdbc, const char *pszSql,
                           SQLROWSETSIZE iBatchSize,
                           SQLROWSETSIZE* piBatchSizeProcessed)
{

#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCExecuteBatch (hdbc, pszSql, iBatchSize);
#endif

  {
    /* locals */
    SQLRETURN rc;
    HSTMT hstmt;
    DSTRING message;
    SDWORD iNumRowsAffected = 0;
    SQLROWSETSIZE iBatchSizeProcessed = 1;
    ttTime ttStartTime;
    ttTime ttEndTime;
    float fElapsedTime;
    char szFormattedTime [32] = "";


    /* report the function call */
    DST_LOG_REPORT_ME ("DSTExecuteBatch");
    DSTLogODBCHandle ("HDBC", hdbc);

    /* validate params */
    assert (pszSql != NULL);


    /* set the output batch size processed param to */
    /* 0 as a default */
    if (piBatchSizeProcessed != NULL)
      *piBatchSizeProcessed = 0;


    /* allocate the statement  */
    rc = DSTAllocStmt (hdbc, &hstmt);

    /* was it successful? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_UNREPORT_ME;
      return rc;
    }

    /* set the size of the batch */
    rc = SQLParamOptions (hstmt, iBatchSize, &iBatchSizeProcessed);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLParamOptions () failed.");

      DSTFreeStmt (hstmt);
      DST_LOG_UNREPORT_ME;
      return rc;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLParamOptions () info.");
    }


    /* execute the statement */
    STRInit (&message, "Executing batch: %s");
    STRInsertCString (&message, pszSql);
    DST_LOG (STRC (message));



    /* record the intention to execute the statement in the */
    /* test results repository */
    DSTPostPreExecStmt (hstmt, pszSql, iBatchSize);


    /* execute the statement and measure the time required for it to return */
    /* get the elapsed time in milliseconds */
    ttGetTime (&ttStartTime);

    rc = SQLExecDirect (hstmt, (SQLCHAR*) pszSql, SQL_NTS);

    ttGetTime (&ttEndTime);
    ttDiffTimeMilli (&ttStartTime, &ttEndTime, &fElapsedTime);

    /* convert from milliseconds to seconds */
    fElapsedTime = fElapsedTime / 1000;

    sprintf (szFormattedTime, "%.3f", fElapsedTime);
    STRCopyCString (&message, "Elapsed execution time: %s seconds");
    STRInsertCString (&message, szFormattedTime);
    DST_LOG (STRC (message));


    /* report the number of statement processed in the batch */
    STRCopyCString (&message, "%llu of %llu batch statements processed.");
    STRInsertUnsignedBigInt (&message, iBatchSizeProcessed);
    STRInsertUnsignedBigInt (&message, iBatchSize);
    DST_LOG (STRC (message));

    if (piBatchSizeProcessed != NULL)
      *piBatchSizeProcessed = iBatchSizeProcessed;


    /* did statement execution generate any errors or info? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLExecDirect() failed.");

      /* record the failed batch statement */
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLExecDirect () info.");
    }


    if (rc != SQL_NEED_DATA)
    {
      /* did the statement affect any rows? */
      iNumRowsAffected = DSTGetRowsAffectedCount (hstmt);

      /* were any rows affected? if true then report the number. */
      if (iNumRowsAffected > 0)
      {
        STRCopyCString (&message, "Rows affected: %d");
        STRInsertInt (&message,  iNumRowsAffected);
        DST_LOG (STRC (message));
      }
    }


    if (DSTIsSuccess (rc))
      DST_LOG ("Execution successful.");


    /* record the batch execution results */
    DSTPostExecBatchStmt (hstmt, pszSql, fElapsedTime, rc,
      iNumRowsAffected, iBatchSize, iBatchSizeProcessed);



    /* deallocate the statement */
    DSTFreeStmt (hstmt);

    STRFree (&message);

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTExecute ()
*
* Description: Executes a SQL statement. The statement can be an update or
* query. The results of the statement's execution are automatically
* logged to the test results repository.
*
* In: hdbc, pszSql
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecute (HDBC hdbc, const char *pszSql)
{
  /* locals */
  SQLRETURN rc;
  HSTMT hstmt;

  /* report the function call */
  DST_LOG_REPORT_ME ("DSTExecute");
  DSTLogODBCHandle ("HDBC", hdbc);


  /* validate params */
  assert (pszSql != NULL && hdbc != SQL_NULL_HDBC);


  /* allocate the statement  */
  rc = DSTAllocStmt (hdbc, &hstmt);

  /* was it successful? */
  if (!DSTIsSuccess (rc))
  {
    DST_LOG_UNREPORT_ME;
    return rc;
  }


  /* execute the statement */
  rc = DSTExecDirectStmt (hstmt, pszSql);


  /* was a result set returned */
  if (DSTIsSuccess (rc) &&
    DSTIsResultSet (hstmt))
  {
    /* print the result set */
    DSTRESULTSET resultSet;

    if (DSTBindResultSet (hstmt, &resultSet))
    {
      if (!DSTPrintBoundResultSet (&resultSet, FALSE))
        rc = SQL_ERROR;

      DSTFreeResultSet (&resultSet);
    }
    else
    {
      rc = SQL_ERROR;
    }
  }



  /* deallocate the statement */
  DSTFreeStmt (hstmt);


  DST_LOG_UNREPORT_ME;
  return rc;
}


/*
* Function: DSTExecuteAsPLSQL ()
*
* Description: Executes a SQL statement within a PLSQL anonymous
* block.
*
* In: hdbc, pszSql
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecuteAsPLSQL (HDBC hdbc, const char *pszSql)
{
  SQLRETURN rc;
  DSTRING message;
  HSTMT hstmt;

  DST_LOG_REPORT_ME ("DSTExecuteAsPLSQL");

  STRInit (&message, "Executing as PLSQL: ");
  STRAppendCString (&message, pszSql);

  DST_LOG (STRC (message));

  DSTAllocStmt (hdbc, &hstmt);
  DSTSetParamAsString (hstmt, 1, pszSql);

  rc = DSTExecDirectStmt (hstmt, "BEGIN execute (:p1); END;");

  DSTFreeStmt (hstmt);

  STRFree (&message);

  DST_LOG_UNREPORT_ME;
  return rc;
}


#ifdef DST_INCLUDE_COREXEL

/*
* Function: DSTExecAsync ()
*
* Description: Entry function for a separate thread to
* execute an arbitrary SQL statement.
*
* In: pExecInfo
*
* Out:
*
* In/Out:
*
*/
TtThreadStart DSTExecAsync (DSTSQLTHREAD* pExecInfo)
{
  pExecInfo->rc = DSTExecute (pExecInfo->hdbc, pExecInfo->pszSql);
  return TtThreadDone;
}

#endif


/*
* Function: DSTExecRetryCount ()
*
* Description: Executes a SQL statement. The statement can be an update or
* query. The results of the statement's execution are automatically
* logged to the test results repository. If the statement fails then
* the statement will be re-executed for up to iRetryCount times or until
* the statement succeeds.
*
* In: hdbc, pszSql, iRetryCount
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecRetryCount (HDBC hdbc, char *pszSql,
                             SDWORD iRetryCount)
{
  /* locals */
  SDWORD iExecCount = 0;
  SQLRETURN rc;

  /* validate the retry count */
  assert (iRetryCount >= 0);

  do
  {
    rc = DSTExecute (hdbc, pszSql);
    iExecCount ++;
  }
  while (!DSTIsSuccess (rc) &&
    iExecCount < iRetryCount);


  return rc;
}


/*
* Function: DSTExecRetryWait ()
*
* Description: Executes a SQL statement. The statement can be an update or
* query. The results of the statement's execution are automatically
* logged to the test results repository. If the statement fails then
* the statement will be re-executed for up to iRetryDuration seconds or
* until the statement succeeds.
*
* In: hdbc, pszSql, iRetryDuration
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecRetryWait (HDBC hdbc, const char *pszSql,
                            SDWORD iRetryDuration)
{
  /* locals */
  SQLRETURN rc;
  time_t iStartTime, iEndTime;

  DST_LOG_REPORT_ME ("DSTExecRetryWait");

  /* validate the retry duration */
  assert (iRetryDuration >= 0);

  /* get the current time */
  iStartTime = DSTGetTicks ();


  /* execute, but do not retry fatal errors */

  /* Note that client/server failover connections can automatically recover */
  /* to a valid state from an invalid state. The DSTIsInvalid() function is */
  /* not used here to avoid returning prematurely for failover connections. */
  while (!DSTIsSuccess (rc = DSTExecute (hdbc, pszSql)) &&
    !DSTIsLastSqlErrorFatal ())
  {
    iEndTime = DSTGetTicks ();

    /* has the retry duration expired? */
    if ((iEndTime - iStartTime > iRetryDuration))
    {
      DST_LOG_ERROR ("Timeout has expired.");
      break;
    }

    /* wait DST_ASYNC_RETRY_INTERVAL seconds before retrying */
    DST_LOG ("Retrying ... ");
    DSTSleep (DST_ASYNC_RETRY_INTERVAL);
  }

  DST_LOG_UNREPORT_ME;
  return rc;
}


/*
* Function: DSTExecRetryTransient ()
*
* Description: Executes a SQL statement. The statement can be an update or
* query. The results of the statement's execution are automatically
* logged to the test results repository. If the statement fails due to
* a transient error code then the statement will be re-executed for up to
* iRetryDuration seconds or until the statement succeeds or until a non
* transient error is returned.
*
* In: hdbc, pszSql
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecRetryTransient (HDBC hdbc, const char *pszSql,
                                 SDWORD iRetryDuration)
{
  /* locals */
  SQLRETURN rc;
  time_t iStartTime, iEndTime;

  DST_LOG_REPORT_ME ("DSTExecRetryTransient");


  /* validate the retry duration */
  assert (iRetryDuration >= 0);


  /* get the current time */
  iStartTime = DSTGetTicks ();


  while (!DSTIsSuccess (rc = DSTExecute (hdbc, pszSql)) &&
    DSTIsLastSqlErrorTransient ())
  {
    iEndTime = DSTGetTicks ();

    /* has the retry duration expired? */
    if ((iEndTime - iStartTime > iRetryDuration))
    {
      DST_LOG_ERROR ("Timeout has expired.");
      break;
    }

    /* wait DST_ASYNC_RETRY_INTERVAL seconds before retrying */
    DST_LOG ("Retrying ... ");
    DSTSleep (DST_ASYNC_RETRY_INTERVAL);
  }


  DST_LOG_UNREPORT_ME;
  return rc;
}


/*
* Function: DSTExecDirectStmtRetryTransient ()
*
* Description: Executes a SQL statement. The statement can be an update or
* query. The results of the statement's execution are automatically
* logged to the test results repository. If the statement fails due to
* a transient error code then the statement will be re-executed for up to
* iRetryDuration seconds or until the statement succeeds or until a non
* transient error is returned.
*
* In: hdbc, pszSql
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTExecDirectStmtRetryTransient (HSTMT hstmt, const char *pszSql,
                                           SDWORD iRetryDuration)
{
  /* locals */
  SQLRETURN rc;
  time_t iStartTime, iEndTime;

  DST_LOG_REPORT_ME ("DSTExecDirectStmtRetryTransient");


  /* validate the retry duration */
  assert (iRetryDuration >= 0);


  /* get the current time */
  iStartTime = DSTGetTicks ();


  while (!DSTIsSuccess (rc = DSTExecDirectStmt (hstmt, pszSql)) &&
    DSTIsLastSqlErrorTransient ())
  {
    iEndTime = DSTGetTicks ();

    /* has the retry duration expired? */
    if ((iEndTime - iStartTime > iRetryDuration))
    {
      DST_LOG_ERROR ("Timeout has expired.");
      break;
    }

    /* wait DST_ASYNC_RETRY_INTERVAL seconds before retrying */
    DST_LOG ("Retrying ... ");
    DSTSleep (DST_ASYNC_RETRY_INTERVAL);
  }


  DST_LOG_UNREPORT_ME;
  return rc;
}



/*
* Function: DSTExecLoadFromOracle ()
*
* Description: As of TimesTen 12.1 the ttLoadFromOracle() built-in procedure 
* does not return an ODBC error status and error codes in the same way as other
* statements. This error information is conveyed via a result set rather than
* the ODBC return code and error stack. By calling this procedure with a 
* ttLoadFromOracle() statement, any returned error information is conveyed via
* the DST library using the old ODBC error conventions. For example, if 
* ttLoadFromOracle() fails, a call to DSTGetLastSqlError() will return the 
* associated error information. In addition, a caller may optionally pass in a
* reference to a DSTLOADFROMORAINFO structure which is populated with additional
* information from the result set.
*
* In: hdbc, pszSql
*
* Out: pLoadInfo
*
* In/Out:
*
*/
SQLRETURN DSTExecLoadFromOracle (HDBC hdbc, const char *pszSql, 
  struct DSTLOADFROMORAINFO* pLoadInfo)
{
  SQLRETURN rc = SQL_SUCCESS;

  HSTMT hstmt = SQL_NULL_HSTMT;
  DSTRESULTSET rs;
  DSTRING row;
  SWORD iResultColCount = 0;

  DST_LOG_REPORT_ME ("DSTExecLoadFromOracle");

  /* if the info structure has been passed in then initialize it */
  if (pLoadInfo != NULL)
  {
    pLoadInfo->iLoadStatus = 0;
    pLoadInfo->iNumErrors = 0;
    pLoadInfo->iNumResultRows = 0;
    pLoadInfo->iOracleSCN = 0;
    pLoadInfo->iRowsIgnored = 0;
    pLoadInfo->iRowsLoaded = 0;
  }


  STRInit (&row, "");

  rc = DSTAllocStmt (hdbc, &hstmt); 

  if (DSTIsSuccess (rc))
  {

    rc = DSTExecDirectStmt (hstmt, pszSql);

    if (DSTIsSuccess (rc) && 
      DSTBindResultSet (hstmt, &rs) &&
      DSTGetResultColumnCount (hstmt, &iResultColCount))
    {
      SDWORD iRowNum = 0;

      while (DSTIsSuccess (DSTFetch (hstmt)))
      {        
        iRowNum ++;

        /* get the record output */
        DSTGetBoundRowAsString (&rs, &row, TRUE);

        /* Columns:
          Rows_Loaded	     TT_BIGINT	Number of rows loaded
          Errors           TT_INT     Number of errors encountered  

          Error_Code       TT_INTEGER TimesTen Error Code 
             0  : Load successfully completed 
            -1  : Load successfully completed with errors
            -2  : Load terminated early with errors
            -3  : Load terminated early with fatal error

          Error_Message	   TT_VARCHAR	Error Message
        */


        /* earlier versions of TimesTen (before 12.x) returned only 1 column */
        if (iResultColCount == 1)
        {
          if (pLoadInfo != NULL)
          {
            /* get the number of rows loaded in column 1 */
            sscanf (rs.pColumns->pszBuffer, "%u", 
              &(pLoadInfo->iRowsLoaded));
          }

          /* log the output */
          DST_LOG ("");
          DST_LOG (STRC (row));

          continue;
        }

        /* If the third column from the first result row indicates any sort */
        /* of 'early termination' error then set the SQLRETURN status to */
        /* SQL_ERROR. */
        if (iRowNum == 1 &&
          strcmp (rs.pColumns->pNext->pNext->pszBuffer, "0") != 0 &&
          strcmp (rs.pColumns->pNext->pNext->pszBuffer, "-1") != 0)
        {
          rc = SQL_ERROR;
          /* subsequent rows should have the actual error information */
        }
        /* If subsequent result set rows indicate an error then set the last */
        /* SQL error or SQL warning in the logging module. */
        else if (iRowNum > 1 &&
          strcmp (rs.pColumns->pNext->pNext->pszBuffer, "0") != 0)
        {

          /* this is an error row */
          if (!DSTIsSuccess (rc))
          {
            /* set the error info. in the logging module */
            DSTSetLastSqlError (NULL,
              (SDWORD) atoi (rs.pColumns->pNext->pNext->pszBuffer),
              rs.pColumns->pNext->pNext->pNext->pszBuffer);
          }
          /* this is a warning row */
          else
          {
            rc = SQL_SUCCESS_WITH_INFO;

            /* set the warning info. in the logging module */
            DSTSetLastSqlWarning (NULL,
              (SDWORD) atoi (rs.pColumns->pNext->pNext->pszBuffer),
              rs.pColumns->pNext->pNext->pNext->pszBuffer);
          }

        }

        /* log the output as an error? */
        if (!DSTIsSuccess (rc))
        {
          DST_LOG_ERROR ("");
          DST_LOG_ERROR (STRC (row));
        }
        else
        {
          DST_LOG ("");
          DST_LOG (STRC (row));
        }

        /* set attributes of the optional info structure */
        if (pLoadInfo != NULL)
        {
          pLoadInfo->iNumResultRows = iRowNum;

          /* set fields associated with the first row only */
          if (iRowNum == 1)
          {
            DSTRING mssg, keyValue, key, value;
            size_t iNumTokens, iTokenIndex;

            /* get the number of rows loaded in column 1 */
            sscanf (rs.pColumns->pszBuffer, "%u", 
              &(pLoadInfo->iRowsLoaded));

            /* get the total number of errors in column 2 */
            sscanf (rs.pColumns->pNext->pszBuffer, "%u", 
              &(pLoadInfo->iNumErrors));

            /* get the overall status in column 3 */
            sscanf (rs.pColumns->pNext->pNext->pszBuffer, "%d", 
              &(pLoadInfo->iLoadStatus));

            /* parse the message in column 4 */
            STRInit (&mssg, rs.pColumns->pNext->pNext->pNext->pszBuffer);

            /* get the Oracle SCN */
            iNumTokens = STRGetNumTokens (mssg, ";");

            for (iTokenIndex = 1; iTokenIndex <= iNumTokens; iTokenIndex ++)
            {
              STRInit (&keyValue, "");
              STRInit (&key, "");
              STRInit (&value, "");

              STRGetTokenAt (mssg, iTokenIndex, ";", &keyValue);
              STRGetTokenAt (keyValue, 1, "=", &key);
              STRGetTokenAt (keyValue, 2, "=", &value);

              STRToUpper (key);
              STRTrim (&key, STR_WHITESPACE_CHARS);

              /* get the SCN */
              if (STREqualsCString (key, "ORACLESCN"))
              {
#ifdef SB_P_OS_NT
                sscanf (STRC (value), "%I64u", 
                  &(pLoadInfo->iOracleSCN));
#elif TT64
                sscanf (STRC (value), "%lu", 
                  &(pLoadInfo->iOracleSCN));
#else
                sscanf (STRC (value), "%llu", 
                  &(pLoadInfo->iOracleSCN));
#endif
              }

              /* get the number of rows ignored */
              if (STREqualsCString (key, "ROWS IGNORED"))
              {
                sscanf (STRC (value), "%u", 
                  &(pLoadInfo->iRowsIgnored));
              }

              STRFree (&value);
              STRFree (&key);
              STRFree (&keyValue);
            }

            STRFree (&mssg);
          }


        }

      } /* end fetch loop */

      DSTFreeResultSet (&rs);
    }

    DSTFreeStmt (hstmt);
  }


  STRFree (&row);

  DST_LOG_UNREPORT_ME;
  return rc;
}


/*
* Function: DSTExecLoadFromOracleRetryWait ()
*
* Description: This function will call ttLoadFromOracle() until it returns
* a success status or the timeout expires.
*
* In: hdbc, pszLoadSql, iTimeout
*
* Out:
*
* In/Out:
*
*/
BOOL DSTExecLoadFromOracleRetryWait (HDBC hdbc, const char* pszLoadSql, 
                                     SDWORD iTimeout)
{
  BOOL bStatus = TRUE;

  time_t iStartTime = DSTGetTicks ();

  while (!DSTIsSuccess (DSTExecLoadFromOracle (hdbc, pszLoadSql, NULL)))
  {
    if (DSTGetTicks () < iStartTime + iTimeout)
    {
      continue;
    }

    bStatus = FALSE;
    break;
  }


  return bStatus;
}


/*
* Function: DSTGetRowsAffectedCount  ()
*
* Description: Returns the number of rows affected by the execution of the
* associated statement.
*
* In: hstmt
*
* Out:
*
* In/Out:
*
*/
SDWORD DSTGetRowsAffectedCount (HSTMT hstmt)
{

  /* locals */
  SQLRETURN rcRowCount;
  SQLLEN iNumRowsAffected = -1;

  DST_LOG_QUIET_ME ("DSTGetRowsAffectedCount");

  rcRowCount = SQLRowCount (hstmt, &iNumRowsAffected);

  /* was SQLRowCount () successful? */
  if (!DSTIsSuccess (rcRowCount))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLRowCount () failed.");
  }
  else if (rcRowCount == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLRowCount () info.");
  }


  DST_LOG_UNREPORT_ME;
  return (SDWORD) iNumRowsAffected;
}


/*
* Function: DSTFreeStmt ()
*
* Description: Frees a statement handle. If the statement handle matches
* a statement handle that is recorded in the ALLOC_STATEMENT table of the
* test results repository - and if that record has not been marked
* 'FREED' then the record will be marked 'FREED'.
*
* In: hstmt
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTFreeStmt (HSTMT hstmt)
{

#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCFreeStmt (hstmt);
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTFreeStmt");

    /* record the freed statement before actually freeing it */
    DSTPostFreeStmt (hstmt);

    /* free the statement */
    rc = SQLFreeStmt (hstmt, SQL_DROP);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLFreeStmt () failed.");
    }

    /* note that if SQL_SUCCESS_WITH_INFO is returned there */
    /* is no way to pass in a valid statement handle to retrieve */
    /* the info message */


    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTCloseStmtCursor ()
*
* Description: Closes an open cursor on the statement handle.
*
* In:  hstmt
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTCloseStmtCursor (HSTMT hstmt)
{

#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCCloseStmtCursor (hstmt);
#endif

  {
    SQLRETURN rc;
    DST_LOG_QUIET_ME ("DSTCloseStmtCursor");

    /* validate params. */
    assert (hstmt != SQL_NULL_HSTMT);

    rc = SQLFreeStmt (hstmt, SQL_CLOSE);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLFreeStmt () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLFreeStmt () info.");
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}




/* ---------------------------------------------------------------- */
/* Result Set and Data Transfer Functions */




/*
* Function: DSTPrintQueryResult ()
*
* Description: This is a convenience function that prints only the result
* from a given query. If bVertical is TRUE then each row value is printed
* on a separate line with the column's name preceding the column's value.
*
* In: hdbc, pszQuery, bVertical
*
* Out:
*
* In/Out:
*
*/
BOOL DSTPrintQueryResult (HDBC hdbc, const char* pszQuery, BOOL bVertical)
{
  BOOL bStatus = TRUE;
  HSTMT hstmt = SQL_NULL_HSTMT;
  DST_LOG_QUIET_ME ("DSTPrintQueryResult");

  /* validate params */
  assert (pszQuery != NULL && hdbc != SQL_NULL_HDBC);


  /* allocate the statement  */
  if (!DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DST_LOG_UNREPORT_ME;
    return FALSE;
  }


  /* execute the statement */
  if (!DSTIsSuccess (DSTExecDirectStmt (hstmt, pszQuery)))
    bStatus = FALSE;


  /* was a result set returned */
  if (bStatus && DSTIsResultSet (hstmt))
  {
    /* print the result set */
    DSTRESULTSET resultSet;

    if (DSTBindResultSet (hstmt, &resultSet))
    {
      DST_LOG_VERBOSE;

      if (!DSTPrintBoundResultSet (&resultSet, bVertical))
        bStatus = FALSE;

      DST_LOG_RESTORE;

      DSTFreeResultSet (&resultSet);
    }
    else
    {
      bStatus = FALSE;
    }
  }

  /* deallocate the statement */
  DSTFreeStmt (hstmt);


  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTIsResultSet ()
*
* Description: Returns TRUE if a result set is associated
* with the specified statement handle. Otherwise FALSE is
* returned.
*
* In:  hstmt
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsResultSet (HSTMT hstmt)
{
  /* locals */
  BOOL bStatus = FALSE;
  SWORD iNumCols = 0;
  DST_LOG_QUIET_ME ("DSTIsResultSet");

  if (DSTGetResultColumnCount (hstmt, &iNumCols) && iNumCols > 0)
  {
    bStatus = TRUE;
  }

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetResultColumnCount ()
*
* Description: Returns the number of result set columns
* associated with the statement handle. If the procedure
* fails then FALSE is returned.
*
* In:  hstmt
*
* Out: piNumCols
*
* In/Out:
*
*/
BOOL DSTGetResultColumnCount (HSTMT hstmt, SWORD* piNumCols)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCGetNumResultCols (hstmt, piNumCols);
#endif

  {
    /* locals */
    SQLRETURN rc;
    BOOL bStatus = TRUE;

    DST_LOG_QUIET_ME ("DSTGetResultColumnCount");

    assert (hstmt != SQL_NULL_HSTMT && piNumCols != NULL);


    /* did the statement return a result set? */
    rc = SQLNumResultCols (hstmt, piNumCols);

    /* was it successful? */
    if (!DSTIsSuccess (rc))
    {
      bStatus = FALSE;
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLNumResultCols () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLNumResultCols () info.");
    }

    DST_LOG_UNREPORT_ME;
    return bStatus;
  }
}


/*
* Function: DSTFetch ()
*
* Description: This function calls SQLFetch () for the
* specified statement handle.
*
* In:  hstmt
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTFetch (HSTMT hstmt)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCFetch (hstmt);
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTFetch");

    /* call SQLFetch... */
    rc = SQLFetch (hstmt);

    /* did an error occur? */
    if (!DSTIsSuccess (rc) && rc != SQL_NO_DATA_FOUND)
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV,
        SQL_NULL_HDBC,
        hstmt,
        "SQLFetch () failed.");
    }
    /* was information returned? */
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV,
        SQL_NULL_HDBC,
        hstmt,
        "SQLFetch () info.");
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}

/*
* Function: DSTSetMaxRows ()
*
* Description: Sets the maximum number of rows returned by
* calls to SQLFetch() on the statement handle. If iRowCount
* is 0 then all rows are returned.
*
* In:  hstmt, iRowCount
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetMaxRows (HSTMT hstmt, SQLROWCOUNT iRowCount)
{
  BOOL bStatus = TRUE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTSetMaxRows");


  /* set the maximum number of rows to return from */
  /* the fetch cycle */
  if (!DSTIsProCStmt (hstmt))
  {
    rc = SQLSetStmtOption (hstmt,
      SQL_MAX_ROWS, iRowCount);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV,
        SQL_NULL_HDBC,
        hstmt,
        "SQLSetStmtOption () failed.");

      bStatus = FALSE;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV,
        SQL_NULL_HDBC,
        hstmt,
        "SQLSetStmtOption () failed.");
    }
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTBindResultSet ()
*
* Description: Initializes a result set structure for an
* executed statement and binds the result set columns as
* C strings.
*
* In:  hstmt
*
* Out: pResultSet
*
* In/Out:
*
* Global Properties:
* DST_MAX_ROWS
*
*/
BOOL DSTBindResultSet (HSTMT hstmt,
                       DSTRESULTSET* pResultSet)
{
  SQLRETURN rc = SQL_SUCCESS;
  SQLROWCOUNT iMaxRows = 0;
  BOOL bStatus = TRUE;
  SQLUSMALLINT iIndex = 0;
  DSTCOLUMN* pLastColumn = NULL;
  DSTCOLUMN* pThisColumn = NULL;
  char szColumnName [DST_RESULTSET_COLUMN_NAME_LENGTH] = "";


  DST_LOG_QUIET_ME ("DSTBindResultSet");


  /* validate params */
  assert (hstmt != SQL_NULL_HSTMT &&
    pResultSet != NULL);

  /* get global properties */
  PROPGetAsInt ("DST_MAX_ROWS", (SDWORD*) &iMaxRows);


  /* initialize the result set structure member variables */
  pResultSet->hstmt = hstmt;
  pResultSet->pColumns = NULL;
  pResultSet->iChecksum = 0;
  pResultSet->iRowCount = 0;


  /* get the number of columns associated with this result set */
  if (DSTGetResultColumnCount (hstmt, &pResultSet->iNumColumns))
  {
    /* does a result set exist? */
    if (pResultSet->iNumColumns <= 0)
    {
      DST_LOG_ERROR ("A result set is not associated "
        "with this statement handle.");
      assert (FALSE);
    }
  }
  else
  {
    /* SQLNumResultCols() failed */
    DST_LOG_UNREPORT_ME;
    return FALSE;
  }


  /* describe and allocate column structures for the result set */
  for (iIndex = 1; iIndex <= pResultSet->iNumColumns; iIndex ++)
  {

    pThisColumn = (DSTCOLUMN*) DST_MALLOC (sizeof (DSTCOLUMN));

    if (pThisColumn == NULL)
    {
      bStatus = FALSE;
      break;
    }

    /* initialize the column structure */
    pThisColumn->pszBuffer = NULL;
    pThisColumn->iBufferLength = 0;
    pThisColumn->iBytesReturned = 0;
    pThisColumn->iProCIndicator = 0;

    STRInitToSize (&pThisColumn->columnName,
      DST_RESULTSET_COLUMN_NAME_LENGTH);

    pThisColumn->iSqlType = 0;
    pThisColumn->iLength = 0;
    pThisColumn->iPrecision = 0;
    pThisColumn->iScale = 0;
    pThisColumn->iNullable = 0;
    pThisColumn->pNext = NULL;


    /* add the structure to the linked list */
    if (pLastColumn == NULL)
      pResultSet->pColumns = pThisColumn;
    else
      pLastColumn->pNext = pThisColumn;

    pLastColumn = pThisColumn;

#ifdef DST_PRC
    if (DSTIsProCStmt (hstmt))
    {
      /* get the column's Pro C attributes */
      rc = PRCDescribeCol (hstmt, iIndex,
        &pThisColumn->columnName,
        &pThisColumn->iSqlType,
        &pThisColumn->iLength,
        &pThisColumn->iPrecision,
        &pThisColumn->iScale,
        &pThisColumn->iNullable);

      if (!DSTIsSuccess (rc))
      {
        bStatus = FALSE;
        break;
      }

      /* set the Pro C buffer size to the length */
      /* plus 1 for the null terminator */
       pThisColumn->iBufferLength = (pThisColumn->iLength *
          DST_TT_MAX_BYTES_PER_CHAR) + 1;
    }
    else
#endif
    {
      /* get the metadata for this column */
      rc = SQLDescribeCol (pResultSet->hstmt,
        iIndex,
        (SQLCHAR*) szColumnName,
        DST_RESULTSET_COLUMN_NAME_LENGTH,
        NULL,
        &pThisColumn->iSqlType,
        &pThisColumn->iPrecision,
        &pThisColumn->iScale,
        &pThisColumn->iNullable);

      STRCopyCString (&pThisColumn->columnName,
        szColumnName);

      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV,
          SQL_NULL_HDBC,
          pResultSet->hstmt,
          "SQLDescribeCol () failed.");

        bStatus = FALSE;
        break;
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV,
          SQL_NULL_HDBC,
          pResultSet->hstmt,
          "SQLDescribeCol () info.");
      }


      /* get the display buffer size */
      rc = SQLColAttributes (pResultSet->hstmt,
        iIndex,
        SQL_COLUMN_DISPLAY_SIZE,
        0, 0, 0,
        &pThisColumn->iBufferLength);

      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV,
          SQL_NULL_HDBC,
          pResultSet->hstmt,
          "SQLColAttributes () failed.");

        bStatus = FALSE;
        break;
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV,
          SQL_NULL_HDBC,
          pResultSet->hstmt,
          "SQLColAttributes () info.");
      }

      /* SQL_COLUMN_DISPLAY_SIZE is expressed as */
      /* characters - not bytes. So to calculate a */
      /* maximum result set buffer size multiple */
      /* the display size by the maximum number of */
      /* bytes that may be required to store 1 character */
      /* in the connection character set plus a null */
      /* terminator. */
      pThisColumn->iBufferLength = (pThisColumn->iBufferLength *
        DST_TT_MAX_BYTES_PER_CHAR) + 1;
    }


    /* the buffer size must not be less than the minimum */
    if (pThisColumn->iBufferLength < DST_RESULTSET_COLUMN_MIN_BUFFER_SIZE)
      pThisColumn->iBufferLength = DST_RESULTSET_COLUMN_MIN_BUFFER_SIZE;

    /* the buffer size must not be greater than the maximum */
    if (pThisColumn->iBufferLength > DST_RESULTSET_COLUMN_MAX_BUFFER_SIZE)
      pThisColumn->iBufferLength = DST_RESULTSET_COLUMN_MAX_BUFFER_SIZE;

    /* allocate the data buffer */
    pThisColumn->pszBuffer = (char*) DST_MALLOC (pThisColumn->iBufferLength);

    if (pThisColumn->pszBuffer == NULL)
    {
      bStatus = FALSE;
      break;
    }

 } /* end describe loop */



  /* bind the column buffers for the result set */
  pThisColumn = pResultSet->pColumns;

  for (iIndex = 1; bStatus &&
    iIndex <= pResultSet->iNumColumns; iIndex ++)
  {

#ifdef DST_PRC
    if (DSTIsProCStmt (hstmt))
    {
      /* bind the Pro C result set column */
      rc = PRCBindColAsCString (hstmt, iIndex,
        pThisColumn->pszBuffer,
        (unsigned long)pThisColumn->iBufferLength,
        &pThisColumn->iProCIndicator,
        (unsigned long*)&pThisColumn->iBytesReturned);

      if (!DSTIsSuccess (rc))
      {
        bStatus = FALSE;
        break;
      }
    }
    else
#endif
    {
      /* bind the buffer to the column */
      if (pThisColumn->iSqlType == SQL_WCHAR ||
        pThisColumn->iSqlType == SQL_WVARCHAR ||
        pThisColumn->iSqlType == SQL_WLONGVARCHAR)
      {
        rc = SQLBindCol (pResultSet->hstmt,
          iIndex,
          SQL_C_WCHAR,
          pThisColumn->pszBuffer,
          pThisColumn->iBufferLength,
          &pThisColumn->iBytesReturned);
      }
      else
      {
        rc = SQLBindCol (pResultSet->hstmt,
          iIndex,
          SQL_C_CHAR,
          pThisColumn->pszBuffer,
          pThisColumn->iBufferLength,
          &pThisColumn->iBytesReturned);
      }


      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV,
          SQL_NULL_HDBC,
          pResultSet->hstmt,
          "SQLBindCol() failed.");

        bStatus = FALSE;
        break;
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV,
          SQL_NULL_HDBC,
          pResultSet->hstmt,
          "SQLBindCol() info.");
      }
    }

    /* goto the next column in the list */
    pThisColumn = pThisColumn->pNext;

  } /* end bind loop */



  /* set the maximum number of rows to return from */
  /* the fetch cycle */
  if (!DSTSetMaxRows (pResultSet->hstmt, iMaxRows))
    bStatus = FALSE;


  /* if result set initialization failed then free */
  /* any allocated storage */
  if (!bStatus)
  {
    DSTFreeResultSet (pResultSet);
  }
#ifdef DST_PRC
  else if (DSTIsProCStmt (hstmt))
  {
    /* associate this result set with the Pro*C hstmt */
    PRCMapResultSet (hstmt, pResultSet);
  }
#endif

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTFreeResultSet ()
*
* Description: Frees a result set structure initialized by
* the DSTBindResultSet () function.
*
* In:  pResultSet
*
* Out:
*
* In/Out:
*
*
*/
void DSTFreeResultSet (DSTRESULTSET* pResultSet)
{
  DSTCOLUMN* pColumn = NULL;
  DSTCOLUMN* pNextColumn = NULL;

  assert (pResultSet != NULL);
  pColumn = pResultSet->pColumns;

  while (pColumn != NULL)
  {
    STRFree (&pColumn->columnName);

    if (pColumn->pszBuffer != NULL)
    {
      free (pColumn->pszBuffer);
    }

    pNextColumn = pColumn->pNext;
    free (pColumn);
    pColumn = pNextColumn;
  }

  return;
}


/*
* Function: DSTAddBoundRowChecksum ()
*
* Description: Calculates a simple checksum for the current bound row value and
* adds it to the checksum stored in the DSTRESULTSET structure.
*
* In:  pResultSet
*
* Out:
*
* In/Out:
*
*
*/
void DSTAddBoundRowChecksum (DSTRESULTSET* pResultSet)
{
  SQLUINTEGER iChecksum = 0;
  DSTCOLUMN* pColumn = pResultSet->pColumns;

  /* Do not calculate checksums on Pro*C result sets. There are several */
  /* difficulties with calculating the maximum column buffer size and */
  /* determining the size of returned data. */
  if (DSTIsProCStmt (pResultSet->hstmt))
    return;


  /* for each column value, add the value of each stored byte to the checksum */
  while (pColumn != NULL)
  {
    SQLLEN iPos = 0; 
    
    /* the length of the data returned should never exceed the allocated */
    /* buffer size */
    assert (pColumn->iBytesReturned <= pColumn->iBufferLength);

    /* the buffer should always be allocated */
    assert (pColumn->pszBuffer != NULL);


    while (iPos < pColumn->iBytesReturned)
    {
      /* Note that pszBuffer in the DSTRESULTSET structure is defined as a */
      /* signed char instead of SQLCHAR (unsigned). The SQLCHAR cast below */
      /* is required to maintain consistent checksums across platforms. */
      iChecksum += ((SQLCHAR)pColumn->pszBuffer [iPos]);
      iPos ++;
    }

    pColumn = pColumn->pNext;
  }

  pResultSet->iChecksum += iChecksum;
  return;
}


/*
* Function: DSTGetResultSetChecksum ()
*
* Description: Calculates a simple checksum for the entire result set returned 
* by the given statement.
*
* In:  hdbc pszSql
*
* Out: piChecksum
*
* In/Out:
*
*
*/
BOOL DSTGetResultSetChecksum (HDBC hdbc, const char* pszSql, 
                              SQLULEN* piChecksum)
{
  BOOL bStatus = TRUE;
  HSTMT hstmt = SQL_NULL_HSTMT;

  /* report the function call */
  DST_LOG_QUIET_ME ("DSTGetResultSetChecksum");

  assert (hdbc != SQL_NULL_HDBC && pszSql != NULL && piChecksum != NULL);


  /* default output value */
  *piChecksum = 0;


  /* allocate the statement  */
  if (!DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DST_LOG_UNREPORT_ME;
    return FALSE;
  }


  /* execute the statement */
  if (DSTIsSuccess (DSTExecDirectStmt (hstmt, pszSql)))
  {
    /* was a result set returned */
    if (DSTIsResultSet (hstmt))
    {
      DSTRESULTSET resultSet;

      /* get the result set */
      if (DSTBindResultSet (hstmt, &resultSet))
      {
        if (!DSTPrintBoundResultSet (&resultSet, FALSE))
          bStatus = FALSE;


        /* assign the output variable */
        *piChecksum = resultSet.iChecksum;

        DSTFreeResultSet (&resultSet);
      }
      else
        bStatus = FALSE;
    }
  }
  else
    bStatus = FALSE;



  /* deallocate the statement */
  DSTFreeStmt (hstmt);


  DST_LOG_UNREPORT_ME;
  return bStatus;
}






/*
* Function: DSTPrintBoundResultSet ()
*
* Description: Iterates thru a result set initialized by the DSTBindResultSet() 
* function and prints each row. If bVertical is TRUE then each row is printed
* vertically so that the column name precedes the column's value.
*
* In: pResultSet, bVertical
*
* Out:
*
* In/Out:
*
* Global Properties:
* DST_MAX_ROWS
*
*/
BOOL DSTPrintBoundResultSet (DSTRESULTSET* pResultSet, BOOL bVertical)
{
  SQLRETURN rc = SQL_SUCCESS;
  BOOL bStatus = TRUE;
  DSTRING row, message;
  SQLROWCOUNT iMaxRows = 0;

  /* report the function call */
  DST_LOG_QUIET_ME ("DSTPrintBoundResultSet");

  assert (pResultSet != NULL &&
    pResultSet->hstmt != SQL_NULL_HSTMT);


  /* init. dynamic strings */
  STRInit (&row, "");
  STRInit (&message, "");


  /* get global properties */
  PROPGetAsInt ("DST_MAX_ROWS", (SDWORD*) &iMaxRows);


  /* display the rows */
  DST_LOG_VERBOSE;


  /* iterate through all of the rows in the result set */
  while (DSTIsSuccess (rc))
  {

    /* fetch the next row */
    rc = DSTFetch (pResultSet->hstmt);

    if (rc == SQL_NO_DATA_FOUND)
    {
      rc = SQL_SUCCESS;
      break;
    }
    else if (!DSTIsSuccess (rc))
    {
      /* unexpected error */
      bStatus = FALSE;
      break;
    }

    /* increment the row count */
    pResultSet->iRowCount ++;

    /* calculate the checksum */
    DSTAddBoundRowChecksum (pResultSet);


    /* write out the current result set row  */
    if (bVertical)
    {
      DSTGetBoundRowAsString (pResultSet, &row, TRUE);
      DST_LOG ("");
    }
    else
      DSTGetBoundRowAsString (pResultSet, &row, FALSE);
    
    DST_LOG (STRC (row));

  } /* end fetch loop */


  /* close the cursor */
  DSTCloseStmtCursor (pResultSet->hstmt);


  /* insert the record of this result set into the TEST_STMT_RESULT_SET table */
  DSTPostBoundResultSet (pResultSet);



  /* linefeed */
  DST_LOG ("");

  if (iMaxRows > 0)
  {
    /* report the value of SQL_MAX_ROWS  */
    /* note that the ODBC call to set the maximum rows was */
    /* executed in DSTBindResultSet () */
    STRCopyCString (&message, "SQL_MAX_ROWS: %llu");
    STRInsertUnsignedBigInt (&message, iMaxRows);
    DST_LOG (STRC (message));
  }

  /* report the number of rows returned */
  STRCopyCString (&message, "Rows returned: %llu");
  STRInsertUnsignedBigInt (&message, pResultSet->iRowCount);
  DST_LOG (STRC (message));


  STRFree (&row);
  STRFree (&message);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetBoundRowAsString ()
*
* Description: When bVertical is FALSE the currently bound result set row is 
* returned as a line where each column value is delimited by '<' and '>'. When 
* bVertical is TRUE then the current result set row is returned as a set of 
* lines where each line includes the result set column name and its associated 
* value.
*
* In: pResultSet, bVertical
*
* Out: pRow
*
* In/Out:
*
*
*/
void DSTGetBoundRowAsString (DSTRESULTSET* pResultSet, DSTRING* pRow,
                             BOOL bVertical)
{
  DSTCOLUMN* pColumn = pResultSet->pColumns;
  DSTRING formattedValue;

  assert (pResultSet != NULL && pRow !=NULL);

  STRInit (&formattedValue, "");


  /* clear the output row buffer */
  STRCopyCString (pRow, "");

  /* iterate through each column in the current row */
  while (pColumn != NULL)
  {
    /* format the column's value */
    if (DSTIsProCStmt (pResultSet->hstmt) &&
      pColumn->iProCIndicator == SQL_NULL_DATA)
    {
      STRCopyCString (&formattedValue, DST_NULL_STRING);
    }
    else if (pColumn->iBytesReturned == SQL_NULL_DATA)
    {
      STRCopyCString (&formattedValue, DST_NULL_STRING);
    }
    else if (pColumn->iSqlType == SQL_WCHAR ||
      pColumn->iSqlType == SQL_WVARCHAR ||
      pColumn->iSqlType == SQL_WLONGVARCHAR)
    {
      /* display the UTF16 buffer as a Unicode escape sequence */
      STRCopyUTF16Buffer (&formattedValue, (UTF16*) pColumn->pszBuffer,
        pColumn->iBytesReturned);
    }
    else
    {
      STRCopyCString (&formattedValue,
        pColumn->pszBuffer);
    }


    if (bVertical)
    {
      /* add the column's name */
      STRPrependCString (&formattedValue, "\n%s: ");
      STRInsertString (&formattedValue, pColumn->columnName);
    }
    else
    {
      /* append the column value to the current row */
      STRPrependCString (&formattedValue, "<");
      STRAppendCString (&formattedValue, "> ");
    }

    
    /* append to the output buffer */
    STRAppend (pRow, formattedValue);

    /* goto the next column */
    pColumn = pColumn->pNext;
  }


  STRFree (&formattedValue);

  return;
}


/*
* Function: DSTPrintResultSet ()
*
* Description: Prints the result set for the specified statement handle. The 
* result set is printed via calls to SQLGetData (). If bVertical is TRUE then
* the each row is printed so that the column's name precedes the column's
* value on separate lines.
*
* In:  hstmt, bVertical
*
* Out:
*
* In/Out:
*
* Global Properties:
* DST_MAX_ROWS
*
*/
BOOL DSTPrintResultSet (HSTMT hstmt, BOOL bVertical)
{

  /* locals */
  SQLRETURN rc = SQL_SUCCESS;
  BOOL bStatus = TRUE;
  SQLROWCOUNT iRowsReturned = 0;
  SQLROWCOUNT iMaxRows = 0;
  DSTRING row, message;


  /* report the function call */
  DST_LOG_QUIET_ME ("DSTPrintResultSet");


  assert (hstmt != SQL_NULL_HSTMT);


  /* get global properties */
  PROPGetAsInt ("DST_MAX_ROWS", (SDWORD*) &iMaxRows);


  /* init. dynamic strings */
  STRInit (&row, "");
  STRInit (&message, "");


  /* set the maximum number of rows to return from */
  /* the fetch cycle */
  if (!DSTSetMaxRows (hstmt, iMaxRows))
    bStatus = FALSE;



  DST_LOG_VERBOSE;

  /* iterate through all of the rows in the result set */
  while (bStatus && DSTIsSuccess (rc))
  {

    /* fetch the next row */
    rc = DSTFetch (hstmt);

    if (rc == SQL_NO_DATA_FOUND)
    {
      rc = SQL_SUCCESS;
      break;
    }
    else if (!DSTIsSuccess (rc))
    {
      /* unexpected error */
      bStatus = FALSE;
      break;
    }


    /* increment the row counter */
    iRowsReturned ++;

    /* write out the current row  */
    if (bVertical)
    {
      DSTGetRowAsString (hstmt, &row, TRUE);
      DST_LOG ("");
    }
    else
      DSTGetRowAsString (hstmt, &row, FALSE);
    
    DST_LOG (STRC (row));

  } /* end fetch loop */


  /* close the cursor */
  DSTCloseStmtCursor (hstmt);



  /* linefeed */
  DST_LOG ("");

  if (iMaxRows > 0)
  {
    /* report the value of SQL_MAX_ROWS - if it was called  */
    STRCopyCString (&message, "SQL_MAX_ROWS: %llu");
    STRInsertUnsignedBigInt (&message, iMaxRows);
    DST_LOG (STRC (message));
  }

  /* report the number of rows returned */
  STRCopyCString (&message, "Rows returned: %llu");
  STRInsertUnsignedBigInt (&message, iRowsReturned);
  DST_LOG (STRC (message));


  /* free dynamic strings */
  STRFree (&row);
  STRFree (&message);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetRowAsString ()
*
* Description: Returns the current result set row associated with the statement
* handle as a string via calls to SQLGetData (). When bVertical is FALSE the 
* result set row is returned as a line where each column value is delimited by 
* '<' and '>'. When bVertical is TRUE then the current result set row is 
* returned as a set of lines where each line includes the result set column name
* and its associated value.
*
* In: hstmt, bVertical
*
* Out: pRow
*
* In/Out:
*
*
*/
SQLRETURN DSTGetRowAsString (HSTMT hstmt, DSTRING* pRow, BOOL bVertical)
{
  SQLRETURN rc = SQL_SUCCESS;
  DSTRING formattedValue;
  SWORD iColumn, iColumnCount = 0;
  SWORD iSqlType = 0;

  DST_LOG_QUIET_ME ("DSTGetRowAsString");


  assert (hstmt != NULL && pRow != NULL);


  STRInit (&formattedValue, "");


  /* get the column count */
  DSTGetResultColumnCount (hstmt, &iColumnCount);


  /* clear the output row buffer */
  STRCopyCString (pRow, "");


  /* iterate through each column in the current row */
  for (iColumn = 1; iColumn <= iColumnCount; iColumn++)
  {

    if (DSTIsProCStmt (hstmt))
    {
      rc = DSTGetColAsString (hstmt, iColumn,
          &formattedValue);
    }
    else
    {
      /* get the column's SQL type */
      rc = SQLDescribeCol (hstmt, iColumn,
        NULL, 0, NULL,
        &iSqlType,
        NULL, NULL, NULL);


      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV,
          SQL_NULL_HDBC, hstmt,
          "SQLDescribeCol () failed.");

        break;
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        /* Note: SQLDescribeCol () will return a warning
           if the ColumnName output param. is null - this
           probably shouldn't happen so ignore it

        DST_LOG_SQL_INFO (SQL_NULL_HENV,
          SQL_NULL_HDBC, hstmt,
          "SQLDescribeCol () info.");

        */
      }


      /* get the column's value */
      if (iSqlType == SQL_WCHAR ||
        iSqlType == SQL_WVARCHAR ||
        iSqlType == SQL_WLONGVARCHAR)
      {
        SQLLEN iByteLength = 0;
        UTF16* pData = NULL;

        rc = DSTGetColAsUTF16 (hstmt, iColumn,
          &pData, &iByteLength);

        if (pData != NULL)
        {
          STRCopyUTF16Buffer (&formattedValue,
            pData, (size_t) iByteLength);

          free (pData);
        }
        else
        {
          STRCopyCString (&formattedValue, DST_NULL_STRING);
        }
      }
      else
      {
        rc = DSTGetColAsString (hstmt, iColumn,
          &formattedValue);
      }
    }


    if (bVertical)
    {
      char szColumnName [DST_RESULTSET_COLUMN_NAME_LENGTH] = "";

      /* get the name of this column */
      rc = SQLDescribeCol (hstmt, iColumn,
        (SQLCHAR*) szColumnName,
        DST_RESULTSET_COLUMN_NAME_LENGTH,
        NULL, NULL, NULL, NULL, NULL);


      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV,
          SQL_NULL_HDBC, hstmt,
          "SQLDescribeCol () failed.");

        break;
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV,
          SQL_NULL_HDBC, hstmt,
          "SQLDescribeCol () info.");
      }


      /* add the column's name */
      STRPrependCString (&formattedValue, "\n%s: ");
      STRInsertCString (&formattedValue, szColumnName);
    }
    else
    {
      /* append the column value to the current row */
      STRPrependCString (&formattedValue, "<");
      STRAppendCString (&formattedValue, "> ");
    }

    STRAppend (pRow, formattedValue);


    /* break out if SQLGetData () failed */
    if (!DSTIsSuccess (rc))
      break;
  }


  STRFree (&formattedValue);


  DST_LOG_UNREPORT_ME;
  return rc;
}



/*
* Function: DSTGetColAsString ()
*
* Description: Returns the contents of a given column as a
* DSTRING object. The DSTRING output parameter is sized to
* fit the result. This procedure uses a SQLGetData ()
* SQL_C_CHAR binding to retrieve the result. If the column
* value is NULL then STRC (*pValue) == '<NULL>'.
*
* In: hstmt, iColumn
*
* Out: pValue
*
* In/Out:
*
*/
SQLRETURN DSTGetColAsString (HSTMT hstmt,
                             SQLUSMALLINT iColumn,
                             DSTRING* pValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCGetColAsString (hstmt, iColumn, pValue);
#endif


  {
    /* locals */
    SQLRETURN rc;
    SQLLEN iBytesReturned = SQL_NULL_DATA;
    SQLLEN iMaxColDisplaySize = 0;
    char* pszBuffer = NULL;

    DST_LOG_QUIET_ME ("DSTGetColAsString");

    /* determine the maximum possible display size of */
    /* the the data */
    rc = SQLColAttributes (hstmt,
      iColumn,
      SQL_COLUMN_DISPLAY_SIZE,
      NULL, 0, NULL,
      &iMaxColDisplaySize);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV,
        SQL_NULL_HDBC, hstmt,
        "SQLColAttributes () failed.");

      DST_LOG_UNREPORT_ME;
      return rc;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV,
        SQL_NULL_HDBC, hstmt,
        "SQLColAttributes () info.");
    }

    /* SQL_COLUMN_DISPLAY_SIZE is expressed as */
    /* characters - not bytes. So to calculate a */
    /* maximum result set buffer size multiple */
    /* the display size by the maximum number of */
    /* bytes that may be required to store 1 character */
    /* in the connection character set plus a null */
    /* terminator. */
    iMaxColDisplaySize = (iMaxColDisplaySize *
      DST_TT_MAX_BYTES_PER_CHAR) + 1;

    /* the buffer size must not be less than the minimum */
    if (iMaxColDisplaySize  < DST_RESULTSET_COLUMN_MIN_BUFFER_SIZE)
      iMaxColDisplaySize = DST_RESULTSET_COLUMN_MIN_BUFFER_SIZE;

    /* the display size must not be greater than the maximum */
    if (iMaxColDisplaySize > DST_RESULTSET_COLUMN_MAX_BUFFER_SIZE)
      iMaxColDisplaySize = DST_RESULTSET_COLUMN_MAX_BUFFER_SIZE;


    /* allocate an output buffer */
    pszBuffer = DST_MALLOC (iMaxColDisplaySize);


    if (pszBuffer != NULL)
    {
      /* convert the type to a SQL_C_CHAR for output */
      rc = SQLGetData (hstmt,
        iColumn,
        SQL_C_CHAR,
        pszBuffer,
        iMaxColDisplaySize,
        &iBytesReturned);


      /* SQL_NO_DATA_FOUND should never be returned */
      assert (rc != SQL_NO_DATA_FOUND);

      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV,
          SQL_NULL_HDBC, hstmt,
          "SQLGetData () failed.");
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV,
          SQL_NULL_HDBC, hstmt,
          "SQLGetData () info.");
      }

      /* is this value NULL? */
      if (iBytesReturned == SQL_NULL_DATA)
      {
        STRCopyCString (pValue, DST_NULL_STRING);
      }
      else
      {
        STRCopyCString (pValue, pszBuffer);
      }
    }
    else
    {
      /* malloc failure */
      rc = SQL_ERROR;
    }


    /* free the local buffer */
    if (pszBuffer != NULL)
    {
      free (pszBuffer);
    }


    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTGetColAsUTF16 ()
*
* Description: Returns the contents of a given column as a
* an array of UTF16 characters. This procedure uses a
* SQLGetData() SQL_C_WCHAR binding to retrieve the column's
* data. The input pointer to a pointer is allocated by this
* procedure and the caller is responsible for freeing this
* memory block after use. If the column value is SQL NULL
* then *ppData == NULL and *piBytesWritten = SQL_NULL_DATA.
*
* In: hstmt, iColumn
*
* Out: ppValue, piBytesWritten
*
* In/Out:
*
*/
SQLRETURN DSTGetColAsUTF16 (HSTMT hstmt,
                            SQLUSMALLINT iColumn,
                            UTF16** ppData,
                            SQLLEN* piBytesWritten)
{
  SQLRETURN rc;
  SQLLEN iMaxColDisplaySize = 0;
  SQLLEN iBytesWritten = SQL_NULL_DATA;


  DST_LOG_QUIET_ME ("DSTGetColAsUTF16");

  assert (ppData != NULL);

  /* set the default output parameters */
  *ppData = NULL;



  /* determine the maximum amount of data that */
  /* may be transferred */
  rc = SQLColAttributes (hstmt,
    iColumn,
    SQL_COLUMN_DISPLAY_SIZE,
    NULL, 0, NULL,
    &iMaxColDisplaySize);


  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV,
      SQL_NULL_HDBC, hstmt,
      "SQLColAttributes () failed.");

    DST_LOG_UNREPORT_ME;
    return rc;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV,
      SQL_NULL_HDBC, hstmt,
      "SQLColAttributes () info.");
  }



  /* SQL_COLUMN_DISPLAY_SIZE is expressed as */
  /* characters - not bytes. So to calculate a */
  /* maximum result set buffer size multiple */
  /* the display size by the maximum number of */
  /* bytes that may be required to store 1 character */
  /* in the connection character set plus a null */
  /* terminator. */
  iMaxColDisplaySize = (iMaxColDisplaySize *
    DST_TT_MAX_BYTES_PER_CHAR) + DST_TT_MAX_BYTES_PER_CHAR;


  /* the display size must not be greater than the maximum */
  if (iMaxColDisplaySize > DST_RESULTSET_COLUMN_MAX_BUFFER_SIZE)
    iMaxColDisplaySize = DST_RESULTSET_COLUMN_MAX_BUFFER_SIZE;


  /* allocate an output buffer */
  *ppData = DST_MALLOC (iMaxColDisplaySize);


  if (*ppData != NULL)
  {
    /* convert the type to a SQL_C_WCHAR for output */
    rc = SQLGetData (hstmt,
      iColumn,
      SQL_C_WCHAR,
      *ppData,
      iMaxColDisplaySize,
      &iBytesWritten);


    /* SQL_NO_DATA_FOUND should never be returned */
    assert (rc != SQL_NO_DATA_FOUND);

    /* did the call succeed? */
    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV,
        SQL_NULL_HDBC, hstmt,
        "SQLGetData () failed.");

      /* clean up */
      free (*ppData);
      *ppData = NULL;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV,
        SQL_NULL_HDBC, hstmt,
        "SQLGetData () info.");
    }


    /* set the output length/NULL indicator */
    if (piBytesWritten != NULL)
    {
      *piBytesWritten = iBytesWritten;
    }

    /* is this value NULL? */
    if (iBytesWritten == SQL_NULL_DATA)
    {
      free (*ppData);
      *ppData = NULL;
    }

  }
  else
  {
    /* malloc failure */
    rc = SQL_ERROR;
  }



  DST_LOG_UNREPORT_ME;
  return rc;
}





/*
* Function: DSTGetColAsInt ()
*
* Description: Returns the contents of a given column as a
* signed integer. This procedure wraps SQLGetData (). If
* the column value is NULL then *piValue == 0.
*
* In: hstmt, iColumn
*
* Out: piValue
*
* In/Out:
*
*/
SQLRETURN DSTGetColAsInt (HSTMT hstmt,
                          SQLUSMALLINT iColumn,
                          SDWORD *piValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCGetColAsInt (hstmt, iColumn, piValue);
#endif

  {
    /* locals */
    SQLRETURN rc;
    SQLLEN cbValue = SQL_NULL_DATA;

    DST_LOG_QUIET_ME ("DSTGetColAsInt");

    /* convert the type to a SQL_C_SLONG for output */
    rc = SQLGetData (hstmt, iColumn,
      SQL_C_SLONG ,
      piValue, 0, &cbValue);


    /* SQL_NO_DATA_FOUND should never be returned */
    assert (rc != SQL_NO_DATA_FOUND);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLGetData () failed.");

      DST_LOG_UNREPORT_ME;
      return rc;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLGetData () info.");
    }

    /* if NULL is returned then set *piValue = 0 */
    if (cbValue == SQL_NULL_DATA)
    {
      *piValue = 0;
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTGetColAsUnsignedInt ()
*
* Description: Returns the contents of a given column as an
* unsigned integer. This procedure wraps SQLGetData (). If
* the column value is NULL then *piValue == 0.
*
* In: hstmt, iColumn
*
* Out: piValue
*
* In/Out:
*
*/
SQLRETURN DSTGetColAsUnsignedInt (HSTMT hstmt,
                                  SQLUSMALLINT iColumn,
                                  UDWORD *piValue)
{
  /* locals */
  SQLRETURN rc;
  SQLLEN cbValue = SQL_NULL_DATA;


  DST_LOG_QUIET_ME ("DSTGetColAsUnsignedInt");


  /* convert the type to a SQL_C_ULONG for output */
  rc = SQLGetData (hstmt, iColumn,
    SQL_C_ULONG ,
    piValue, 0, &cbValue);


  /* SQL_NO_DATA_FOUND should never be returned */
  assert (rc != SQL_NO_DATA_FOUND);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLGetData () failed.");

    DST_LOG_UNREPORT_ME;
    return rc;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLGetData () info.");
  }

  /* if NULL is returned then set *piValue = 0 */
  if (cbValue == SQL_NULL_DATA)
  {
    *piValue = 0;
  }

  DST_LOG_UNREPORT_ME;
  return rc;
}

/*
* Function: DSTGetColAsBigInt ()
*
* Description: Returns the contents of a given column as an SQLBIGINT. This 
* procedure wraps SQLGetData (). If the column value is NULL then *piValue == 0.
*
* In: hstmt, iColumn
*
* Out: piValue
*
* In/Out:
*
*/
SQLRETURN DSTGetColAsBigInt (HSTMT hstmt, 
                             SQLUSMALLINT iColumn,
                             SQLBIGINT *piValue)
{
  /* locals */
  SQLRETURN rc;
  SQLLEN cbValue = SQL_NULL_DATA;


  DST_LOG_QUIET_ME ("DSTGetColAsBigInt");


  /* convert the type to a SQL_C_ULONG for output */
  rc = SQLGetData (hstmt, iColumn,
    SQL_C_SBIGINT ,
    piValue, 0, &cbValue);


  /* SQL_NO_DATA_FOUND should never be returned */
  assert (rc != SQL_NO_DATA_FOUND);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLGetData () failed.");

    DST_LOG_UNREPORT_ME;
    return rc;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLGetData () info.");
  }

  /* if NULL is returned then set *piValue = 0 */
  if (cbValue == SQL_NULL_DATA)
  {
    *piValue = 0;
  }

  DST_LOG_UNREPORT_ME;
  return rc;
}


/*
* Function: DSTSetParamAsString ()
*
* Description: Sets the specified parameter in the
* prepared statement associated with hstmt as an string
* value.
*
* In: hstmt, iParam, pszValue
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsString (const HSTMT hstmt,
                               const SWORD iParam,
                               const char* pszValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCSetParamAsString (hstmt, iParam, pszValue);
#endif
  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTSetParamAsString");


    /* the SQL type used here was changed from SQL_CHAR */
    /* to SQL_VARCHAR to avoid BugDb #7167389 */
    rc = SQLBindParameter (hstmt, iParam,
      SQL_PARAM_INPUT_OUTPUT, SQL_C_CHAR,
      SQL_VARCHAR , (SQLULEN) strlen (pszValue), 0,
      (SQLPOINTER) pszValue, 0, NULL);


    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () info.");
    }


    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTSetParamAsUTF16 ()
*
* Description: Sets the specified statement parameter
* to the value of the UFTF16 buffer.
*
* In: hstmt, iParam, pData, piDataSize
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsUTF16 (const HSTMT hstmt,
                              const SWORD iParam,
                              const UTF16* pData,
                              SQLLEN* piDataSize)
{
  /* locals */
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTSetParamAsUTF16");

  rc = SQLBindParameter (hstmt, iParam,
    SQL_PARAM_INPUT_OUTPUT, SQL_C_WCHAR,
    SQL_WCHAR, (*piDataSize) / 2, 0,
    (SQLPOINTER) pData, 0, piDataSize);


  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLBindParameter () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLBindParameter () info.");
  }

  DST_LOG_UNREPORT_ME;
  return rc;
}



/*
* Function: DSTSetParamAsInt ()
*
* Description: Sets the specified parameter in
* the prepared statement associated with hstmt as
* an SDWORD value.
*
* In: hstmt, iParam, piValue
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsInt (const HSTMT hstmt,
                            const SWORD iParam,
                            const SDWORD *piValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCSetParamAsInt (hstmt, iParam, piValue);
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTSetParamAsInt");

    rc = SQLBindParameter (hstmt,
      iParam, SQL_PARAM_INPUT_OUTPUT,
      SQL_C_SLONG, SQL_INTEGER, 0, 0,
      (SQLPOINTER) piValue, 0, NULL);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () info.");
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTSetParamAsUnsignedInt ()
*
* Description: Sets the specified parameter in the prepared
* statement associated with hstmt as an UDWORD value.
*
* In: hstmt, iParam, piValue
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsUnsignedInt (const HSTMT hstmt,
                                    const SWORD iParam,
                                    const UDWORD *piValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCSetParamAsUnsignedInt (hstmt, iParam, piValue);
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTSetParamAsUnsignedInt");

    rc = SQLBindParameter (hstmt,
      iParam, SQL_PARAM_INPUT_OUTPUT,
      SQL_C_ULONG, SQL_INTEGER, 0, 0,
      (SQLPOINTER) piValue, 0, NULL);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () info.");
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}




/*
* Function: DSTSetParamAsBigInt ()
*
* Description: Sets the specified parameter in the
* prepared statement associated with hstmt as a
* SQLBIGINT value.
*
* In: hstmt, iParam, piValue
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsBigInt (const HSTMT hstmt,
                               const SWORD iParam,
                               SQLBIGINT *piValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCSetParamAsBigInt (hstmt, iParam, piValue);
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTSetParamAsBigInt");

    rc = SQLBindParameter (hstmt,
      iParam, SQL_PARAM_INPUT_OUTPUT,
      SQL_C_SBIGINT, SQL_BIGINT, 0, 0,
      (SQLPOINTER) piValue, 0, NULL);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () info.");
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTSetParamAsDouble ()
*
* Description: Sets the specified parameter in the prepared
* statement associated with hstmt as a floating point double
* value.
*
* In: hstmt, iParam, pdValue
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsDouble (const HSTMT hstmt,
                              const SWORD iParam,
                              double *pdValue)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return SQL_ERROR; /* this is not yet supported via Pro*C */
#endif

  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTSetParamAsDouble");

    rc = SQLBindParameter (hstmt,
      iParam, SQL_PARAM_INPUT_OUTPUT,
      SQL_C_DOUBLE, SQL_DOUBLE, 0, 0,
      (SQLPOINTER) pdValue, 0, NULL);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () info.");
    }

    DST_LOG_UNREPORT_ME;
    return rc;
  }
}



/*
* Function: DSTSetParamAsNull ()
*
* Description: Sets the specified parameter in
* the prepared statement associated with hstmt as
* a NULL value.
*
* In: hstmt, iParam
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsNull (const HSTMT hstmt,
                             const SWORD iParam)
{
#ifdef DST_PRC
  if (DSTIsProCStmt (hstmt))
    return PRCSetParamAsNull (hstmt, iParam);
#endif
  {
    /* locals */
    SQLRETURN rc;

    DST_LOG_QUIET_ME ("DSTSetParamAsNull");

    /* set the target SQL type to a single char */
    rc = SQLBindParameter (hstmt,
      iParam, SQL_PARAM_INPUT,
      SQL_C_DEFAULT, SQL_CHAR, 1, 0,
      NULL, 0, &giNullValue);


    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
        "SQLBindParameter () info.");
    }


    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTSetParamAsRefCursor ()
*
* Description: Sets the specified parameter as a REF
* CURSOR statement handle.
*
* In: hstmt, iParam, hstmtRefCursor
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTSetParamAsRefCursor (const HSTMT hstmt,
                                  const SWORD iParam,
                                  HSTMT hstmtRefCursor)
{
  /* locals */
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTSetParamAsRefCursor");


  rc = SQLBindParameter (hstmt, iParam, SQL_PARAM_OUTPUT,
    SQL_C_REFCURSOR, SQL_REFCURSOR, 0, 0,
    hstmtRefCursor, 0, 0);


  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLBindParameter () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLBindParameter () info.");
  }


  DST_LOG_UNREPORT_ME;
  return rc;
}



/* ---------------------------------------------------------------- */
/* Metadata Functions. */

/*
* Function: DSTGetConnOption ()
*
* Description: This returns a connection option value
* in the location pointed to by pValue. If the value
* returned is a string then the caller is responsible
* for allocating a string buffer of at least
* SQL_MAX_OPTION_STRING_LENGTH + 1 bytes.
*
*
* In:  hdbc, iOption
*
* Out: pValue
*
* In/Out:
*
*/
BOOL DSTGetConnOption (const HDBC hdbc,
                       SQLUSMALLINT iOption,
                       SQLPOINTER* pValue)
{
  BOOL bStatus = TRUE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTGetConnOption");

  /* Pro*C connections cannot do this */
  if (DSTIsProCConn (hdbc))
  {
    DST_LOG_ERROR ("DSTGetConnOption () is not available for Pro*C connections.");
    DST_LOG_UNREPORT_ME;
    return TRUE;
  }


  rc = SQLGetConnectOption (hdbc, iOption, pValue);

  if (!DSTIsSuccess (rc))
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetConnectOption () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetConnectOption () info.");
  }

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTSetConnOption ()
*
* Description: This sets a connection option using
* the value of iValue.
*
* In:  hdbc, iOption, iValue
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetConnOption (const HDBC hdbc,
                       SQLUSMALLINT iOption,
                       SQLULEN iValue)
{
  BOOL bStatus = TRUE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTSetConnOption");

  /* Pro*C connections cannot do this */
  if (DSTIsProCConn (hdbc))
  {
    DST_LOG_ERROR ("DSTSetConnOption () is not available for Pro*C connections.");
    DST_LOG_UNREPORT_ME;
    return TRUE;
  }


  rc = SQLSetConnectOption (hdbc, iOption, iValue);

  if (!DSTIsSuccess (rc))
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLSetConnectOption () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLSetConnectOption () info.");
  }

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTGetStmtOption ()
*
* Description: This returns a statement option value
* in the location pointed to by pValue. If the value
* returned is a string then the caller is responsible
* for allocating a string buffer of at least
* SQL_MAX_OPTION_STRING_LENGTH + 1 bytes.
*
*
* In:  hstmt, iOption
*
* Out: pValue
*
* In/Out:
*
*/
BOOL DSTGetStmtOption (const HSTMT hstmt,
                       SQLUSMALLINT iOption,
                       SQLPOINTER* pValue)
{
  BOOL bStatus = TRUE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTGetStmtOption");

  /* Pro*C connections cannot do this */
  if (DSTIsProCStmt (hstmt))
  {
    DST_LOG_ERROR ("DSTGetStmtOption () is not available for Pro*C connections.");
    DST_LOG_UNREPORT_ME;
    return TRUE;
  }


  rc = SQLGetStmtOption (hstmt, iOption, pValue);

  if (!DSTIsSuccess (rc))
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLGetStmtOption () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLGetStmtOption () info.");
  }

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTSetStmtOption ()
*
* Description: This sets a statement option using
* the value of iValue.
*
* In:  hstmt, iOption, iValue
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetStmtOption (const HSTMT hstmt,
                       SQLUSMALLINT iOption,
                       SQLULEN iValue)
{
  BOOL bStatus = TRUE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTSetStmtOption");

  /* Pro*C connections cannot do this */
  if (DSTIsProCStmt (hstmt))
  {
    DST_LOG_ERROR ("DSTSetStmtOption () is not available for Pro*C connections.");
    DST_LOG_UNREPORT_ME;
    return TRUE;
  }


  rc = SQLSetStmtOption (hstmt, iOption, iValue);

  if (!DSTIsSuccess (rc))
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLSetStmtOption () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLSetStmtOption () info.");
  }

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetObjectOwner ()
*
* Description: This procedure attempts to determine the owner
* of a given object by searching for the object in the entire
* database. If an owner is not found then FALSE is returned.
*
* In:  hdbc, pszObjectName
*
* Out: pObjectOwner
*
* In/Out:
*
*/
BOOL DSTGetObjectOwner (HDBC hdbc, 
                        const char* pszObjectName,
                        DSTRING* pObjectOwner)
{
  BOOL bStatus = FALSE;
  HSTMT hstmt;
  DSTRING query;

  DST_LOG_QUIET_ME ("DSTGetObjectOwner");

  /* query all objects */
  STRInit (&query, 
    "SELECT TRIM (OWNER) "
    "FROM (%s) "
    "WHERE TRIM (NAME) = :p1 "
    "ORDER BY PRECEDENCE");
  STRInsertCString (&query, DST_OBJECT_TYPE_QUERY);

  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DSTSetParamAsString (hstmt, 1, pszObjectName);
    DSTExecDirectStmt (hstmt, STRC (query));

    if (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsString (hstmt, 1, pObjectOwner);

      /* success */
      bStatus = TRUE;
    }

    DSTFreeStmt (hstmt);
  }

  STRFree (&query);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}





/*
* Function: DSTGetRandomObject ()
*
* Description: This function returns a randomly selected object name
* and owner in pObjOwner and pObjName. The pszObjTypeList and
* pszNotObjTypeList parameters specify a string of space delimited object
* types to randomly choose or not choose from. Object type identifiers may
* be part of a hierarchy of types from general to specific. Some object types
* like indexes will return a second qualifier besides the object's
* owner. In the case of indexes this is the name of the table associated
* with the index. In these cases the additional qualifier is returned in
* pObjQualifier if it is not NULL. System objects are never returned by this
* procedure. If bIncludeSynonyms is TRUE then synonyms that directly reference
* the target object types will be included in the set of objects to select
* from.
*
* The list of the valid objects type identifiers:
*
* "TABLE"
*   "TABLE_GLOBAL_TEMPORARY"
*   "TABLE_VIEW"
*     "TABLE_VIEW_LOGICAL"
*     "TABLE_VIEW_MATERIALIZED"
*       "TABLE_VIEW_MATERIALIZED_ASYNCHRONOUS"
*       "TABLE_VIEW_MATERIALIZED_SYNCHRONOUS"
* "SEQUENCE"
* "SYNONYM"
*   "SYNONYM_PUBLIC"
* "INDEX"
*   "INDEX_UNIQUE_COLUMN"
*   "INDEX_PRIMARY_KEY"
*   "INDEX_NLSSORT"
* "CACHE_GROUP"
* "REPLICATION_SCHEME"
*
* If no objects are available to select randomly from then FALSE is
* returned.
*
* In:  hdbc, pszObjTypeList, pszNotObjTypeList, bIncludeSynonyms
*
* Out: pObjOwner, pObjName, pObjQualifier
*
* In/Out:
*
*/
BOOL DSTGetRandomObject (const HDBC hdbc, const char* pszObjTypeList,
                         const char* pszNotObjTypeList, BOOL bIncludeSynonyms,
                         DSTRING* pObjOwner, DSTRING* pObjName,
                         DSTRING* pObjQualifier)
{
  BOOL bStatus = FALSE;
  SDWORD iNumObjs = 0, iTargetObj = 0;
  DSTRING query;
  DSTRING objTypeList, objTypePredicates, notObjTypePredicates;
  DSTRING mssg, type;
  HSTMT hstmt;

  DST_LOG_REPORT_ME ("DSTGetRandomObject");

  STRInit (&query,
    "SELECT TYPE, RTRIM (OWNER), RTRIM (NAME), RTRIM (QUALIFIER) FROM (\n"
    DST_OBJECT_TYPE_QUERY
    ") OBJECTS \n"

    "WHERE OBJECTS.NAME NOT LIKE 'MV%$%' \n"
    "AND OBJECTS.NAME NOT LIKE 'MGT%$%' \n"
    "AND OBJECTS.NAME NOT LIKE 'CD$%' \n" /* exclude compressed column tables */

    /* This is a temporary fix to avoid bug #27195304 which results in a hang */
    /* in grid mode when operating on global temp. tables. */
    "AND RTRIM (OBJECTS.OWNER) = USER \n"
    "AND TYPE != 'TABLE_GLOBAL_TEMPORARY' \n"
    
    "AND RTRIM (OBJECTS.OWNER) NOT IN ('SYS', 'SYSTEM', 'TTREP', 'GRID', "
    "'" DST_TEST_DATA_SCHEMA "', '" DST_TEST_RESULTS_SCHEMA "') \n");


  STRInit (&type, "");
  STRInit (&mssg, "");
  STRInit (&objTypePredicates, "");
  STRInit (&objTypeList, "");
  STRInit (&notObjTypePredicates, "");


  /* construct the type predicates to select from by processing each type */
  /* in pszObjTypeList */
  if (pszObjTypeList != NULL)
  {
    size_t iIndex, iNumTypes;

    /* report the inclusion list */
    STRCopyCString (&mssg, "Included object types: ");
    STRAppendCString (&mssg, pszObjTypeList);
    DST_LOG (STRC (mssg));

    STRCopyCString (&objTypeList, pszObjTypeList);
    iNumTypes = STRGetNumTokens (objTypeList, STR_WHITESPACE_CHARS);

    if (iNumTypes > 0)
    {
      STRCopyCString (&objTypePredicates, "AND (");

      for (iIndex = 1; iIndex <= iNumTypes; iIndex ++)
      {
        /* get this type */
        STRGetTokenAt (objTypeList, iIndex, STR_WHITESPACE_CHARS, &type);

        /* add this type to the predicate list */
        STRAppendCString (&objTypePredicates, "INSTR (TYPE, '");
        STRAppend (&objTypePredicates, type);
        STRAppendCString (&objTypePredicates, "') != 0 OR ");
      }

      /* append the predicates to the query */
      STRChop (&objTypePredicates, 4);
      STRAppendCString (&objTypePredicates, ")\n");
      STRAppend (&query, objTypePredicates);
    }
  }

  /* construct the type predicates to exclude by processing each type */
  /* in pszNotObjTypeList */
  if (pszNotObjTypeList != NULL)
  {
    size_t iIndex, iNumTypes;

    /* report the exclusion list */
    STRCopyCString (&mssg, "Excluded object types: ");
    STRAppendCString (&mssg, pszNotObjTypeList);
    DST_LOG (STRC (mssg));


    STRCopyCString (&objTypeList, pszNotObjTypeList);
    iNumTypes = STRGetNumTokens (objTypeList, STR_WHITESPACE_CHARS);

    if (iNumTypes > 0)
    {
      STRCopyCString (&notObjTypePredicates, "AND (");

      for (iIndex = 1; iIndex <= iNumTypes; iIndex ++)
      {
        /* get this type */
        STRGetTokenAt (objTypeList, iIndex, STR_WHITESPACE_CHARS, &type);

        /* add this type to the predicate list */
        STRAppendCString (&notObjTypePredicates, "INSTR (TYPE, '");
        STRAppend (&notObjTypePredicates, type);
        STRAppendCString (&notObjTypePredicates, "') = 0 AND ");
      }

      /* append the predicates to the query */
      STRChop (&notObjTypePredicates, 5);
      STRAppendCString (&notObjTypePredicates, ") \n");
      STRAppend (&query, notObjTypePredicates);
    }
  }

  if (bIncludeSynonyms)
  {
    /* add the union that will include synonyms associated with the */
    /* selected objects */
    STRAppendCString (&query,
      "UNION \n"
      "SELECT 'SYNONYM', OWNER, SYNONYM_NAME, NULL FROM SYS.ALL_SYNONYMS \n"
      "WHERE (TABLE_OWNER, TABLE_NAME) IN (\n"

      "SELECT RTRIM (OWNER), RTRIM (NAME) FROM (\n"
      DST_OBJECT_TYPE_QUERY
      ") OBJECTS \n"

      "WHERE OBJECTS.NAME NOT LIKE 'MV%$%' \n"
      "AND OBJECTS.NAME NOT LIKE 'MGT%$%' \n"
      "AND OBJECTS.NAME NOT LIKE 'CD$%' \n" /* exclude compressed column group tables */
      "AND RTRIM (OBJECTS.OWNER) NOT IN ('SYS', 'SYSTEM', 'TTREP', 'GRID', "
        "'" DST_TEST_DATA_SCHEMA "', '" DST_TEST_RESULTS_SCHEMA "') \n");

    STRAppend (&query, objTypePredicates);
    STRAppend (&query, notObjTypePredicates);
    STRAppendCString (&query, ")");
  }

  DST_LOG_QUIET;

  /* prepare the statement */
  DSTAllocStmt (hdbc, &hstmt);
  DSTPrepareStmt (hstmt, STRC (query));

  /* execute the query */
  DSTExecPreparedStmt (hstmt);


  DST_LOG ("Found objects:");

  /* count the number of objects to choose from */
  while (DSTIsSuccess (DSTFetch (hstmt)))
  {
    iNumObjs ++;

    /* display the set of found objects only if the caller is verbose */
    if (bIsCallerLogVerbose)
    {
      DSTGetRowAsString (hstmt, &mssg, FALSE);
      DST_LOG (STRC (mssg));
    }
  }

  DSTCloseStmtCursor (hstmt);


  /* choose the target object */
  if (iNumObjs > 0)
  {
    iTargetObj = (DSTRand () % iNumObjs) + 1;
    iNumObjs = 0;

    /* execute the query again */
    DSTExecPreparedStmt (hstmt);

    /* select the target object */
    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      iNumObjs ++;

      if (iNumObjs == iTargetObj)
      {
        DSTGetColAsString (hstmt, 1, &type);

        /* output parameters */
        if (pObjOwner != NULL)
          DSTGetColAsString (hstmt, 2, pObjOwner);

        if (pObjName != NULL)
          DSTGetColAsString (hstmt, 3, pObjName);

        if (pObjQualifier != NULL)
          DSTGetColAsString (hstmt, 4, pObjQualifier);

        /* success */
        bStatus = TRUE;
        break;
      }
    }
  }


  /* report the result */
  DST_LOG_VERBOSE;

  if (bStatus)
  {
    STRCopyCString (&mssg, "Selected object %s.%s of type %s.");
    STRInsertString (&mssg, *pObjOwner);
    STRInsertString (&mssg, *pObjName);
    STRInsertString (&mssg, type);
    DST_LOG (STRC (mssg));
  }


  DSTFreeStmt (hstmt);

  STRFree (&query);
  STRFree (&mssg);
  STRFree (&type);
  STRFree (&objTypePredicates);
  STRFree (&objTypeList);
  STRFree (&notObjTypePredicates);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTResolveSynonym ()
*
* Description: This is the internal version of DSTResolveSynonym(). It
* is called recursively to resolve a chain of synonyms. The pSynChain
* parameter points to a space delimited list of synonyms that are
* chained together. This list is used to determine whather a chain
* of synonyms is circular.
*
* In:  hdbc, pszSynOwner, pszSynName
*
* Out: pObjOwner, pObjName
*
* In/Out:
*
*/
BOOL DSTResolveSynonymChain (const HDBC hdbc,
                             const char* pszSynOwner, const char* pszSynName,
                             DSTRING* pObjOwner, DSTRING* pObjName,
                             DSTRING* pSynChain)
{
  BOOL bFound = FALSE;
  SQLRETURN rc;
  HSTMT hstmt;
  DSTRING synOwner, refOwner, refName;
  DSTRING chainKey;

  DST_LOG_QUIET_ME ("DSTResolveSynonymChain");


  /* init. dynamic strings */
  STRInit (&synOwner, "");
  STRInit (&refOwner, "");
  STRInit (&refName, "");
  STRInit (&chainKey, "");


  /* validate the parameters */
  assert (pszSynName != NULL && hdbc != SQL_NULL_HDBC);


  /* if the pszSynOwner param. is NULL then get the owner */
  /* name from the hdbc */
  if (pszSynOwner == NULL)
    DSTGetCurrentUser (hdbc, &synOwner);
  else
    STRCopyCString (&synOwner, pszSynOwner);


  /* allocate a statement handle */
  DSTAllocStmt (hdbc, &hstmt);

  DSTSetParamAsString (hstmt, 1, STRC (synOwner));
  DSTSetParamAsString (hstmt, 2, pszSynName);

  /* query SYS.ALL_SYNONYMS for synonyms that refer to objects that exist */
  rc = DSTExecDirectStmt (hstmt,
    "SELECT TABLE_OWNER, TABLE_NAME "
    "FROM SYS.ALL_SYNONYMS "
    "WHERE OWNER = :p1 AND SYNONYM_NAME = :p2 "
    "AND (TABLE_OWNER, TABLE_NAME) IN "
      "(SELECT OWNER, OBJECT_NAME FROM SYS.ALL_OBJECTS) ");


  /* was the reference found? */
  if (DSTIsSuccess (rc) && DSTIsSuccess (DSTFetch (hstmt)))
  {
    bFound = TRUE;
    DSTGetColAsString (hstmt, 1, &refOwner);
    DSTGetColAsString (hstmt, 2, &refName);

    DSTCloseStmtCursor (hstmt);

    /* if this is a synonym call this function again */
    if (DSTIsSynonym (hdbc, STRC (refOwner), STRC (refName)))
    {
      STRCopyCString (&chainKey, "\"%s\".\"%s\" ");
      STRInsertString (&chainKey, synOwner);
      STRInsertCString (&chainKey, pszSynName);

      /* is this synonym already in the chain ? */
      if (STRHasSequence (*pSynChain, STRC (chainKey), NULL))
      {
        /* this is a circular chain - abort */
        bFound = FALSE;
      }
      else
      {
        /* add this synonym to the chain */
        STRAppend (pSynChain, chainKey);

        /* resolve it again */
        bFound = DSTResolveSynonymChain (hdbc, STRC (refOwner), STRC (refName),
          pObjOwner, pObjName, pSynChain);
      }
    }
    else
    {
      /* copy the output params. on success */
      STRCopy (pObjOwner, refOwner);
      STRCopy (pObjName, refName);
    }
  }

  /* deallocate the statement handle */
  DSTFreeStmt (hstmt);


  /* free dynamic strings */
  STRFree (&synOwner);
  STRFree (&refOwner);
  STRFree (&refName);
  STRFree (&chainKey);

  DST_LOG_UNREPORT_ME;
  return bFound;

}


/*
* Function: DSTResolveSynonym ()
*
* Description: This functions resolves the object name that a synonym
* refers to. If the pszSynOwner parameter is NULL then the user associated
* with the hdbc is used to search for the synonym. If the synonym is not
* found then FALSE is returned. If the synonym refers to another synonym
* then the resolution process will follow the chain until the terminal object
* is found. If the synonym chain is circular or if the synonym refers to
* an object that does not exist then FALSE is returned.
*
* In:  hdbc, pszSynOwner, pszSynName
*
* Out: pObjOwner, pObjName
*
* In/Out:
*
*/
BOOL DSTResolveSynonym (const HDBC hdbc,
                        const char* pszSynOwner, const char* pszSynName,
                        DSTRING* pObjOwner, DSTRING* pObjName)
{
  BOOL bFound = FALSE;
  DSTRING synChain;

  STRInit (&synChain, "");

  bFound = DSTResolveSynonymChain (hdbc, pszSynOwner, pszSynName,
    pObjOwner, pObjName, &synChain);

  STRFree (&synChain);

  return bFound;
}


/*
* Function: DSTGetCurrentUser ()
*
* Description: This functions returns the current user name in
* pUserName associated with the hdbc. If an error occurs then
* FALSE is returned.
*
* In:  hdbc
*
* Out:
*
* In/Out: pUserName
*
*/
BOOL DSTGetCurrentUser (const HDBC hdbc, DSTRING* pUserName)
{
#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCGetCurrentUser (hdbc, pUserName);
#endif
  {
    /* locals */
    SQLRETURN rc;
    BOOL bStatus = FALSE;
    char szUser [DST_TT_MAX_IDENTIFIER_SIZE] = "";

    DST_LOG_QUIET_ME ("DSTGetCurrentUser");

    /* call SQLGetInfo () */
    rc = SQLGetInfo (hdbc,
      SQL_USER_NAME, (SQLPOINTER) szUser,
      DST_TT_MAX_IDENTIFIER_SIZE, NULL);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLGetInfo () failed.");
    }
    else
    {
      bStatus = TRUE;

      /* copy to the output param. */
      STRCopyCString (pUserName, szUser);

      if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
          "SQLGetInfo () info.");
      }
    }

    DST_LOG_UNREPORT_ME;
    return bStatus;
  }
}


/*
* Function: DSTGetDataStorePath ()
*
* Description: Returns the full path to the database associated
* with the hdbc.
*
* In:  hdbc
*
* Out: pDataStorePath
*
* In/Out:
*
*/
BOOL DSTGetDataStorePath (const HDBC hdbc, DSTRING* pDataStorePath)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;
  char szFilePath [DST_TT_MAX_FILEPATH_SIZE] = "<UNKNOWN>";

  DST_LOG_QUIET_ME ("DSTGetDataStorePath");


  /* Pro*C connections cannot do this */
  if (!DSTIsProCConn (hdbc))
  {
    /* call SQLGetInfo () */
    rc = SQLGetInfo (hdbc, SQL_DATABASE_NAME,
      (SQLPOINTER) szFilePath,
      DST_TT_MAX_FILEPATH_SIZE, NULL);

    if (!DSTIsSuccess (rc))
    {
      bStatus = FALSE;
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLGetInfo () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLGetInfo () info.");
    }
  }

  /* copy to the output param. */
  STRCopyCString (pDataStorePath, szFilePath);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTGetDataStoreName ()
*
* Description: Returns the simple name of this database.
*
* In:  hdbc
*
* Out: pDataStoreName
*
* In/Out:
*
*/
BOOL DSTGetDataStoreName (const HDBC hdbc, DSTRING* pDataStoreName)
{
  /* locals */
  BOOL bStatus = TRUE;
  DSTRING dataStorePath;
  size_t iNumPathElements;

  STRInit (&dataStorePath, "");

  /* the name of the database is the last component */
  /* in the full path to the database */
  bStatus = DSTGetDataStorePath (hdbc, &dataStorePath);

  if (bStatus)
  {
    /* extract the last token in the path to get the name */
    iNumPathElements = STRGetNumTokens (dataStorePath, "\\/");
    STRGetTokenAt (dataStorePath, iNumPathElements, "\\/", pDataStoreName);
  }


  STRFree (&dataStorePath);

  return bStatus;
}

/*
* Function: DSTGetDriverName ()
*
* Description: Return the name of the ODBC driver for this connection.
*
* In:  hdbc
*
* Out: pDriverName
*
* In/Out:
*
*/
BOOL DSTGetDriverName (const HDBC hdbc, DSTRING* pDriverName)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;
  char szDriverName [DST_TT_MAX_IDENTIFIER_SIZE] = "";

  DST_LOG_QUIET_ME ("DSTGetDriverName");

  /* if this is a Pro*C connection then return <Unknown> */
  if (DSTIsProCConn (hdbc))
  {
    STRCopyCString (pDriverName, "<Unknown>");
    DST_LOG_UNREPORT_ME;
    return TRUE;
  }


  /* call SQLGetInfo () */
  rc = SQLGetInfo (hdbc, SQL_DRIVER_NAME,
    (SQLPOINTER) szDriverName,
    DST_TT_MAX_IDENTIFIER_SIZE, NULL);

  if (!DSTIsSuccess (rc))
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () info.");
  }

  /* copy to the output param. */
  STRCopyCString (pDriverName, szDriverName);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsClientDriver ()
*
* Description: Returns TRUE if the connection handle is associated with
* the TimesTen client driver.
*
* In:  hdbc
*
* Out: 
*
* In/Out:
*
*/
BOOL DSTIsClientDriver (HDBC hdbc)
{
  BOOL bIsClient = FALSE;
  DSTRING driver;

  DST_LOG_QUIET_ME ("DSTIsClientDriver");

  STRInit (&driver, "");

  DSTGetDriverName (hdbc, &driver);
  STRToLower (driver);

  if ((STRHasSequence (driver, "ttcl", NULL) ||
    STRHasSequence (driver, "libttclient", NULL)))
  {
    bIsClient = TRUE;
  }

  STRFree (&driver);

  DST_LOG_UNREPORT_ME;
  return bIsClient;
}


/*
* Function: DSTGetDriverVersion ()
*
* Description: Returns the version string of the ODBC driver 
* for this connection.
*
* In:  hdbc
*
* Out: pDriverVersion
*
* In/Out:
*
*/
BOOL DSTGetDriverVersion (const HDBC hdbc, DSTRING* pDriverVersion)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;
  char szDriverVersion [DST_TT_MAX_IDENTIFIER_SIZE] = "";

  DST_LOG_QUIET_ME ("DSTGetDriverVersion");

  /* if this is a Pro*C connection then return <Unknown> */
  if (DSTIsProCConn (hdbc))
  {
    STRCopyCString (pDriverVersion, "<Unknown>");
    DST_LOG_UNREPORT_ME;
    return TRUE;
  }


  /* call SQLGetInfo () */
  rc = SQLGetInfo (hdbc, SQL_DRIVER_VER,
    (SQLPOINTER) szDriverVersion,
    DST_TT_MAX_IDENTIFIER_SIZE, NULL);

  if (!DSTIsSuccess (rc))
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () info.");
  }

  /* copy to the output param. */
  STRCopyCString (pDriverVersion, szDriverVersion);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTGetConfigValue ()
*
* Description: Returns the value of the specified key via a call to the 
* TimesTen built-in ttConfiguration() procedure. If the key is not found or if 
* a failure occurs then FALSE is returned. The output parameter is specified as 
* a string and/or integer. Either output parameter can be null.
*
* In: hdbc, pszKey
*
* Out: pValue, piValue
*
* In/Out:
*
*/
BOOL DSTGetConfigValue (HDBC hdbc, char* pszKey,
                        DSTRING* pValue, SDWORD* piValue)
{
  /* locals */
  HSTMT hstmt;
  BOOL bStatus = FALSE;
  DSTRING value;
  DST_CONN_STATE_SAVE (hdbc);

  DST_LOG_QUIET_ME ("DSTGetConfigValue");

  STRInit (&value, DST_NULL_STRING);


  /* for Oracle (PassThrough=3) connections we need to */
  /* switch to TimesTen execution */
  DST_CONN_STATE_TIMESTEN (hdbc);

  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DSTSetParamAsString (hstmt, 1, pszKey);

    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      "CALL ttConfiguration (?)")) &&
      DSTIsSuccess (DSTFetch (hstmt)) &&
      DSTIsSuccess (DSTGetColAsString (hstmt, 2, &value)))
    {
      /* key value pair found */
      bStatus = TRUE;

      if (pValue != NULL)
        STRCopy (pValue, value);

      if (piValue != NULL)
        sscanf (STRC (value), "%i", piValue);
    }

    DSTFreeStmt (hstmt);
  }

  STRFree (&value);

  DST_CONN_STATE_RESTORE (hdbc);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetDbConfigValue ()
*
* Description: Returns the value of the specified key via a call to the 
* TimesTen built-in ttDbConfig() procedure. If the key is not found or if 
* a failure occurs then FALSE is returned. The output parameter is specified as 
* a string and/or integer. Either output parameter can be null.
*
* In: hdbc, pszKey
*
* Out: pValue, piValue
*
* In/Out:
*
*/
BOOL DSTGetDbConfigValue (HDBC hdbc, char* pszKey,
                          DSTRING* pValue, SDWORD* piValue)
{
  /* locals */
  HSTMT hstmt;
  BOOL bStatus = FALSE;
  DSTRING value;
  DST_CONN_STATE_SAVE (hdbc);

  DST_LOG_QUIET_ME ("DSTGetDbConfigValue");

  STRInit (&value, DST_NULL_STRING);


  /* for Oracle (PassThrough=3) connections we need to */
  /* switch to TimesTen execution */
  DST_CONN_STATE_TIMESTEN (hdbc);

  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DSTSetParamAsString (hstmt, 1, pszKey);

    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      "CALL ttDbConfig (?)")) &&
      DSTIsSuccess (DSTFetch (hstmt)) &&
      DSTIsSuccess (DSTGetColAsString (hstmt, 2, &value)))
    {
      /* key value pair found */
      bStatus = TRUE;

      if (pValue != NULL)
        STRCopy (pValue, value);

      if (piValue != NULL)
        sscanf (STRC (value), "%i", piValue);
    }

    DSTFreeStmt (hstmt);
  }

  STRFree (&value);

  DST_CONN_STATE_RESTORE (hdbc);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}




/*
* Function: DSTSetTTTypeMode ()
*
* Description: Sets the TimesTen SQL data type type
* mode for ODBC functions that return SQL data type
* constants. If TRUE is specified then TimesTen type
* constants are returned. If FALSE is specified then
* generic ODBC type constants are returned.
*
*
* In: hdbc, bTTTypeMode
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetTTTypeMode (HDBC hdbc, BOOL bTTTypeMode)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;

  DST_LOG_QUIET_ME ("DSTSetTTTypeMode");

  assert (hdbc != SQL_NULL_HDBC);


  /* call SQLSetConnectOption () */
  rc = SQLSetConnectOption (hdbc, TT_GET_FIXED_TYPE_CODES,
    (SQLULEN) bTTTypeMode);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLSetConnectOption () failed.");

    bStatus = FALSE;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLSetConnectOption () info.");
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}




/*
* Function: DSTEnableStats ()
*
* Description: Enables or disables the collection of database
* statistics via the ttSysStatsLevelSet() built-in procedure.
*
*
* In: hdbc, bEnableStats
*
* Out:
*
* In/Out:
*
*/
BOOL DSTEnableStats (HDBC hdbc, BOOL bEnableStats)
{

  /* locals */
  HSTMT hstmt;
  BOOL bStatus = FALSE;
  DSTRING statsLevel;


  DST_LOG_REPORT_ME ("DSTEnableStats");

  /* init. dynamic strings */
  STRInit (&statsLevel, "");


  /* validate the parameters */
  assert (hdbc != SQL_NULL_HDBC);


  DSTAllocStmt (hdbc, &hstmt);

  if (bEnableStats)
    DSTSetParamAsString (hstmt, 1, "ALL");
  else
    DSTSetParamAsString (hstmt, 1, "NONE");

  /* set the stat level */
  if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      "CALL ttSysStatsLevelSet (:level)")))
  {

    /* report the stat level */
    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
        "CALL ttSysStatsLevelGet ()")) &&
        DSTIsSuccess (DSTFetch (hstmt)))
    {
      /* get and report the level */
      DSTGetColAsString (hstmt, 1, &statsLevel);

      STRPrependCString (&statsLevel, "Database statistics level: ");
      DST_LOG (STRC (statsLevel));

      bStatus = TRUE;
    }
  }



  DSTFreeStmt (hstmt);

  STRFree (&statsLevel);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsRepInvalid ()
*
* Description: This functions returns TRUE if the current
* replication state is 'FAILED'.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsRepInvalid (const HDBC hdbc)
{
  SQLINTEGER bIsRepInvalid = FALSE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTIsRepInvalid");

  assert (hdbc != SQL_NULL_HDBC);


  /* call SQLGetInfo () with  TT_REPLICATION_INVALID */
  rc = SQLGetInfo (hdbc, TT_REPLICATION_INVALID,
    &bIsRepInvalid, 0, NULL);


  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () info.");
  }

  DST_LOG_UNREPORT_ME;
  return (BOOL) bIsRepInvalid;
}


/*
* Function: DSTIsInvalid ()
*
* Description: This functions returns TRUE if the database
* has been invalidated.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsInvalid (const HDBC hdbc)
{
  SQLINTEGER bIsInvalid = TRUE;
  SQLRETURN rc;

  DST_LOG_QUIET_ME ("DSTIsInvalid");

  assert (hdbc != SQL_NULL_HDBC);

  /* if this is a Pro*C connection we can't call */
  /* SQLGetInfo so query the current user, if */
  /* this fails then assume the DS is invalid */
  if (DSTIsProCConn (hdbc))
  {
    DSTRING userName;
    STRInit (&userName, "");
    bIsInvalid = DSTGetCurrentUser (hdbc, &userName);
    STRFree (&userName);

    DST_LOG_UNREPORT_ME;
    return !bIsInvalid;
  }



  /* call SQLGetInfo () with  TT_DATA_STORE_INVALID */
  rc = SQLGetInfo (hdbc, TT_DATA_STORE_INVALID,
    &bIsInvalid, 0, NULL);


  if (!DSTIsSuccess (rc))
  {
    /* if this call fails for any reason then consider the database 'invalid' */
    /* in the sense that it is inaccessible */
    bIsInvalid = TRUE;

    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () info.");
  }


  DST_LOG_UNREPORT_ME;
  return (BOOL) bIsInvalid;
}

/*
* Function: DSTIsCharLengthColumn ()
*
* Description: This function returns TRUE if the specified
* column's length is defined in terms of database characters
* rather than bytes.
*
* In: hstmt, pszTableOwner, pszTableName, pszColumnName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsCharLengthColumn (const HDBC hdbc,
                            const char* pszTableOwner,
                            const char* pszTableName,
                            const char* pszColumnName)
{

  /* locals */
  SQLRETURN rc;
  HSTMT hstmt;
  char szLengthSemantic [] = "BYTE";
  BOOL bIsCharLength = FALSE;
  DSTRING sql;


  DST_LOG_QUIET_ME ("DSTIsCharLengthColumn");


  /* validate params */
  assert (hdbc != SQL_NULL_HDBC &&
    pszTableOwner != NULL &&
    pszTableName != NULL &&
    pszColumnName != NULL);


  /* construct the query */
  STRInit (&sql, "SELECT \"%s\" FROM \"%s\".\"%s\"");
  STRInsertCString (&sql, pszColumnName);
  STRInsertCString (&sql, pszTableOwner);
  STRInsertCString (&sql, pszTableName);


  /* prepare the statement */
  DSTAllocStmt (hdbc, &hstmt);
  DSTPrepareStmt (hstmt, STRC (sql));


  /* get the length semantic */
  rc = SQLColAttributes (hstmt, 1,
    TT_COLUMN_LENGTH_SEMANTICS,
    szLengthSemantic, 5, NULL, NULL);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLColAttributes () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, SQL_NULL_HDBC, hstmt,
      "SQLColAttributes () info.");
  }

  if (strcmp (szLengthSemantic, "CHAR") == 0)
  {
    bIsCharLength = TRUE;
  }

  /* clean up */
  DSTFreeStmt (hstmt);
  STRFree (&sql);


  DST_LOG_UNREPORT_ME;
  return bIsCharLength;
}




/*
* Function: DSTIsPrimaryKeyColumn ()
*
* Description: This function returns TRUE if the specified
* column on the specified table is a primary key column
* - otherwise FALSE is returned.
*
* In: hstmt, pszTableOwner, pszTableName, pszColumnName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsPrimaryKeyColumn (const HDBC hdbc,
                            const char* pszTableOwner,
                            const char* pszTableName,
                            const char* pszColumnName)
{

  /* locals */
  BOOL bStatus = TRUE;
  HSTMT hstmt;
  DSTRING tableOwner, tableName, columnName;
  BOOL bIsPrimaryKey = FALSE;

  DST_LOG_QUIET_ME ("DSTIsPrimaryKeyColumn");

  /* validate params */
  assert (hdbc != SQL_NULL_HDBC &&
    pszTableOwner != NULL && 
    pszTableName != NULL &&
    pszColumnName != NULL);

  /* init. dynamic strings */
  STRInit (&columnName, "");
  STRInit (&tableOwner, pszTableOwner);
  STRInit (&tableName, pszTableName);


  /* if this is a synonym then SQLPrimaryKeys will not recognize */
  /* it - see Bug #8993610 - so resolve the synonym here */
  if (DSTIsSynonym (hdbc, pszTableOwner, pszTableName))
  {
    if (!DSTResolveSynonym (hdbc, pszTableOwner, pszTableName,
      &tableOwner, &tableName))
    {
      bStatus = FALSE;
      DST_LOG_ERROR ("Synonym does not resolve to a concrete object.");
    }
  }


  /* call SQLPrimaryKeys () ...*/
  DSTAllocStmt (hdbc, &hstmt);


  if (!DSTIsSuccess (SQLPrimaryKeys (hstmt, NULL, 0,
    (SQLCHAR*) STRC (tableOwner), SQL_NTS,
    (SQLCHAR*) STRC (tableName), SQL_NTS)))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, hstmt,
      "SQLPrimaryKeys () failed.");

    bStatus = FALSE;
  }


  /* is there a column name in the list of primary keys */
  /* that matches pszColumnName */
  while (bStatus && DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* get the column name and compare */
    DSTGetColAsString (hstmt, 4, &columnName);

    if (STREqualsCString (columnName, pszColumnName))
    {
      bIsPrimaryKey = TRUE;
      break;
    }
  }


  DSTFreeStmt (hstmt);

  /* free dynamic strings */
  STRFree (&columnName);
  STRFree (&tableOwner);
  STRFree (&tableName);

  DST_LOG_UNREPORT_ME;
  return bIsPrimaryKey;
}



/*
* Function: DSTIsForeignKeyColumn ()
*
* Description: This function returns TRUE if the specified
* column on the specified table is a foreign key column
* - otherwise FALSE is returned.
*
* In: hstmt, pszChildTableOwner, pszChildTableName, pszColumnName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsForeignKeyColumn (const HDBC hdbc,
                            const char* pszChildTableOwner,
                            const char* pszChildTableName,
                            const char* pszColumnName)
{

  /* locals */
  SQLRETURN rc;
  HSTMT hstmt;
  DSTRING columnName;
  BOOL bIsForeignKey = FALSE;

  DST_LOG_QUIET_ME ("DSTIsForeignKeyColumn");

  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC &&
    pszChildTableName != NULL &&
    pszColumnName != NULL);


  DSTAllocStmt (hdbc, &hstmt);


  /* call SQLForeignKeys () ...*/
  rc = SQLForeignKeys (hstmt,
    NULL, 0, NULL, 0,  NULL, 0,  NULL, 0,
    (SQLCHAR*) pszChildTableOwner, SQL_NTS,
    (SQLCHAR*) pszChildTableName, SQL_NTS);


  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, hstmt,
      "SQLForeignKeys () failed.");

    DSTFreeStmt (hstmt);

    DST_LOG_UNREPORT_ME;
    return rc;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, hstmt,
      "SQLForeignKeys () info.");
  }


  /* init. dynamic strings */
  STRInit (&columnName, "");


  while (DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* get the column name */
    DSTGetColAsString (hstmt, 8, &columnName);

    if (STREqualsCString (columnName, pszColumnName))
    {
      bIsForeignKey = TRUE;
      break;
    }
  }


  DSTFreeStmt (hstmt);

  /* free dynamic strings */
  STRFree (&columnName);

  DST_LOG_UNREPORT_ME;
  return bIsForeignKey;
}


/*
* Function: DSTGetReferencedColumn ()
*
* Description: Returns the root table name, owner, and column
* for given child table foreign key column.
*
* In: hstmt, pszChildTableOwner, pszChildTableName, pszColumnName
*
* Out: pRootTableOwner, pRootTableName, pRootColumnName
*
* In/Out:
*
*/
BOOL DSTGetReferencedColumn (const HDBC hdbc,
                             const char* pszChildTableOwner,
                             const char* pszChildTableName,
                             const char* pszColumnName,
                             DSTRING* pRootTableOwner,
                             DSTRING* pRootTableName,
                             DSTRING* pRootColumnName)
{
  /* locals */
  BOOL bStatus = FALSE;
  SQLRETURN rc;
  HSTMT hstmt;
  DSTRING columnName;

  DST_LOG_QUIET_ME ("DSTGetReferencedColumn");

  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC &&
    pszChildTableOwner != NULL &&
    pszChildTableName != NULL &&
    pszColumnName != NULL);


  DSTAllocStmt (hdbc, &hstmt);


  /* call SQLForeignKeys () ...*/
  rc = SQLForeignKeys (hstmt,
    NULL, 0, NULL, 0,  NULL, 0,  NULL, 0,
    (SQLCHAR*) pszChildTableOwner, SQL_NTS,
    (SQLCHAR*) pszChildTableName, SQL_NTS);


  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, hstmt,
      "SQLForeignKeys () failed.");

    DSTFreeStmt (hstmt);

    DST_LOG_UNREPORT_ME;
    return rc;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, hstmt,
      "SQLForeignKeys () info.");
  }


  /* init. dynamic strings */
  STRInit (&columnName, "");

  while (DSTIsSuccess (DSTFetch (hstmt)))
  {

    /* get the child table column name */
    DSTGetColAsString (hstmt, 8, &columnName);

    /* if this is the target column then get the */
    /* root table attributes */
    if (STREqualsCString (columnName, pszColumnName))
    {
      DSTGetColAsString (hstmt, 2, pRootTableOwner);
      DSTGetColAsString (hstmt, 3, pRootTableName);
      DSTGetColAsString (hstmt, 4, pRootColumnName);

      bStatus = TRUE;
      break;
    }
  }


  DSTFreeStmt (hstmt);

  /* free dynamic strings */
  STRFree (&columnName);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTTableHasNonPrimaryKeyColumn ()
*
* Description: This function returns TRUE if at least one
* column in the specified table is not a primary key
* column.
*
* In: hdbc, pszTableOwner, pszTableName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTTableHasNonPrimaryKeyColumn (const HDBC hdbc,
                                     const char* pszTableOwner,
                                     const char* pszTableName)
{
  BOOL bStatus = FALSE;
  SQLRETURN rc;
  HSTMT hstmt;
  DSTRING columnName;

  DST_LOG_QUIET_ME ("DSTTableHasNonPrimaryKeyColumn");

  /* init. dynamics strings */
  STRInit (&columnName, "");


  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC &&
    pszTableOwner != NULL &&
    pszTableName != NULL);



  /* build the select statement based on the names of the */
  /* columns in the table */
  DSTAllocStmt (hdbc, &hstmt);

  rc = SQLColumns (hstmt, NULL, 0,
    (SQLCHAR*) pszTableOwner, SQL_NTS,
    (SQLCHAR*) pszTableName,
    SQL_NTS, NULL, 0);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, hstmt,
      "SQLColumns () failed.");
    bStatus = FALSE;
  }
  else
  {
    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      /* get the column name */
      DSTGetColAsString (hstmt, 4, &columnName);

      /* is this not a primary key column? */
      if (!DSTIsPrimaryKeyColumn (hdbc, pszTableOwner,
        pszTableName, STRC (columnName)))
      {
        bStatus = TRUE;
        break;
      }
    }
  }

  DSTFreeStmt (hstmt);

  STRFree (&columnName);


  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsTable ()
*
* Description: Returns TRUE if the table exists in the specified data source. 
* The pszOwner parameter can be NULL. If pszOwner is NULL then the owner name 
* associated with the hdbc is used to search for the table.
*
* In: hdbc, pszOwner, pszName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsTable (const HDBC hdbc,
                 const char* pszOwner,
                 const char* pszName)
{

  /* locals */
  HSTMT hstmt;
  BOOL bTableExists = FALSE;
  DSTRING ownerName;


  DST_LOG_QUIET_ME ("DSTIsTable");

  /* init. dynamic strings */
  STRInit (&ownerName, "");


  /* validate the parameters */
  assert (pszName != NULL && hdbc != SQL_NULL_HDBC);


  /* if the pszOwner param. is NULL then get the owner */
  /* name from the hdbc */
  if (pszOwner == NULL)
    DSTGetCurrentUser (hdbc, &ownerName);
  else
    STRCopyCString (&ownerName, pszOwner);




  /* allocate a statement handle */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {

    DSTPrepareStmt (hstmt,
      "SELECT 1 FROM ALL_OBJECTS "
      "WHERE (OWNER = TRIM (UPPER (:p1)) OR OWNER IN ('SYS')) AND "
      "OBJECT_NAME = TRIM (UPPER (:p2)) AND "
        "(OBJECT_TYPE = 'TABLE' OR "
        "OBJECT_TYPE = 'VIEW' OR "
        "OBJECT_TYPE = 'MATERIALIZED VIEW' OR "
        "OBJECT_TYPE = 'SYNONYM')");

    DSTSetParamAsString (hstmt, 1, STRC (ownerName));
    DSTSetParamAsString (hstmt, 2, pszName);

    /* was the table found? */
    if (DSTIsSuccess (DSTExecPreparedStmt (hstmt)) &&
      DSTIsSuccess (DSTFetch (hstmt)))
    {
      /* if at least one row is returned in the result set */
      /* then the answer is yes */
      bTableExists = TRUE;
    }

    /* deallocate the statement handle */
    DSTFreeStmt (hstmt);
  }


  /* free dynamic strings */
  STRFree (&ownerName);

  DST_LOG_UNREPORT_ME;
  return bTableExists;
}



/*
* Function: DSTIsChildTable ()
*
* Description: This function returns TRUE if the any column
* in the specified table is part of a foreign key.
*
* In: hdbc, pszTableOwner, pszTableName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsChildTable (const HDBC hdbc,
                      const char* pszTableOwner,
                      const char* pszTableName)
{
  BOOL bStatus = TRUE;
  BOOL bIsChildTable = FALSE;
  DSTRING tableOwner, tableName;
  SQLRETURN rc;
  HSTMT hstmt;


  DST_LOG_QUIET_ME ("DSTIsChildTable");

  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC &&
    pszTableName != NULL &&
    pszTableOwner != NULL);

  STRInit (&tableOwner, pszTableOwner);
  STRInit (&tableName, pszTableName);


  /* if this is a synonym then SQLForeignKeys() will not recognize */
  /* it - see Bug #8993610 - so resolve the synonym here */
  if (DSTIsSynonym (hdbc, pszTableOwner, pszTableName))
  {
    if (!DSTResolveSynonym (hdbc, pszTableOwner, pszTableName,
      &tableOwner, &tableName))
    {
      bStatus = FALSE;
      DST_LOG_ERROR ("Synonym does not resolve to a concrete object.");
    }
  }
  else
  {
    /* reset the table owner/name */
    STRCopyCString (&tableName, pszTableName);
    STRCopyCString (&tableOwner, pszTableOwner);
  }




  DSTAllocStmt (hdbc, &hstmt);

  /* call SQLForeignKeys () ...*/
  rc = SQLForeignKeys (hstmt,
    NULL, 0, NULL, 0,  NULL, 0,  NULL, 0,
    (SQLCHAR*) STRC (tableOwner), SQL_NTS,
    (SQLCHAR*) STRC (tableName), SQL_NTS);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, hstmt,
      "SQLForeignKeys () failed.");
    bStatus = FALSE;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, hstmt,
      "SQLForeignKeys () info.");
  }


  /* if any rows are returned then this table */
  /* contains foreign keys */
  if (bStatus && DSTIsSuccess (DSTFetch (hstmt)))
    bIsChildTable = TRUE;


  DSTFreeStmt (hstmt);



  STRFree (&tableOwner);
  STRFree (&tableName);


  DST_LOG_UNREPORT_ME;
  return bIsChildTable;
}

/*
* Function: DSTIsSequence ()
*
* Description: Returns TRUE if the named sequence exists in the
* data source. The pszSeqOwner parameter can be NULL. If this
* parameter is NULL then the owner name associated with the hdbc
* is used to search for the sequence. The sequence owner or name
* parameters may include LIKE predicate wild cards.
*
* In: hdbc, pszSeqOwner, pszSeqName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsSequence (const HDBC hdbc,
                    const char* pszSeqOwner,
                    const char* pszSeqName)
{

  /* locals */
  SQLRETURN rc;
  HSTMT hstmt;
  BOOL bSeqExists = FALSE;
  DSTRING userName;

  DST_LOG_QUIET_ME ("DSTIsSequence");

  /* init. dynamic strings */
  STRInit (&userName, "");


  /* validate the parameters */
  assert (pszSeqName != NULL && hdbc != SQL_NULL_HDBC);


  /* if the pszSeqOwner param. is NULL then get the owner */
  /* name from the hdbc */
  if (pszSeqOwner == NULL)
    DSTGetCurrentUser (hdbc, &userName);
  else
    STRCopyCString (&userName, pszSeqOwner);


  /* allocate a statement handle */
  DSTAllocStmt (hdbc, &hstmt);

  DSTSetParamAsString (hstmt, 1, STRC (userName));
  DSTSetParamAsString (hstmt, 2, pszSeqName);

  /* query SYS.ALL_OBJECTS */
  rc = DSTExecDirectStmt (hstmt,
    "SELECT 1 FROM SYS.ALL_OBJECTS "
    "WHERE OWNER LIKE :p1 AND OBJECT_NAME LIKE :p2 "
    "AND OBJECT_TYPE = 'SEQUENCE'");

  /* was the sequence found? */
  if (DSTIsSuccess (rc) &&
    DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* if at least one row is returned in the result set */
    /* then the answer is yes */
    bSeqExists = TRUE;
  }

  /* deallocate the statement handle */
  DSTFreeStmt (hstmt);

  /* free synamic strings */
  STRFree (&userName);

  DST_LOG_UNREPORT_ME;
  return bSeqExists;
}

/*
* Function: DSTIsSynonym ()
*
* Description: Returns TRUE if the named synonym exists in the
* data source. The pszSynOwner parameter can be NULL. If this
* parameter is NULL then the owner name associated with the hdbc
* is used to search for the synonym. The synonym owner or name
* parameters may include LIKE predicate wild cards.
*
* In: hdbc, pszSynOwner, pszSynName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsSynonym (const HDBC hdbc,
                   const char* pszSynOwner,
                   const char* pszSynName)
{
  /* locals */
  SQLRETURN rc;
  HSTMT hstmt;
  BOOL bSynExists = FALSE;
  DSTRING userName;

  DST_LOG_QUIET_ME ("DSTIsSynonym");

  /* init. dynamic strings */
  STRInit (&userName, "");


  /* validate the parameters */
  assert (pszSynName != NULL && hdbc != SQL_NULL_HDBC);


  /* if the pszSynOwner param. is NULL then get the owner */
  /* name from the hdbc */
  if (pszSynOwner == NULL)
    DSTGetCurrentUser (hdbc, &userName);
  else
    STRCopyCString (&userName, pszSynOwner);


  /* allocate a statement handle */
  DSTAllocStmt (hdbc, &hstmt);

  DSTSetParamAsString (hstmt, 1, STRC (userName));
  DSTSetParamAsString (hstmt, 2, pszSynName);

  /* query SYS.ALL_SYNONYMS */
  rc = DSTExecDirectStmt (hstmt,
    "SELECT 1 FROM SYS.ALL_SYNONYMS "
    "WHERE OWNER LIKE :p1 AND SYNONYM_NAME LIKE :p2 ");

  /* was the synonym found? */
  if (DSTIsSuccess (rc) && DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* if at least one row is returned in the result set */
    /* then the answer is yes */
    bSynExists = TRUE;
  }

  /* deallocate the statement handle */
  DSTFreeStmt (hstmt);

  /* free dynamic strings */
  STRFree (&userName);

  DST_LOG_UNREPORT_ME;
  return bSynExists;
}


/*
* Function: DSTIsUser ()
*
* Description: Returns TRUE if the specified UID is known
* to TimesTen.
*
* In: hdbc, pszUid
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsUser (const HDBC hdbc, const char* pszUid)
{
  BOOL bExists = FALSE;
  HSTMT hstmt;

  DST_LOG_QUIET_ME ("DSTIsUser");

  DSTAllocStmt (hdbc, &hstmt);
  DSTSetParamAsString (hstmt, 1, pszUid);

  DSTExecDirectStmt (hstmt,
    "SELECT USERNAME FROM DBA_USERS "
    "WHERE USERNAME = UPPER (:p1)");

  if (DSTIsSuccess (DSTFetch (hstmt)))
    bExists = TRUE;

  DSTFreeStmt (hstmt);

  DST_LOG_UNREPORT_ME;
  return bExists;
}


/*
* Function: DSTIsSystemUser ()
*
* Description: Returns TRUE if the specified UID is a system
* defined TimesTen user. These users include SYS, SYSTEM,
* TTREP, PUBLIC and the instance administration user.
*
* In: hdbc, pszUid
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsSystemUser (const HDBC hdbc, const char* pszUid)
{
  BOOL bIsSystemUser = FALSE;
  HSTMT hstmt;

  DST_LOG_QUIET_ME ("DSTIsSystemUser");

  DSTAllocStmt (hdbc, &hstmt);
  DSTSetParamAsString (hstmt, 1, pszUid);

  /* a user that was created at the time that the database was */
  /* created is a system user */
  DSTExecDirectStmt (hstmt,
    "SELECT 1 FROM SYS.USER$ WHERE NAME = 'SYS' "
    "AND CTIME = (SELECT CTIME FROM SYS.USER$ WHERE NAME = :p1)");

  if (DSTIsSuccess (DSTFetch (hstmt)))
    bIsSystemUser = TRUE;

  DSTFreeStmt (hstmt);

  DST_LOG_UNREPORT_ME;
  return bIsSystemUser;
}

/*
* Function: DSTIsAdminConn ()
*
* Description: Returns TRUE if the specified connection has
* the ADMIN system privilege.
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsAdminConn (const HDBC hdbc)
{
  BOOL bIsAdmin = FALSE;
  HSTMT hstmt;

  DST_LOG_QUIET_ME ("DSTIsAdminConn");

  DSTAllocStmt (hdbc, &hstmt);

  DSTExecDirectStmt (hstmt,
    "SELECT 1 FROM SYS.DBA_SYS_PRIVS WHERE GRANTEE = USER "
    "AND PRIVILEGE = 'ADMIN'");

  if (DSTIsSuccess (DSTFetch (hstmt)))
    bIsAdmin = TRUE;

  DSTFreeStmt (hstmt);

  DST_LOG_UNREPORT_ME;
  return bIsAdmin;
}


/*
* Function: DSTIsMaterializedView ()
*
* Description: Returns TRUE if the specified table is a
* materialized view. The pszTableOwner parameter can be
* NULL. If this parameter is NULL then the owner name is
* searched for within the hdbc. If the table is not a
* materialized view, or if the table is not found then
* FALSE is returned.
*
* In: hdbc, pszTableOwner, pszTableName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsMaterializedView (const HDBC hdbc,
                             const char* pszTableOwner,
                             const char* pszTableName)
{
  /* locals */
  BOOL bStatus = FALSE;
  DSTRING tableOwner;
  HSTMT hstmt = SQL_NULL_HSTMT;

  DST_LOG_QUIET_ME ("DSTIsMaterializedView");

  /* validate params */
  assert (hdbc != SQL_NULL_HDBC && pszTableName != NULL);


  STRInit (&tableOwner, "");


  /* get the table's owner */
  if (pszTableOwner == NULL)
    DSTGetCurrentUser (hdbc, &tableOwner);
  else
    STRCopyCString (&tableOwner, pszTableOwner);


  /* query SYS.VIEWS */
  DSTAllocStmt (hdbc, &hstmt);

  DSTPrepareStmt (hstmt,
    "SELECT 1 FROM SYS.ALL_OBJECTS "
    "WHERE OBJECT_NAME = :p1 AND OWNER = :p2 "
    "AND OBJECT_TYPE = 'MATERIALIZED VIEW'");

  DSTSetParamAsString (hstmt, 1, pszTableName);
  DSTSetParamAsString (hstmt, 2, STRC (tableOwner));

  if (DSTIsSuccess (DSTExecPreparedStmt (hstmt)) &&
    DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* materialzed view found */
    bStatus = TRUE;
  }

  DSTFreeStmt (hstmt);


  STRFree (&tableOwner);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsLogicalView ()
*
* Description: Returns TRUE if the specified table is
* a regular (not materialized) view. The pszTableOwner parameter
* can be NULL. If this parameter is NULL then the owner name
* is searched for within the hdbc. If the table is not a regular
* view, or if the table is not found then FALSE is returned.
*
* In: hdbc, pszTableOwner, pszTableName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsLogicalView (const HDBC hdbc,
                       const char* pszTableOwner,
                       const char* pszTableName)
{
  /* locals */
  BOOL bStatus = FALSE;
  DSTRING tableOwner;
  HSTMT hstmt = SQL_NULL_HSTMT;

  DST_LOG_QUIET_ME ("DSTIsLogicalView");

  /* validate params */
  assert (hdbc != SQL_NULL_HDBC && pszTableName != NULL);


  STRInit (&tableOwner, "");

  /* get the table's owner */
  if (pszTableOwner == NULL)
    DSTGetCurrentUser (hdbc, &tableOwner);
  else
    STRCopyCString (&tableOwner, pszTableOwner);


  /* query SYS.ALL_OBJECTS */
  DSTAllocStmt (hdbc, &hstmt);

  DSTPrepareStmt (hstmt,
    "SELECT 1 FROM SYS.ALL_OBJECTS "
    "WHERE OBJECT_NAME = :p1 AND OWNER = :p2 "
    "AND OBJECT_TYPE = 'VIEW'");

  DSTSetParamAsString (hstmt, 1, pszTableName);
  DSTSetParamAsString (hstmt, 2, STRC (tableOwner));

  if (DSTIsSuccess (DSTExecPreparedStmt (hstmt)) &&
    DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* regular view found */
    bStatus = TRUE;
  }

  DSTFreeStmt (hstmt);


  STRFree (&tableOwner);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetTableNameFromSysId ()
*
* Description:Returns a table's name and owner based on the system ID
* for the table. If the table is not found then FALSE is returned.
*
* In: hdbc, iSysId
*
* Out: pTableOwner, pTableName
*
* In/Out:
*
*/
BOOL DSTGetTableNameFromSysId (const HDBC hdbc, const SQLUBIGINT iSysId,
                               DSTRING* pTableOwner, DSTRING* pTableName)
{
  /* locals */
  BOOL bStatus = FALSE;
  HSTMT hstmt = SQL_NULL_HSTMT;

  DST_LOG_QUIET_ME ("DSTGetTableNameFromSysId");


  /* validate params */
  assert (hdbc != SQL_NULL_HDBC);
  assert (pTableOwner != NULL && pTableName != NULL);

  /* get the table name and owner */
  DSTAllocStmt (hdbc, &hstmt);

  DSTPrepareStmt (hstmt, "SELECT "
    "RTRIM (TBLNAME), RTRIM (TBLOWNER) "
    "FROM SYS.TABLES WHERE TBLID = :p1");

  DSTSetParamAsBigInt (hstmt, 1, (SQLBIGINT*) &iSysId);

  DSTExecPreparedStmt (hstmt);

  if (DSTIsSuccess (DSTFetch (hstmt)))
  {
    /* table found */
    bStatus = TRUE;

    DSTGetColAsString (hstmt, 1, pTableName);
    DSTGetColAsString (hstmt, 2, pTableOwner);
  }
  else
  {
    /* table not found */
    bStatus = FALSE;
  }

  DSTFreeStmt (hstmt);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/* ---------------------------------------------------------------- */
/* Oracle Metadata Functions. */




/*
* Function: DSTIsOraConn ()
*
* Description: This function returns TRUE if the connection associated with the 
* hdbc is successfully operating against the Oracle RDBMS. Note that a TimesTen 
* connection may be configured (Passthrough=3) to operate against the Oracle
* RDBMS but may not have access due to service loss, incorrect credentials, etc.
*
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsOraConn (const HDBC hdbc)
{
  HSTMT hstmt;
  SDWORD iCount = 1;

  DST_LOG_QUIET_ME ("DSTIsOraConn");

  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {

    /* Test for the TTREP.CLIENT_FAILOVER table. If this table is not found */
    /* then assume that the conection is operating against the Oracle RDBMS. */
    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      "SELECT COUNT (*) FROM SYS.DBA_OBJECTS "
      "WHERE OWNER = 'TTREP' AND "
        "OBJECT_NAME = 'CLIENTFAILOVER' AND "
        "OBJECT_TYPE = 'TABLE'")))
    {
      DSTFetch (hstmt);
      DSTGetColAsInt (hstmt, 1, &iCount);
    }

    DSTFreeStmt (hstmt);
  }


  DST_LOG_UNREPORT_ME;

  if (iCount == 1)
    return FALSE;

  return TRUE;
}



/*
* Function: DSTOraColumns ()
*
* Description: Generates a result set of all Oracle table
* columns that match the owner name, table name and column
* name patterns. Autocommit must be turned off to call this
* function. The generated result set has the following
* columns:
*
*    TABLE_OWNER                     VARCHAR2 (30) NOT NULL
*    TABLE_NAME                      VARCHAR2 (30) NOT NULL
*    COLUMN_NAME                     VARCHAR2 (30) NOT NULL
*    DATA_TYPE                       NUMBER
*    TYPE_NAME                       VARCHAR2 (106)
*    PRECISION                       NUMBER
*    LENGTH                          NUMBER NOT NULL
*    SCALE                           NUMBER
*    NULLABLE                        NUMBER
*    DEFAULT_VALUE                   VARCHAR2 (4194304)
*
* In: hdbc, hstmt, pszOwnerNamePattern, pszTableNamePattern,
*     pszColumnPattern
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTOraColumns (HDBC hdbc, HSTMT hstmt,
                         const char* pszOwnerPattern,
                         const char* pszTablePattern,
                         const char* pszColumnPattern)
{
  /* locals */
  SQLRETURN rc;
  DSTRING columnQuery;
  DSTRING ownerPattern, tablePattern, columnPattern;

  DST_CONN_STATE_SAVE (hdbc);
  DST_LOG_QUIET_ME ("DSTOraColumns");


  /* validate params */
  assert (hstmt != SQL_NULL_HSTMT &&
    hdbc != SQL_NULL_HDBC &&
    DSTGetAutoCommit (hdbc) == SQL_AUTOCOMMIT_OFF);


  /* set the connection for Oracle */
  DST_CONN_STATE_ORACLE (hdbc);


  STRInit (&columnQuery,
    "SELECT "
      "OWNER TABLE_OWNER, "
      "TABLE_NAME, "
      "COLUMN_NAME, "
      "CASE "
        "WHEN DATA_TYPE = 'CHAR' THEN 1 "
        "WHEN DATA_TYPE = 'VARCHAR2' OR DATA_TYPE = 'LONG' OR DATA_TYPE = 'CLOB' THEN 12 "
        "WHEN DATA_TYPE = 'RAW' OR DATA_TYPE = 'ROWID' THEN -2 "
        "WHEN DATA_TYPE = 'LONG RAW' OR DATA_TYPE = 'BLOB' OR DATA_TYPE = 'BFILE' THEN -3 "
        "WHEN DATA_TYPE = 'NCHAR' THEN -8 "
        "WHEN DATA_TYPE = 'NVARCHAR2' OR DATA_TYPE = 'NCLOB' THEN -9 "
        "WHEN DATA_TYPE = 'DATE' OR DATA_TYPE LIKE 'TIMESTAMP%' THEN 11 "
        "WHEN DATA_TYPE = 'FLOAT' THEN 8 "
        "WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN 6 "
        "WHEN DATA_TYPE = 'BINARY_FLOAT' THEN 7 "
        "WHEN DATA_TYPE = 'NUMBER' AND DATA_PRECISION = 0 AND DATA_SCALE = 0 THEN 8 "
        "WHEN DATA_TYPE = 'NUMBER' THEN 3 "
        "WHEN DATA_TYPE = 'INTERVAL' THEN 3 "
        "ELSE 0 END AS DATA_TYPE, "
      "DATA_TYPE TYPE_NAME, "
      "CASE "
        "WHEN DATA_PRECISION IS NULL AND DATA_LENGTH = 0 THEN 4194304 "
        "WHEN DATA_PRECISION IS NULL AND DATA_TYPE LIKE '%CHAR%' THEN CHAR_LENGTH "
        "WHEN DATA_PRECISION IS NULL THEN DATA_LENGTH "
        "ELSE DATA_PRECISION END AS PRECISION, "
      "CASE "
        "WHEN DATA_LENGTH = 0 THEN 4194304 "
        "ELSE DATA_LENGTH END AS LENGTH, "
      "DATA_SCALE SCALE, "
      "CASE NULLABLE "
        "WHEN 'YES' THEN 1 "
        "ELSE 0 END AS NULLABLE, "
      "DATA_DEFAULT DEFAULT_VALUE "
    "FROM SYS.ALL_TAB_COLUMNS "
    "WHERE "
    "OWNER LIKE :p1 "
    "AND TABLE_NAME LIKE :p2 "
    "AND COLUMN_NAME LIKE :p3 "
      "ORDER BY 1, 2, 3");


  /* if the owner, table or column names have string lengths */
  /* greater than 0 then use these values as search patterns */
  /* - otherwise set the search pattern to "%" */
  STRInit (&ownerPattern, "%");
  STRInit (&tablePattern, "%");
  STRInit (&columnPattern, "%");

  if (pszOwnerPattern != NULL &&
    strlen (pszOwnerPattern) != 0)
  {
    STRCopyCString (&ownerPattern, pszOwnerPattern);
  }

  if (pszTablePattern != NULL &&
    strlen (pszTablePattern) != 0)
  {
    STRCopyCString (&tablePattern, pszTablePattern);
  }

  if (pszColumnPattern != NULL &&
    strlen (pszColumnPattern) != 0)
  {
    STRCopyCString (&columnPattern, pszColumnPattern);
  }


  /* prepare the query */
  DSTPrepareStmt (hstmt, STRC (columnQuery));

  /* set the parameters */
  DSTSetParamAsString (hstmt, 1, STRC (ownerPattern));
  DSTSetParamAsString (hstmt, 2, STRC (tablePattern));
  DSTSetParamAsString (hstmt, 3, STRC (columnPattern));

  /* execute the query */
  rc = DSTExecPreparedStmt (hstmt);


  /* restore the connection */
  DST_CONN_STATE_RESTORE (hdbc);


  STRFree (&columnQuery);
  STRFree (&ownerPattern);
  STRFree (&tablePattern);
  STRFree (&columnPattern);

  DST_LOG_UNREPORT_ME;
  return rc;
}




/*
* Function: DSTGetRandomOraTable ()
*
* Description: This function returns a random Oracle table name
* and the associated owner in pOwnerName and pTableName. If
* pOwnerName and/or pTableName are not 0 length strings when
* the function is called then the values of pOwnerName and/or
* pTableName are used as search patterns for the set of tables
* that will be used to select randomly from. If no tables are
* available to select randomly from or if an error occurs during
* processing then FALSE is returned.
*
* In:  hdbc
*
* Out:
*
* In/Out: pOwnerName, pTableName
*
*/
BOOL DSTGetRandomOraTable (const HDBC hdbc,
                           DSTRING* pOwnerName,
                           DSTRING* pTableName)
{
  /* locals */
  BOOL bStatus = TRUE;
  HSTMT hstmt;
  SDWORD iNumRows = 0;
  SDWORD iCurrentRow = 0;
  SDWORD iTargetRow = 0;
  DSTRING tableQuery, tableCountQuery;

  DST_CONN_STATE_SAVE (hdbc);
  DST_LOG_QUIET_ME ("DSTGetRandomOraTable");


  /* set the connection for Oracle */
  DST_CONN_STATE_ORACLE (hdbc);



  STRInit (&tableQuery,
    "SELECT OWNER, TABLE_NAME " \
    "FROM ALL_TABLES " \
    "WHERE OWNER LIKE :p1 AND TABLE_NAME LIKE :p2 " \
    "ORDER BY 1,2");

  STRInit (&tableCountQuery,
    "SELECT COUNT (*) " \
    "FROM ALL_TABLES " \
    "WHERE OWNER LIKE :p1 AND TABLE_NAME LIKE :p2 ");


  /* if the owner or table name have string lengths greater */
  /* than 0 then use these values as search patterns */
  /* - otherwise set the search pattern to "%" */
  if (STRC_LENGTH (*pOwnerName) == 0)
    STRCopyCString (pOwnerName, "%");

  if (STRC_LENGTH (*pTableName) == 0)
    STRCopyCString (pTableName, "%");


  DSTAllocStmt (hdbc, &hstmt);

  /* count the number of tables from the list of all possible tables */
  DSTPrepareStmt (hstmt, STRC (tableCountQuery));

  DSTSetParamAsString (hstmt, 1, STRC (*pOwnerName));
  DSTSetParamAsString (hstmt, 2, STRC (*pTableName));

  if (!DSTIsSuccess (DSTExecPreparedStmt (hstmt)))
  {
    bStatus = FALSE;
  }

  /* get the count of the number of possible tables to choose from */
  if (bStatus && \
    DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsInt (hstmt, 1, &iNumRows);
  }

  DSTCloseStmtCursor (hstmt);



  /* select one of the table rows randomly */
  if (iNumRows != 0)
  {
    iTargetRow = (DSTRand () % iNumRows) + 1;
  }
  else
  {
    /* if the number of rows == 0 then return FALSE */
    bStatus = FALSE;
  }


  /* retrieve the owner and table name of the randomly selected table */
  if (bStatus)
  {

    DSTPrepareStmt (hstmt, STRC (tableQuery));

    DSTSetParamAsString (hstmt, 1, STRC (*pOwnerName));
    DSTSetParamAsString (hstmt, 2, STRC (*pTableName));

    if (!DSTIsSuccess (DSTExecPreparedStmt (hstmt)))
    {
      bStatus = FALSE;
    }


    while (bStatus &&
      DSTIsSuccess (DSTFetch (hstmt)))
    {
      iCurrentRow ++;

      /* if this is the randomly selected table then get the owner */
      /* and table name */
      if (iCurrentRow == iTargetRow)
      {
        DSTGetColAsString (hstmt, 1, pOwnerName);
        DSTGetColAsString (hstmt, 2, pTableName);
        break;
      }
    }

  }


  DSTFreeStmt (hstmt);


  /* restore the connection */
  DST_CONN_STATE_RESTORE (hdbc);


  STRFree (&tableQuery);
  STRFree (&tableCountQuery);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsOraPrimaryKeyColumn ()
*
* Description: This function returns TRUE if the
* specified column on the specified Oracle table
* is a column associated with the table's primary key.
* Otherwise FALSE is returned.
*
* In: hstmt, pszTableOwner, pszTableName, pszColumnName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsOraPrimaryKeyColumn (const HDBC hdbc,
                               const char* pszTableOwner,
                               const char* pszTableName,
                               const char* pszColumnName)
{
  /* locals */
  BOOL bStatus = FALSE;
  DSTRING constraintQuery, columnQuery;
  DSTRING constraintName, columnName;
  HSTMT hstmt;

  DST_CONN_STATE_SAVE (hdbc);
  DST_LOG_QUIET_ME ("DSTIsOraPrimaryKeyColumn");


  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC &&
    pszTableOwner != NULL &&
    pszTableName != NULL &&
    pszColumnName != NULL);


  STRInit (&constraintName, "");
  STRInit (&columnName, "");


  /* build the queries */
  STRInit (&constraintQuery,
    "SELECT CONSTRAINT_NAME "
    "FROM ALL_CONSTRAINTS "
    "WHERE CONSTRAINT_TYPE = 'P' AND "
    "OWNER = :p1 AND "
    "TABLE_NAME = :p2 ");

  STRInit (&columnQuery,
    "SELECT COLUMN_NAME "
    "FROM ALL_CONS_COLUMNS "
    "WHERE CONSTRAINT_NAME = :p1 ");


  DST_CONN_STATE_ORACLE (hdbc);


  DSTAllocStmt (hdbc, &hstmt);

  /* find the name of the constraint */
  DSTPrepareStmt (hstmt, STRC (constraintQuery));
  DSTSetParamAsString (hstmt, 1, pszTableOwner);
  DSTSetParamAsString (hstmt, 2, pszTableName);
  DSTExecPreparedStmt (hstmt);


  if (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 1, &constraintName);
  }

  DSTCloseStmtCursor (hstmt);


  /* find the names of the columns associated with the constraint */
  DSTPrepareStmt (hstmt, STRC (columnQuery));
  DSTSetParamAsString (hstmt, 1, STRC (constraintName));
  DSTExecPreparedStmt (hstmt);

  while (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 1, &columnName);
    STRTrim (&columnName, STR_WHITESPACE_CHARS);

    /* is this the target column? */
    if (STREqualsCString (columnName, pszColumnName))
    {
      bStatus = TRUE;
      break;
    }
  }

  DSTFreeStmt (hstmt);


  STRFree (&constraintQuery);
  STRFree (&columnQuery);
  STRFree (&constraintName);
  STRFree (&columnName);


  /* restore the connection */
  DST_CONN_STATE_RESTORE (hdbc);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsOraForeignKeyColumn ()
*
* Description: This function returns TRUE if the
* specified column on the specified Oracle table
* is part of a foreign key, otherwise FALSE is returned.
*
* In: hstmt, pszChildTableOwner, pszChildTableName,
*     pszColumnName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsOraForeignKeyColumn (const HDBC hdbc,
                               const char* pszChildTableOwner,
                               const char* pszChildTableName,
                               const char* pszColumnName)
{
  /* locals */
  BOOL bStatus = FALSE;
  DSTRING constraintQuery, columnQuery;
  DSTRING constraintName, columnName;
  HSTMT hstmt;

  DST_CONN_STATE_SAVE (hdbc);
  DST_LOG_QUIET_ME ("DSTIsOraForeignKeyColumn");

  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC &&
    pszChildTableOwner != NULL &&
    pszChildTableName != NULL &&
    pszColumnName != NULL);


  STRInit (&constraintName, "");
  STRInit (&columnName, "");


  /* build the queries */
  STRInit (&constraintQuery,
    "SELECT CONSTRAINT_NAME "
    "FROM ALL_CONSTRAINTS "
    "WHERE CONSTRAINT_TYPE = 'R' AND "
    "OWNER = :p1 AND "
    "TABLE_NAME = :p2 ");


  STRInit (&columnQuery,
    "SELECT COLUMN_NAME "
    "FROM ALL_CONS_COLUMNS "
    "WHERE CONSTRAINT_NAME = :p1 ");


  DST_CONN_STATE_ORACLE (hdbc);


  DSTAllocStmt (hdbc, &hstmt);

  /* find the name of the constraint */
  DSTPrepareStmt (hstmt, STRC (constraintQuery));
  DSTSetParamAsString (hstmt, 1, pszChildTableOwner);
  DSTSetParamAsString (hstmt, 2, pszChildTableName);
  DSTExecPreparedStmt (hstmt);


  if (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 1, &constraintName);
  }

  DSTCloseStmtCursor (hstmt);


  /* find the names of the columns associated with the constraint */
  DSTPrepareStmt (hstmt, STRC (columnQuery));
  DSTSetParamAsString (hstmt, 1, STRC (constraintName));
  DSTExecPreparedStmt (hstmt);

  while (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 1, &columnName);
    STRTrim (&columnName, STR_WHITESPACE_CHARS);

    /* is this the target column? */
    if (STREqualsCString (columnName, pszColumnName))
    {
      bStatus = TRUE;
      break;
    }
  }

  DSTFreeStmt (hstmt);


  STRFree (&constraintQuery);
  STRFree (&columnQuery);
  STRFree (&constraintName);
  STRFree (&columnName);


  /* restore the connection */
  DST_CONN_STATE_RESTORE (hdbc);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/* ---------------------------------------------------------------- */
/* Transaction Functions */



/*
* Function: DSTSetAutoCommit ()
*
* Description: Turns on or off autocommit mode for the connection.
* The iMode parameter can take a value of SQL_AUTOCOMMIT_ON or
* SQL_AUTOCOMMIT_OFF.
*
* In: hdbc, iMode
*
* Out:
*
* In/Out:
*
* Global Properties:
*
*/
SQLRETURN DSTSetAutoCommit (HDBC hdbc, SQLULEN iMode)
{

#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCSetAutoCommit (hdbc, iMode);
#endif

  {
    /* locals */
    SQLRETURN rc;

    /* report the call */
    DST_LOG_REPORT_ME ("DSTSetAutoCommit");

    /* validate params. */
    assert (iMode == SQL_AUTOCOMMIT_OFF || iMode == SQL_AUTOCOMMIT_ON);
    assert (hdbc != SQL_NULL_HDBC);
    DSTLogODBCHandle ("HDBC", hdbc);



    if (iMode == SQL_AUTOCOMMIT_ON)
    {

#ifdef DTP_H

      /* if using XA, commit any pending global txn before turning autocommit on */
      if (DSTUseXATxns () && DSTIsTransactionPending (hdbc))
      {

        XID xid;
        DTPCreateXIDFromString (DST_XID_STRING, &xid);

        /* end, prepare and commit the current XA txn. */
        DTPEnd (&xid, hdbc, TMSUCCESS);
        if (DTPPrepare (&xid, hdbc, TMNOFLAGS) != XA_RDONLY)
        {
          DTPCommit (&xid, hdbc, TMNOFLAGS);
        }

      }

#endif


      rc = SQLSetConnectOption (hdbc, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON);

      /* post the autocommit to the test results repository */
      DSTPostTestCmd (TEST_CMD_TYPE_CONN, TTISQL_AUTOCOMMIT_ON, hdbc, rc);

      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
          "SQLSetConnectOption () failed.");
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
          "SQLSetConnectOption () info.");
      }

      if (DSTIsSuccess (rc))
        DST_LOG ("AUTOCOMMIT mode is ON.");


    }
    else
    {
      rc = SQLSetConnectOption (hdbc, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF);

      /* post the autocommit off to the test results repository */
      DSTPostTestCmd (TEST_CMD_TYPE_CONN, TTISQL_AUTOCOMMIT_OFF, hdbc, rc);


      if (!DSTIsSuccess (rc))
      {
        DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
          "SQLSetConnectOption () failed.");
      }
      else if (rc == SQL_SUCCESS_WITH_INFO)
      {
        DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
          "SQLSetConnectOption () info.");
      }

      if (DSTIsSuccess (rc))
        DST_LOG ("AUTOCOMMIT mode is OFF.");


#ifdef DTP_H

      /* when using XA, start a global transaction after autocommit is turned off */
      if (DSTIsSuccess (rc) && DSTUseXATxns ())
      {
        XID globalXid;
        DTPCreateXIDFromString (DST_XID_STRING, &globalXid);
        DTPStart (&globalXid, hdbc, TMNOFLAGS);
      }

#endif

    }


    DST_LOG_UNREPORT_ME;
    return rc;
  }
}


/*
* Function: DSTGetAutoCommit ()
*
* Description: Returns the current autocommit mode as
* SQL_AUTOCOMMIT_OFF or SQL_AUTOCOMMIT_ON.
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*
*/
SQLINTEGER DSTGetAutoCommit (HDBC hdbc)
{

#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCGetAutoCommit (hdbc);
#endif

  {
    /* locals */
    SQLRETURN rc;
    SQLINTEGER iAutoCommitMode;

    DST_LOG_QUIET_ME ("DSTGetAutoCommit");

    rc = SQLGetConnectOption (hdbc,
      SQL_AUTOCOMMIT, &iAutoCommitMode);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLGetConnectOption () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLGetConnectOption () info.");
    }

    DST_LOG_UNREPORT_ME;
    return iAutoCommitMode;
  }
}


/*
* Function: DSTGetPassThrough ()
*
* Description: Returns the current Oracle Connect
* passthrough level. The passthrough level can be 0,
* 1, 2, or 3.
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*
*/
SWORD DSTGetPassThrough (HDBC hdbc)
{
#ifdef DST_PRC

  if (DSTIsProCConn (hdbc))
  {
    /* The TimesTen Pro*C implementation cannot process built-in */
    /* procedures that return result sets. This is a workaround. */
    if (DSTIsOraConn (hdbc))
      return 3;
    else
      return 0;
  }

#endif
  {
    /* locals */
    SDWORD iPassThroughLevel = 0;
    HSTMT hstmt;

    DST_LOG_QUIET_ME ("DSTGetPassThrough");

    if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
    {
      DSTExecDirectStmt (hstmt,
        "CALL ttOptGetFlag ('PassThrough')");

      if (DSTIsSuccess (DSTFetch (hstmt)))
        DSTGetColAsInt (hstmt, 2, &iPassThroughLevel);

      DSTFreeStmt (hstmt);
    }

    /* return logging status */
    DST_LOG_UNREPORT_ME;
    return (SWORD) iPassThroughLevel;
  }
}


/*
* Function: DSTSetPassThrough ()
*
* Description: Sets the current Cache Connect passthrough
* level. The function returns the previous  passthrough level.
* The passthrough level can be 0, 1, 2, 3, 4 or 5. AUTOCOMMIT
* must be off when calling this function.
*
* In: hdbc, iPassThroughLevel
*
* Out:
*
* In/Out:
*
*
*/
SWORD DSTSetPassThrough (HDBC hdbc, SWORD iPassThroughLevel)
{
  /* locals */
  SWORD iPrevPassThroughLevel;
  DSTRING sql;

  DST_LOG_REPORT_ME ("DSTSetPassThrough");
  DSTLogODBCHandle ("HDBC", hdbc);

  /* validate args and state */
  assert (hdbc != SQL_NULL_HDBC);
  assert (DSTGetAutoCommit (hdbc) == SQL_AUTOCOMMIT_OFF);
  assert (iPassThroughLevel >= 0 && iPassThroughLevel <= 5);

  DST_LOG_QUIET;

  /* construct the SQL */
  STRInit (&sql, "CALL ttOptSetFlag ('PassThrough', %d)");
  STRInsertInt (&sql, iPassThroughLevel);

  /* get the current passthrough level */
  iPrevPassThroughLevel = DSTGetPassThrough (hdbc);

  /* set the new level */
  if (DSTIsSuccess (DSTExecute (hdbc, STRC (sql))))
  {
    DSTRING mssg;

    DST_LOG_RESTORE;

    STRInit (&mssg, "Set passthrough level from %d to %d.");
    STRInsertInt (&mssg, iPrevPassThroughLevel);
    STRInsertInt (&mssg, iPassThroughLevel);
    DST_LOG (STRC (mssg));
    STRFree (&mssg);
  }

  STRFree (&sql);

  DST_LOG_UNREPORT_ME;
  return iPrevPassThroughLevel;
}



/*
* Function: DSTTransact ()
*
* Description: This function commits or rolls back the current transaction
* associated with the hdbc. The iMode parameter can be SQL_COMMIT or
* SQL_ROLLBACK.
*
* In: hdbc, iMode
*
* Out:
*
* In/Out:
*
* Global Properties:
*
*/
SQLRETURN DSTTransact (HDBC hdbc, SQLUSMALLINT iMode)
{
#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCTransact (hdbc, iMode);
#endif

  {
    /* locals */
    SQLRETURN rc;
    BOOL bRollbackOnFail = DSTRollbackOnCommitFailure ();
    BOOL bUseSqlExecute = DSTUseSqlExecuteTxns ();
    ttTime ttStartTime;
    ttTime ttEndTime;
    float fElapsedTime;
    char szFormattedTime [32] = "";
    DSTRING message;


    /* validate params */
    assert (iMode == SQL_COMMIT || iMode == SQL_ROLLBACK);
    assert (hdbc != SQL_NULL_HDBC);


  #ifdef DTP_H

    /* execute an XA commit */
    if (DSTUseXATxns () && iMode == SQL_COMMIT)
    {
      XID xid;
      DTPCreateXIDFromString (DST_XID_STRING, &xid);

      /* end, prepare and commit the current XA txn. */
      DTPEnd (&xid, hdbc, TMSUCCESS);

      if (DTPPrepare (&xid, hdbc, TMNOFLAGS) != XA_RDONLY)
        DTPCommit (&xid, hdbc, TMNOFLAGS);

      /* start a new global txn. */
      rc = (SQLRETURN) DTPStart (&xid, hdbc, TMNOFLAGS);

      /* return the ODBC version of the status */
      if (DTPIsXASuccess (rc))
        return SQL_SUCCESS;

      return SQL_ERROR;
    }
    /* execute an XA rollback */
    else if (DSTUseXATxns ())
    {
      XID xid;
      DTPCreateXIDFromString (DST_XID_STRING, &xid);

      /* end and rollback the current XA txn. */
      DTPEnd (&xid, hdbc, TMFAIL);
      DTPRollback (&xid, hdbc, TMNOFLAGS);

      /* start a new global txn. */
      rc = (SQLRETURN) DTPStart (&xid, hdbc, TMNOFLAGS);

      /* return the ODBC version of the status */
      if (DTPIsXASuccess (rc))
        return SQL_SUCCESS;

      return SQL_ERROR;
    }

  #endif

    /* should SQLExecute() be used ? */
    if (bUseSqlExecute)
    {
      if (iMode == SQL_COMMIT)
      {
        rc = DSTExecute (hdbc, "COMMIT WORK");

        /* is rollback automatic? */
        if (!DSTIsSuccess (rc) && bRollbackOnFail)
          DSTTransact (hdbc, SQL_ROLLBACK);

      }
      else
        rc = DSTExecute (hdbc, "ROLLBACK WORK");

      return rc;
    }



    /* normal commit processing */
    {
      DST_LOG_REPORT_ME ("DSTTransact");
      DSTLogODBCHandle ("HDBC", hdbc);

      STRInit (&message, "");

      if (iMode == SQL_COMMIT)
      {
        DST_LOG ("Committing ...");

        ttGetTime (&ttStartTime);
        rc = SQLTransact (SQL_NULL_HENV, hdbc, SQL_COMMIT);
        ttGetTime (&ttEndTime);


        /* record the commit */
        DSTPostTestCmd (TEST_CMD_TYPE_CONN, TTISQL_COMMIT, hdbc, rc);


        if (!DSTIsSuccess (rc))
        {
          DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
            "SQLTransact () failed.");

          /* is rollback automatic? */
          if (bRollbackOnFail)
            DSTTransact (hdbc, SQL_ROLLBACK);
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
            "SQLTransact () info.");
        }

        if (DSTIsSuccess (rc))
          DST_LOG ("Transaction committed.");


      }
      else
      {
        DST_LOG ("Rolling back ...");

        ttGetTime (&ttStartTime);
        rc = SQLTransact (SQL_NULL_HENV, hdbc, SQL_ROLLBACK);
        ttGetTime (&ttEndTime);


        if (!DSTIsSuccess (rc))
        {
          DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
            "SQLTransact () failed.");
        }
        else if (rc == SQL_SUCCESS_WITH_INFO)
        {
          DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
            "SQLTransact () info.");
        }


        if (DSTIsSuccess (rc))
          DST_LOG ("Transaction rolled back.");


        /* post the rollback to the test results repository */
        DSTPostTestCmd (TEST_CMD_TYPE_CONN, TTISQL_ROLLBACK, hdbc, rc);
      }


      /* report the elapsed commit/rollback time */
      ttDiffTimeMilli (&ttStartTime, &ttEndTime, &fElapsedTime);

      /* convert from milliseconds to seconds */
      fElapsedTime = fElapsedTime / 1000;

      sprintf (szFormattedTime, "%.3f", fElapsedTime);
      STRCopyCString (&message, "Elapsed time: %s seconds");
      STRInsertCString (&message, szFormattedTime);
      DST_LOG (STRC (message));


      STRFree (&message);

      DST_LOG_UNREPORT_ME;
    }

    return rc;
  }
}



/*
* Function: DSTIsTransactionPending ()
*
* Description: This function will return TRUE if a transaction
* is currently associated with the hdbc, otherwise FALSE is returned.
*
* In: henv, hdbc, iMode
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsTransactionPending (HDBC hdbc)
{
  /* locals */
  SQLRETURN rc;
  BOOL bStatus = TRUE;
  BOOL bTxnPending = FALSE;
  int iGetInfoBuffer;

  DST_LOG_QUIET_ME ("DSTIsTransactionPending");

  /* call SQLGetInfo () to determine whether a transaction is pending */
  rc = SQLGetInfo (hdbc, SQL_HDBC_IN_TX,
    (SQLPOINTER)&iGetInfoBuffer, (SWORD)sizeof(iGetInfoBuffer),
    NULL);


  /* was the call successful? */
  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () failed.");
    bStatus = FALSE;
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetInfo () info.");
  }


  if (bStatus)
  {
    /* if 0 is returned then the hdbc is not in a transaction */
    if (iGetInfoBuffer != 0)
      bTxnPending = TRUE;
    else
      bTxnPending = FALSE;
  }

  DST_LOG_UNREPORT_ME;
  return bTxnPending;
}


/*
* Function: DSTSetIsolation ()
*
* Description: Sets the isolation level for the given connection.
*
* In: hdbc, iIsolation
*
* Out:
*
* In/Out:
*
* Global Properties:
*
*/
SQLRETURN DSTSetIsolation (HDBC hdbc, SQLULEN iIsolation)
{
  /* locals */
  SQLRETURN rc;


#ifdef DST_PRC
  if (DSTIsProCConn (hdbc))
    return PRCSetIsolation (hdbc, iIsolation);
#endif

  {
    DST_LOG_REPORT_ME ("DSTSetIsolation");
    DSTLogODBCHandle ("HDBC", hdbc);

    assert (iIsolation == SQL_TXN_READ_COMMITTED ||
      iIsolation == SQL_TXN_SERIALIZABLE);

    if (iIsolation == SQL_TXN_READ_COMMITTED)
      DST_LOG ("Isolation Level = SQL_TXN_READ_COMMITED");
    else if (iIsolation == SQL_TXN_SERIALIZABLE)
      DST_LOG ("Isolation Level = SQL_TXN_SERIALIZABLE");



    /* set the isolation level */
    rc = SQLSetConnectOption (hdbc, SQL_TXN_ISOLATION, iIsolation);

    if (!DSTIsSuccess (rc))
    {
      DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLSetConnectOption () failed.");
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
      DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
        "SQLSetConnectOption () info.");
    }



    /* post the result to the test results repository */
    if (iIsolation == SQL_TXN_READ_COMMITTED)
    {
      DSTPostTestCmd (TEST_CMD_TYPE_CONN,
        TTISQL_ISOLATION " READ_COMMITTED", hdbc, rc);
    }
    else if (iIsolation == SQL_TXN_SERIALIZABLE)
    {
      DSTPostTestCmd (TEST_CMD_TYPE_CONN,
        TTISQL_ISOLATION " SERIALIZABLE", hdbc, rc);
    }


    DST_LOG_UNREPORT_ME;
  }

  return rc;
}




/*
* Function: DSTGetIsolation ()
*
* Description: Returns the isolation level for the given connection.
*
* In: hdbc
*
* Out:
*
* In/Out:
*
* Global Properties:
*
*/
SQLINTEGER DSTGetIsolation (HDBC hdbc)
{
  SQLRETURN rc;
  SQLINTEGER iIsolation = SQL_TXN_READ_COMMITTED;

  DST_LOG_QUIET_ME ("DSTGetIsolation");

  /* there is no current way to get or set the isolation */
  /* level via TimesTen Pro C - return SQL_TXN_READ_COMMITTED */
  if (DSTIsProCConn (hdbc))
  {
    DST_LOG_UNREPORT_ME;
    return SQL_TXN_READ_COMMITTED;
  }


  /* get the isolation level */
  rc = SQLGetConnectOption (hdbc, SQL_TXN_ISOLATION, &iIsolation);

  if (!DSTIsSuccess (rc))
  {
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetConnectOption () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLGetConnectOption () info.");
  }


  DST_LOG_UNREPORT_ME;
  return iIsolation;
}



/* ---------------------------------------------------------------- */
/* TEST_DATA Data Source Management Functions */



/*
* Function: DSTInitTestData ()
*
* Connects to the TEST_DATA datastore and performs any necessary
* initializations. If the connection is successful then
* TRUE is returned.
*
* In:
*
* Out:
*
* In/Out:
*
* Global properties:
* DST_TEST_DATA_SCHEMA_FILE
* DST_TEST_DATA_TTBULKCP_FILES
* DST_TTBULKCP_CMD
* DST_TEST_DATA_CONN_STR
*
*/
BOOL DSTInitTestData ()
{
  /* locals */
  BOOL bStatus = TRUE;
  DSTRING connStr;
  DSTRING tdSchemaFile, tdData, tdTable, tdDataFile;
  DSTRING ttBulkCp, cmd;
  DSTRING schema;
  size_t iIndex, iNumTokens = 0;


  /* report the function call */
  DST_LOG_REPORT_ME ("DSTInitTestData");

  assert (ghenvTD == SQL_NULL_HENV &&
    ghdbcTD == SQL_NULL_HDBC);


  /* init. dynamic strings */
  STRInit (&connStr, "");
  STRInit (&tdSchemaFile, "");
  STRInit (&tdData, "");
  STRInit (&tdTable, "");
  STRInit (&tdDataFile, "");
  STRInit (&ttBulkCp, "");
  STRInit (&cmd, "");
  STRInit (&schema, "");


  /* get the necessary properties  */
  PROPGetAsString ("DST_TEST_DATA_SCHEMA_FILE", &tdSchemaFile);
  PROPGetAsString ("DST_TEST_DATA_TTBULKCP_FILES", &tdData);
  PROPGetAsString ("DST_TTBULKCP_CMD", &ttBulkCp);
  PROPGetAsString ("DST_TEST_DATA_CONN_STR", &connStr);


  /* check the format of the DST_TEST_DATA_TTBULKCP_FILES property */
  iNumTokens = STRGetNumTokens (tdData, STR_WHITESPACE_CHARS);

  /* there must be an even number of tokens */
  assert (iNumTokens > 0 && iNumTokens % 2 == 0);


  /* set the ConnectionName attribute to DST_TEST_DATA_SCHEMA */
  /* this will prevent test data connections from being tracked */
  DSTSetConnStrAttr (&connStr, "ConnectionName", DST_TEST_DATA_SCHEMA);


  /* connect to the TEST_DATA database */
  bStatus = DSTInitConn (&ghenvTD, &ghdbcTD, STRC (connStr));

  DST_LOG_QUIET;

  if (bStatus)
  {
    DSTGetCurrentUser (ghdbcTD, &schema);

    /* if this is not the correct schema user then create */
    /* and switch */
    if (!STREqualsCString (schema, DST_TEST_DATA_SCHEMA))
    {
      DSTRING pwd;
      STRInit (&pwd, "");

      PROPGetAsString ("GLOBAL_PWD", &pwd);

      /* this will create (or alter) the user with ALL */
      /* privileges */
      bStatus = DSTCreateUser (ghdbcTD, DST_TEST_DATA_SCHEMA,
        STRC (pwd), "ALL");

      if (bStatus)
      {
        HENV henv;
        HDBC hdbc;

        /* switch the connection */
        DSTConfigConnStrAsUser (&connStr,
            DST_TEST_DATA_SCHEMA, STRC (pwd));

        if (DSTInitConn (&henv, &hdbc, STRC (connStr)))
        {
          DSTFreeConn (ghenvTD, ghdbcTD);
          ghenvTD = henv;
          ghdbcTD = hdbc;
        }
        else
          bStatus = FALSE;
      }

      /* if anything goes wrong then abort */
      if (!bStatus)
        DSTFreeTestData ();

      STRFree (&pwd);
    }
  }


  /* check for the existence of all tables - if any table */
  /* is not found then drop everything, recreate the tables */
  /* and load the data */
  for (iIndex = 1; bStatus && iIndex < iNumTokens; iIndex = iIndex + 2)
  {

    STRGetTokenAt (tdData, iIndex, STR_WHITESPACE_CHARS, &tdTable);

    if (!DSTIsTable (ghdbcTD, DST_TEST_DATA_SCHEMA, STRC (tdTable)))
    {
      /* drop all existing objects in the schema */
      DSTDropAllObjects (ghdbcTD, DST_TEST_DATA_SCHEMA);

      /* create the schema */
      bStatus = DSTExecuteFromFiles (ghdbcTD, STRC (tdSchemaFile));

      /* report the failure to find the file */
      if (!bStatus)
      {
        DSTRING mssg;
        STRInit (&mssg, "Data file \"%s\" not found.");
        STRInsertString (&mssg, tdSchemaFile);
        DST_LOG_ERROR (STRC (mssg));
        STRFree (&mssg);
      }

      /* load each data file */
      for (iIndex = 1; bStatus && iIndex < iNumTokens; iIndex = iIndex + 2)
      {
        /* build the ttBulkCp command strings */
        STRCopyCString (&cmd, "%s -i -connStr \"%s\" \"%s\" \"%s\"");
        STRInsertString (&cmd, ttBulkCp);
        STRInsertString (&cmd, connStr);

        STRGetTokenAt (tdData, iIndex, STR_WHITESPACE_CHARS, &tdTable);
        STRInsertString (&cmd, tdTable);

        STRGetTokenAt (tdData, iIndex + 1, STR_WHITESPACE_CHARS, &tdDataFile);
        STRInsertString (&cmd, tdDataFile);

        /* execute the bulk load command */
        bStatus = DSTSpawnProcess (STRC (cmd), TRUE);

      } /* end for loop */


      /* if anything goes wrong then abort */
      if (!bStatus)
        DSTFreeTestData ();
      else
        /* update statistics */
        DSTExecute (ghdbcTD, "CALL ttOptUpdateStats ()");


      break;
    }

  } /* end for loop */


  /* free dynamic strings */
  STRFree (&schema);
  STRFree (&connStr);
  STRFree (&tdSchemaFile);
  STRFree (&tdData);
  STRFree (&tdTable);
  STRFree (&tdDataFile);
  STRFree (&ttBulkCp);
  STRFree (&cmd);



  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTFreeTestData ()
*
* Disconnects from the TEST_DATA database.
*
* In:
*
* Out:
*
* In/Out:
*
* GLobal properties:
* DST_TEST_DATA_DROP_ON_DISCONNECT
*
*/
void DSTFreeTestData ()
{
  BOOL bDropOnDisconnect = FALSE;

  /* report the function call */
  DST_LOG_REPORT_ME ("DSTFreeTestData");


  if (PROPIsDefined ("DST_TEST_DATA_DROP_ON_DISCONNECT"))
  {
    PROPGetAsBool ("DST_TEST_DATA_DROP_ON_DISCONNECT",
      &bDropOnDisconnect);
  }


  /* disconnect */
  if (ghdbcTD != SQL_NULL_HDBC)
  {
    if (bDropOnDisconnect)
    {
      DST_LOG ("Dropping data sets ...");
      DST_LOG_QUIET;
      DSTDropAllObjects (ghdbcTD, DST_TEST_DATA_SCHEMA);
      DST_LOG_RESTORE;
    }

    DSTFreeConn (ghenvTD, ghdbcTD);
  }

  ghenvTD = SQL_NULL_HENV;
  ghdbcTD = SQL_NULL_HDBC;


  DST_LOG_UNREPORT_ME;
  return;
}



/*
* Function: DSTGetTestDataConn ()
*
* Returns the connection handle for the TEST_DATA database.
* If the connection does not exist then SQL_NULL_HDBC is
* returned.
*
* In:
*
* Out:
*
* In/Out:
*
*/
HDBC DSTGetTestDataConn ()
{
  /* both the environment and connection handles must not be */
  /* NULL in order for the connection to be valid */
  if (ghenvTD != SQL_NULL_HENV && ghdbcTD != SQL_NULL_HDBC)
    return ghdbcTD;

  return SQL_NULL_HDBC;
}





/* ---------------------------------------------------------------- */
/* Utility Functions */


/*
* Function: DSTParseTableName ()
*
* Description: Returns a table's name and owner based on the
* pszQualifiedTableName input. The input string can take the
* form of '[owner_name.]table_name'. If owner_name is omitted
* then the owner name is looked up in the connection associated
* with hdbc. If the owner name could not be found then FALSE
* is returned.
*
*
* In: hdbc, pszQualifiedTableName
*
* Out: pTableOwner, pTableName
*
* In/Out:
*
*/
BOOL DSTParseTableName (HDBC hdbc, const char* pszQualifiedTableName,
                        DSTRING* pTableOwner, DSTRING* pTableName)
{
  /* locals */
  BOOL bStatus = FALSE;
  size_t iNumTokens = 1;
  DSTRING qualifiedTableName;

  DST_LOG_QUIET_ME ("DSTParseTableName");

  /* validate params. */
  assert (hdbc != SQL_NULL_HDBC && pszQualifiedTableName != NULL);


  STRInit (&qualifiedTableName, pszQualifiedTableName);
  iNumTokens = STRGetNumTokens (qualifiedTableName, ".");

  if (iNumTokens > 1)
  {
    STRGetTokenAt (qualifiedTableName, 1, ".\"", pTableOwner);
    STRGetTokenAt (qualifiedTableName, 2, ".\"", pTableName);
    bStatus = TRUE;
  }
  else if (iNumTokens == 1)
  {
    STRGetTokenAt (qualifiedTableName, 1, ".\"", pTableName);

    /* an owner name is not given so try and determine what the */
    /* owner's name is from metadata */
    bStatus = DSTGetObjectOwner (hdbc, STRC (*pTableName), pTableOwner);
  }
  else
  {
    DST_LOG_ERROR ("Invalid table name syntax.");
    bStatus = FALSE;
  }


  STRFree (&qualifiedTableName);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTDoubleQuoteIdentifier ()
*
* Description: Returns the specified SQL identifier with
* double quotation marks if the identifier requires it. For
* example, if the identifier contains white space characters
* then it will be double quoted.
*
* In:
*
* Out:
*
* In/Out: pIdentifier
*
*/
void DSTDoubleQuoteIdentifier (DSTRING* pIdentifier)
{
  DSTRING keywords, testIdentifier;

  STRInit (&keywords, DST_SQL_KEYWORDS);
  STRInit (&testIdentifier, STRC (*pIdentifier));

  /* normalize the identifier for searching the keywords */
  STRToUpper (testIdentifier);
  STRPrependCString (&testIdentifier, " ");
  STRAppendCString (&testIdentifier, " ");


  /* does the identifier contain whitespace chars or other special chars */
  /* or does the identifier exist in the list of keywords? */
  if (STRGetNumTokens (*pIdentifier, STR_WHITESPACE_CHARS "%" "(" ")" ".") != 1 ||
    STRHasSequence (keywords, STRC (testIdentifier), NULL))
  {
    STRPrependCString (pIdentifier, "\"");
    STRAppendCString (pIdentifier, "\"");
  }
  /* does the identifier begin with a character that is not */
  /* a letter character */
  else if (strcspn (STRC (*pIdentifier),
    gszSqlIdentifierBeginChar) != 0)
  {
    STRPrependCString (pIdentifier, "\"");
    STRAppendCString (pIdentifier, "\"");
  }


  STRFree (&testIdentifier);
  STRFree (&keywords);
  return;
}


/*
* Function: DSTGetConnStrAttr ()
*
* Description: Returns the value of the specified connection string attribute 
* for the given ODBC connection string. If the attribute's value is found then 
* TRUE is returned.
*
* In: pszConnStr, pszAttribute
*
* Out: pValue
*
* In/Out:
*
*/
BOOL DSTGetConnStrAttr (const char* pszConnStr,
                        const char* pszAttribute,
                        DSTRING* pValue)

{
  /* locals */
  BOOL bStatus = FALSE;
  size_t iNumTokens, iIndex;
  DSTRING attributeValue, attribute;
  DSTRING targetAttribute, value;
  DSTRING connStr;


  /* validate params. */
  assert (pszConnStr != NULL && pszAttribute);


  /* init. dynamic strings */
  STRInit (&attributeValue, "");
  STRInit (&attribute, "");

  STRInit (&targetAttribute, pszAttribute);
  STRToUpper (targetAttribute);

  STRInit (&value, "");
  STRInit (&connStr, pszConnStr);


  /* clear the output param. */
  if (pValue != NULL)
    STRCopyCString (pValue, "");

  /* iterate through all attributes in the connection string */
  iNumTokens = STRGetNumTokens (connStr, STR_WHITESPACE_CHARS ";");

  for (iIndex = 1; iIndex <= iNumTokens; iIndex ++)
  {
    /* get this attribute-value pair */
    STRGetTokenAt (connStr, iIndex, STR_WHITESPACE_CHARS ";", &attributeValue);

    STRGetTokenAt (attributeValue, 1, STR_WHITESPACE_CHARS "=", &attribute);
    STRGetTokenAt (attributeValue, 2, STR_WHITESPACE_CHARS "=", &value);

    STRToUpper (attribute);

    /* is this the target attribute? */
    if (STREquals (attribute, targetAttribute))
    {
      bStatus = TRUE;

      if (pValue != NULL)
        STRCopy (pValue, value);

      break;
    }
  }

  /* free dynamic strings */
  STRFree (&attributeValue);
  STRFree (&attribute);
  STRFree (&value);
  STRFree (&targetAttribute);
  STRFree (&connStr);

  return bStatus;
}

/*
* Function: DSTGetConnStrOutAttrValue ()
*
* Description: Returns the value of the specified
* connection string attribute for the given ODBC
* connection string by connecting to the database
* and parsing the full connection string returned
* by SQLDriverConnect(). If the attribute's value is
* found then TRUE is returned.
*
* In: pszConnStrIn, pszAttribute
*
* Out: pValue
*
* In/Out:
*
*/
BOOL DSTGetConnStrOutAttrValue (const char* pszConnStrIn,
                                const char* pszAttribute,
                                DSTRING* pValue)
{
  BOOL bFound = FALSE;
  HENV henv;
  HDBC hdbc;
  char szConnStrOut [DST_CONN_STR_OUT_LENGTH] = "";


  DST_LOG_QUIET_ME ("DSTGetConnStrOutAttrValue");

  if (!DSTIsSuccess (SQLAllocEnv (&henv)))
  {
    DST_LOG_ERROR ("SQLAllocEnv () failed.");
  }
  else if (!DSTIsSuccess (SQLAllocConnect (henv, &hdbc)))
  {
    DST_LOG_SQL_ERROR (henv, SQL_NULL_HDBC, SQL_NULL_HSTMT,
      "SQLAllocConnect () failed.");
    SQLFreeEnv (henv);
  }
  else if (!DSTIsSuccess (SQLDriverConnect (hdbc, NULL,
    (SQLCHAR*) pszConnStrIn, SQL_NTS,
    (SQLCHAR*) szConnStrOut, DST_CONN_STR_OUT_LENGTH, NULL,
    SQL_DRIVER_NOPROMPT)))
  {
    DST_LOG_SQL_ERROR (henv, hdbc, SQL_NULL_HSTMT,
      "SQLDriverConnect () failed.");
    SQLFreeConnect (hdbc);
    SQLFreeEnv (henv);
  }
  else
  {
    /* success... */
    /* get the attribute value from the out conn. string */
    bFound = DSTGetConnStrAttr (szConnStrOut,
      pszAttribute, pValue);

    SQLDisconnect (hdbc);
    SQLFreeConnect (hdbc);
    SQLFreeEnv (henv);
  }

  DST_LOG_UNREPORT_ME;
  return bFound;
}


/*
* Function: DSTRemConnStrAttr  ()
*
* Description: This function removes the specified connection
* string attribute/value pair from the given connection string.
* If the attribute specified in pszAttr does not exist in the
* connection string then the connection string is returned
* unmodified.
*
* In: pszAttr
*
* Out:
*
* In/Out: pConnStr
*
*/
void DSTRemConnStrAttr (DSTRING* pConnStr, const char* pszAttr)
{
  /* locals */
  size_t iNumAttribs, iIndex;
  DSTRING newConnStr;
  DSTRING attr, targetAttr;

  /* validate param. */
  assert (pConnStr != NULL && pszAttr != NULL);

  STRInit (&newConnStr, "");
  STRInit (&attr, "");
  STRInit (&targetAttr, pszAttr);
  STRToUpper (targetAttr);


  /* find the number of semicolon delimited conn. string attributes */
  iNumAttribs = STRGetNumTokens (*pConnStr, ";");

  /* iterate through each attrib. */
  for (iIndex = 1; iIndex <= iNumAttribs; iIndex ++)
  {
    /* get the current attrib. but upper case it for comparison */
    STRGetTokenAt (*pConnStr, iIndex, ";", &attr);
    STRToUpper (attr);
    STRTrim (&attr, STR_WHITESPACE_CHARS);

    /* if this attribute is not the target attribute then add */
    /* it to the new conn. string */
    if (!STRBeginsWithCString (attr, STRC (targetAttr)))
    {
      STRGetTokenAt (*pConnStr, iIndex, ";", &attr);
      STRAppend (&newConnStr, attr);
      STRAppendCString (&newConnStr, ";");
    }
  }


  /* cleanup and copy the new conn. string to the output param */
  STRTrim (&newConnStr, STR_WHITESPACE_CHARS ";");
  STRCopy (pConnStr, newConnStr);

  STRFree (&newConnStr);
  STRFree (&attr);
  STRFree (&targetAttr);

  return;
}

/*

* Function: DSSetConnStrAttr  ()
*
* Description: This function sets the specified connection
* string attribute/value pair for the given connection string.
* If the value specified in pszValue is null then the attribute
* is set to an empty string.
*
* In: pszAttr, pszValue
*
* Out:
*
* In/Out: pConnStr
*
*/
void DSTSetConnStrAttr (DSTRING* pConnStr, const char* pszAttr,
                       const char* pszValue)
{
  assert (pConnStr != NULL && pszAttr != NULL);
  assert (!DSTIsOraConnStr (STRC (*pConnStr)));

  DSTRemConnStrAttr (pConnStr, pszAttr);
  STRAppendCString (pConnStr, ";%s=%s");
  STRInsertCString (pConnStr, pszAttr);

  if (pszValue == NULL)
    STRInsertCString (pConnStr, "");
  else
    STRInsertCString (pConnStr, pszValue);

  return;
}



/*
* Function: DSTConfigConnStrAsUser  ()
*
* Description: This function reconfigures a TimesTen
* connection string so that it will connect as the given
* UID/PWD. If pszUid and/or pszPwd are NULL then the
* UID and/or PWD attributes are set to an empty string.
*
* In: pszUid, pszPwd
*
* Out:
*
* In/Out: pConnStr
*
*/
void DSTConfigConnStrAsUser (DSTRING* pConnStr,
                             const char* pszUid,
                             const char* pszPwd)
{

  /* is this an Oracle conn. string? */
  if (DSTIsOraConnStr (STRC (*pConnStr)))
  {
    DSTRING oraConnStr;
    STRInit (&oraConnStr, STRC (*pConnStr));

    /* get just the server/sid portion of the string */
    if (STRGetNumTokens (*pConnStr, "@") == 2)
      STRGetTokenAt (*pConnStr, 2, "@", &oraConnStr);

    STRTrim (&oraConnStr, "/");
    STRTrim (&oraConnStr, STR_WHITESPACE_CHARS);
    STRPrependCString (&oraConnStr, "@");

    /* prepend the new UID/PWD attributes */
    if (pszPwd != NULL && pszUid != NULL)
    {
      STRPrependCString (&oraConnStr, "%s/%s");
      STRInsertCString (&oraConnStr, pszUid);
      STRInsertCString (&oraConnStr, pszPwd);
    }
    else
    {
      /* to connect as the OS user prepend the conn. string with '/' */
      STRPrependCString (&oraConnStr, "/");
    }

    STRCopy (pConnStr, oraConnStr);
    STRFree (&oraConnStr);
  }
  /* TimesTen ODBC conn. str. */
  else
  {

    /* configure the new attributes */
    if (pszUid != NULL)
      DSTSetConnStrAttr (pConnStr, "UID", pszUid);
    else
      DSTSetConnStrAttr (pConnStr, "UID", NULL);

    if (pszPwd != NULL)
    {
      DSTSetConnStrAttr (pConnStr, "PWD", pszPwd);
      DSTSetConnStrAttr (pConnStr, "OraclePWD", pszPwd);
    }
    else
      DSTSetConnStrAttr (pConnStr, "PWD", NULL);

    DSTSetConnStrAttr (pConnStr, "PWDCrypt", NULL);
  }

  return;
}

/*
* Function: DSTGetRequiredConnStr  ()
*
* Description: Returns a connection string containing the
* required connection attributes necessary to connect to
* the database associated with the hdbc. These attributes
* include:
*
* DSN
* DataBaseCharacterSet
* TypeMode
* Logging
* LogPurge
* PLSQL
* CacheGridEnable
*
* In: hdbc
*
* Out: connStr
*
* In/Out:
*
*/
BOOL DSTGetRequiredConnStr (HDBC hdbc, DSTRING* pConnStr)
{
  BOOL bStatus = TRUE;
  DSTRING key, value;
  HSTMT hstmt = SQL_NULL_HSTMT;

  DST_LOG_QUIET_ME ("DSTGetRequiredConnStr");

  STRInit (&key, "");
  STRInit (&value, "");

  /* clear the output buffer */
  STRCopyCString (pConnStr, "DSN=%s;");

  /* get the name of the DSN */
  DSTGetDataStoreName (hdbc, &value);
  STRInsertString (pConnStr, value);


  /* create the connection string by iterating through all config. values */
  DSTAllocStmt (hdbc, &hstmt);

  if (!DSTIsSuccess (DSTExecDirectStmt (hstmt,
    "CALL ttConfigurationInternal ()")))
  {
    bStatus = FALSE;
  }

  while (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 1, &key);
    DSTGetColAsString (hstmt, 2, &value);

    if (STREqualsCString (key, "DataBaseCharacterSet") ||
      STREqualsCString (key, "TypeMode") ||
      STREqualsCString (key, "Logging") ||
      STREqualsCString (key, "LogPurge") ||
      STREqualsCString (key, "PLSQL") ||
      STREqualsCString (key, "CacheGridEnable"))
    {
      STRAppendCString (pConnStr, "%s=%s;");
      STRInsertString (pConnStr, key);
      STRInsertString (pConnStr, value);
    }
  }

  DSTFreeStmt (hstmt);

  STRFree (&key);
  STRFree (&value);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTIsSuccess ()
*
* Description: Returns TRUE if the ODBC return code
* represents successful execution of an ODBC function call.
*
* In: rc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsSuccess (SQLRETURN rc)
{

  /* SQL_INVALID_HANDLE is not allowed */
  assert (rc != SQL_INVALID_HANDLE);


  /* if the return code does not represent successful */
  /* execution then return FALSE */
  if (rc != SQL_SUCCESS &&
    rc != SQL_SUCCESS_WITH_INFO &&
    rc != SQL_NEED_DATA)
  {
    return FALSE;
  }

  return TRUE;
}


/*
* Function: DSTSleep ()
*
* Description: Sleeps for fSeconds and then returns. The input
* parameter may be a fraction with at least millisecond precision.
*
* In: fSeconds
*
* Out:
*
* In/Out:
*
*/
void DSTSleep (float fSeconds)
{
  DSTRING message;
  DST_LOG_REPORT_ME ("DSTSleep");

  assert (fSeconds >= 0);

  STRInit (&message, "Sleeping for %e second(s)...");
  STRInsertFloat (&message, fSeconds);
  DST_LOG (STRC (message));


#ifdef SB_P_OS_NT
      /* milliseconds */
      Sleep ((DWORD) (fSeconds * 1000));
#else
      /* microseconds */
      usleep (fSeconds * 1000000);
#endif

  /* post the ttIsql sleep command */
  STRCopyCString (&message, TTISQL_SLEEP " %e");
  STRInsertFloat (&message, fSeconds);
  DSTPostTestCmd (TEST_CMD_TYPE_INTERNAL,
    STRC (message), SQL_NULL_HDBC, SQL_SUCCESS);

  STRFree (&message);
  DST_LOG_UNREPORT_ME;
  return;
}


/*
* Function: DSTGetTicks ()
*
* Description: Returns the current elapsed system time
* in seconds.
*
* In:
*
* Out:
*
* In/Out:
*
*/
time_t DSTGetTicks ()
{
  time_t iCurrentTime = time (NULL);
  assert (iCurrentTime != -1);

  return iCurrentTime;
}

/*
* Function: DSTExecuteFromFiles ()
*
* Description: Responsible for reading in and executing a script of SQL
* statements in the specified file(s). Each SQL statement in the file(s) must
* end with a semicolon charachter so that a single SQL statement can span multiple
* lines. If a line begins with '#', '/'+'*' or '--' (disregarding whitespace)
* then the line is considered a comment and is not processed. If the file(s) could
* not be successfully opened then FALSE is returned, otherwise TRUE is returned.
* Multiple files can can be specified in pszFilenames by separating them with
* whitespace characters. If a file name contains space characters use double
* quotation marks to delimit the file name.
*
* In:  hdbc, pszFilenames
*
* Out:
*
* In/Out:
*
*/
BOOL DSTExecuteFromFiles (HDBC hdbc, const char* pszFilenames)
{

  /* locals */
  BOOL bStatus = TRUE;
  size_t iFileIndex, iNumFiles;
  DSTRING fileBuffer, filenames, currentFile;
  DSTRING mssg;

  /* report the call */
  DST_LOG_REPORT_ME ("DSTExecuteFromFiles");

  /* validate params */
  assert (hdbc != SQL_NULL_HDBC && pszFilenames != NULL);

  /* init. dynamic strings */
  STRInit (&fileBuffer, "");
  STRInit (&filenames, pszFilenames);
  STRInit (&currentFile, "");
  STRInit (&mssg, "File list: ");

  /* report the files to be processed */
  STRAppendCString (&mssg, pszFilenames);
  DST_LOG (STRC (mssg));


  /* how many files need to be processed? */
  iNumFiles = STRGetNumTokens (filenames, STR_WHITESPACE_CHARS);

  for (iFileIndex = 1; iFileIndex <= iNumFiles; iFileIndex ++)
  {
    /* get the current filename */
    STRGetTokenAt (filenames, iFileIndex,
      STR_WHITESPACE_CHARS, &currentFile);

    /* clear the file buffer and load the current file */
    STRCopyCString (&fileBuffer, "");
    bStatus = STRLoadFile (STRC (currentFile), &fileBuffer);

    /* if the load failed then break out */
    if (!bStatus)
    {
      STRCopyCString (&mssg, "Failed to load file: \"%s\"");
      STRInsertString (&mssg, currentFile);
      DST_LOG_ERROR (STRC (mssg));

      break;
    }

    /* execute the statements */
    DSTExecuteFromBuffer (hdbc, &fileBuffer, NULL, NULL);

  } /* end file loop */


  /* free dynamic strings */
  STRFree (&fileBuffer);
  STRFree (&filenames);
  STRFree (&currentFile);
  STRFree (&mssg);

  /* done */
  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTExecuteFromBuffer ()
*
* Description: Responsible for reading and executing a
* script of SQL statements in the specified buffer. Each
* SQL statement in the buffer must end with a semicolon
* charachter so that a single SQL statement can span multiple
* lines. If a line begins with '#', '/'+'*' or '--'
* (disregarding whitespace) then the line is considered a
* comment and is not processed. PLSQL procedures and anonymous
* blocks must end with the '/' character.
*
* The optional output parameters indicate the number of
* successful and failed statement executions. Statements
* that executed successfully will be removed from the
* buffer.
*
* In:  hdbc, pszFilenames
*
* Out: piNumSuccesses, piNumFailures
*
* In/Out: pBuffer
*
*/
void DSTExecuteFromBuffer (HDBC hdbc, DSTRING* pBuffer,
                           SDWORD* piNumSuccesses,
                           SDWORD* piNumFailures)
{

  /* locals */
  size_t iStmtIndex = 0, iNumStmts = 0;
  DSTRING sql, inBuffer, outBuffer;
  DSTRING preparedSql;
  HSTMT hPreparedStmt;
  SDWORD iNumSuccesses = 0, iNumFailures = 0;
  DSTRING firstToken, secondToken, thirdToken, fourthToken;
  DSTRING mssg;

  /* report the call */
  DST_LOG_REPORT_ME ("DSTExecuteFromBuffer");

  /* validate params */
  assert (hdbc != SQL_NULL_HDBC && pBuffer != NULL);


  /* init. dynamic strings */
  STRInit (&preparedSql, "");
  STRInit (&sql, "");
  STRInit (&inBuffer, STRC (*pBuffer));
  STRInit (&outBuffer, "");

  STRInit (&firstToken, "");
  STRInit (&secondToken, "");
  STRInit (&thirdToken, "");
  STRInit (&fourthToken, "");

  STRInit (&mssg, "");


  /* execute each statement in the buffer ... */
  STRTrim (&inBuffer, STR_WHITESPACE_CHARS);
  iNumStmts = STRGetNumTokens (inBuffer, ";");

  STRCopyCString (&mssg, "Executing %u statement(s)...");
  STRInsertUnsignedInt (&mssg, (UDWORD) iNumStmts);
  DST_LOG (STRC (mssg));


  for (iStmtIndex = 1; iStmtIndex <= iNumStmts; iStmtIndex ++)
  {
    /* get this SQL statement */
    STRGetTokenAt (inBuffer, iStmtIndex, ";", &sql);

    /* trim whitespace */
    STRTrim (&sql, STR_WHITESPACE_CHARS);

    /* don't execute whitespace */
    if (STRC_LENGTH (sql) == 0)
      continue;


    /* get the initial tokens from the statement */
    STRGetTokenAt (sql, 1, STR_WHITESPACE_CHARS, &firstToken);
    STRToLower (firstToken);

    STRGetTokenAt (sql, 2, STR_WHITESPACE_CHARS, &secondToken);
    STRToLower (secondToken);

    STRGetTokenAt (sql, 3, STR_WHITESPACE_CHARS, &thirdToken);
    STRToLower (thirdToken);

    STRGetTokenAt (sql, 4, STR_WHITESPACE_CHARS, &fourthToken);
    STRToLower (fourthToken);


    /* is this a PLSQL 'CREATE [OR REPLACE] PROCEDURE|PACKAGE' block? */
    if (STREqualsCString (firstToken, "create") &&
      (STREqualsCString (secondToken, "procedure") ||
      STREqualsCString (fourthToken, "package") ||
      STREqualsCString (fourthToken, "procedure") ||
      STREqualsCString (secondToken, "package")))
    {
      size_t iIndex;

      /* begin the PL/SQL block */
      STRGetStringAfterToken (inBuffer, iStmtIndex - 1, ";", &sql);

      /* find the end of the block */
      if (STRHasSequence (sql, "\n/", &iIndex))
      {
        /* terminate the block */
        STRSetCharAt (&sql, '\0', iIndex + 1);

        /* execute the PLSQL block */
        if (!DSTIsSuccess (DSTExecute (hdbc, STRC (sql))))
        {
          iNumFailures ++;
          STRCopy (&outBuffer, sql);
        }
        else
          iNumSuccesses ++;
      }
      else
      {
        /* end of block not found */
        DST_LOG_ERROR ("End of PL/SQL block not found.");
        iNumFailures ++;
        STRCopy (&outBuffer, sql);
      }

      STRHasSequence (inBuffer, "\n/", &iIndex);
      STRCopyCString (&inBuffer, (STRC (inBuffer) + iIndex + 1));
      iNumStmts = STRGetNumTokens (inBuffer, ";");
      iStmtIndex = 1;

      continue;
    }
    /* is this a prepared statement? */
    else if (STREqualsCString (firstToken, TTISQL_PREPARE))
    {
      STRGetStringAfterToken (sql, 1, STR_WHITESPACE_CHARS,
        &preparedSql);

      if (hPreparedStmt == SQL_NULL_HSTMT)
        DSTAllocStmt (hdbc, &hPreparedStmt);

      /* prepare the SQL */
      if (!DSTIsSuccess (DSTPrepareStmt (hPreparedStmt,
        STRC (preparedSql))))
      {
        iNumFailures ++;
        STRCopy (&outBuffer, sql);
        STRAppendCString (&outBuffer, ";\n");
      }
      else
        iNumSuccesses ++;

      continue;
    }
    else if (STREqualsCString (firstToken, TTISQL_FREE) &&
      hPreparedStmt != SQL_NULL_HSTMT)
    {
      /* free the prepared statement handle */
      DSTFreeStmt (hPreparedStmt);
      hPreparedStmt = SQL_NULL_HSTMT;

      continue;
    }
    else if (STREqualsCString (firstToken, TTISQL_EXEC) &&
      hPreparedStmt != SQL_NULL_HSTMT)
    {
      /* execute the prepared statement */
      if (!DSTIsSuccess (DSTExecPreparedStmt (hPreparedStmt)))
      {
        iNumFailures ++;
        STRCopy (&outBuffer, sql);
        STRAppendCString (&outBuffer, ";\n");
      }
      else
        iNumSuccesses ++;

      continue;
    }
    else if (STREqualsCString (firstToken, TTISQL_AUTOCOMMIT))
    {
      STRGetTokenAt (sql, 2, STR_WHITESPACE_CHARS, &secondToken);
      STRToLower (secondToken);

      /* turn autocommit on */
      if (STREqualsCString (secondToken, "1") &&
        !DSTIsSuccess (DSTTransact (hdbc, SQL_AUTOCOMMIT_ON)))
      {
        iNumFailures ++;
        STRCopy (&outBuffer, sql);
        STRAppendCString (&outBuffer, ";\n");
      }
      /* turn autocommit off */
      else if (STREqualsCString (secondToken, "0") &&
        !DSTIsSuccess (DSTTransact (hdbc, SQL_AUTOCOMMIT_OFF)))
      {
        iNumFailures ++;
        STRCopy (&outBuffer, sql);
        STRAppendCString (&outBuffer, ";\n");
      }
      else
        iNumSuccesses ++;

      continue;
    } 



    /* some commands are ignored */
    if (STREqualsCString (firstToken, TTISQL_SET))
    {
      iNumSuccesses ++;
      continue;
    }
    else if (STRBeginsWithCString (firstToken, "#"))
    {
      iNumSuccesses ++;
      continue;
    }



    /* execute the statement */
    if (!DSTIsSuccess (DSTExecute (hdbc, STRC (sql))))
    {
      iNumFailures ++;
      STRCopy (&outBuffer, sql);
      STRAppendCString (&outBuffer, ";\n");
    }
    else
      iNumSuccesses ++;

  } /* end statement loop */


  /* set the output params */
  STRCopy (pBuffer, outBuffer);

  if (piNumSuccesses != NULL)
    *piNumSuccesses = iNumSuccesses;

  if (piNumFailures != NULL)
    *piNumFailures = iNumFailures;


  /* free dynamic strings */
  STRFree (&mssg);
  STRFree (&preparedSql);
  STRFree (&sql);
  STRFree (&inBuffer);
  STRFree (&outBuffer);

  STRFree (&firstToken);
  STRFree (&secondToken);
  STRFree (&thirdToken);
  STRFree (&fourthToken);

  /* done */
  DST_LOG_UNREPORT_ME;
  return;
}


/*
* Function: DSTIsDDL ()
*
* Description: Returns TRUE if the SQL statement is a 
* DDL statement. Otherwise FALSE is returned.
*
* In:  pszSql
*
* Out: 
*
* In/Out:
*
*/
BOOL DSTIsDDL (const char* pszSql)
{
  BOOL bIsDDL = FALSE;
  DSTRING sql, firstToken;

  STRInit (&sql, pszSql);
  STRInit (&firstToken, "");


  /* get the first token in the SQL statement */
  STRGetTokenAt (sql, 1, STR_WHITESPACE_CHARS, &firstToken);
  STRToUpper (firstToken);

  /* test for DDL keywords */
  if (STREqualsCString (firstToken, "CREATE") ||
    STREqualsCString (firstToken, "ALTER") ||
    STREqualsCString (firstToken, "DROP") ||
    STREqualsCString (firstToken, "TRUNCATE") ||
    STREqualsCString (firstToken, "GRANT") ||
    STREqualsCString (firstToken, "REVOKE"))
  {
    bIsDDL = TRUE;
  }

  STRFree (&sql);
  STRFree (&firstToken);

  return bIsDDL;
}

/*
* Function: DSTIsLoadFromOracle ()
*
* Description: Returns TRUE if the SQL statement is a 
* ttLoadFromOracle() statement. Otherwise FALSE is returned.
*
* In:  pszSql
*
* Out: 
*
* In/Out:
*
*/
BOOL DSTIsLoadFromOracle (const char* pszSql)
{
  BOOL bIsLoadFromOracle = FALSE;
  DSTRING sql, secondToken;

  STRInit (&sql, pszSql);
  STRInit (&secondToken, "");

  /* get the first token in the SQL statement */
  STRGetTokenAt (sql, 2, STR_WHITESPACE_CHARS, &secondToken);
  STRToUpper (secondToken);

  /* test for the ttLoadFromOracle keyword */
  if (STREqualsCString (secondToken, "TTLOADFROMORACLE"))
  {
    bIsLoadFromOracle = TRUE;
  }

  STRFree (&sql);
  STRFree (&secondToken);

  return bIsLoadFromOracle;
}


/*
* Function: DSTGetScalarResultAsString ()
*
* Description: Returns the first column from the first row of a query as a 
* string. If the operation succeeds then TRUE is returned.
*
* In: hdbc, pszQuery
*
* Out: pResult
*
* In/Out:
*
*/
BOOL DSTGetScalarResultAsString (HDBC hdbc,
                                 const char* pszQuery,
                                 DSTRING* pResult)
{
  HSTMT hstmt;
  BOOL bStatus = FALSE;

  DST_LOG_QUIET_ME ("DSTGetScalarResultAsString");

  assert (hdbc != SQL_NULL_HDBC && pszQuery != NULL && pResult != NULL);

  /* init. the result to empty */
  STRCopyCString (pResult, "");

  /* execute the query */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {

    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      (char*) pszQuery)))
    {
      if (DSTIsSuccess (DSTFetch (hstmt)))
      {
        DSTGetColAsString (hstmt, 1, pResult);
        bStatus = TRUE;
      }
    }

    DSTFreeStmt (hstmt);
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTGetScalarResultAsInt ()
*
* Description: Returns the first column from the first row of a query as an 
* integer. If the operation succeeds then TRUE is returned.
*
* In: hdbc, pszQuery
*
* Out: piResult
*
* In/Out:
*
*/
BOOL DSTGetScalarResultAsInt (HDBC hdbc,
                              const char* pszQuery,
                              SDWORD* piResult)
{
  HSTMT hstmt;
  BOOL bStatus = FALSE;

  DST_LOG_QUIET_ME ("DSTGetScalarResultAsInt");

  assert (hdbc != SQL_NULL_HDBC && pszQuery != NULL && piResult != NULL);


  /* execute the query */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {

    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      (char*) pszQuery)))
    {
      if (DSTIsSuccess (DSTFetch (hstmt)))
      {
        DSTGetColAsInt (hstmt, 1, piResult);
        bStatus = TRUE;
      }
    }

    DSTFreeStmt (hstmt);
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetScalarResultAsUnsignedInt ()
*
* Description: Returns the first column from the first row of a query as an 
* unsigned integer. If the operation succeeds then TRUE is returned.
*
* In: hdbc, pszQuery
*
* Out: piResult
*
* In/Out:
*
*/
BOOL DSTGetScalarResultAsUnsignedInt (HDBC hdbc,
                                      const char* pszQuery,
                                      UDWORD* piResult)
{
  HSTMT hstmt;
  BOOL bStatus = FALSE;

  DST_LOG_QUIET_ME ("DSTGetScalarResultAsUnsignedInt");

  assert (hdbc != SQL_NULL_HDBC && pszQuery != NULL && piResult != NULL);


  /* execute the query */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {

    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      (char*) pszQuery)))
    {
      if (DSTIsSuccess (DSTFetch (hstmt)))
      {
        DSTGetColAsUnsignedInt (hstmt, 1, piResult);
        bStatus = TRUE;
      }
    }

    DSTFreeStmt (hstmt);
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTGetQueryRowCount ()
*
* Description: Returns the row count for the specified query via
* piRowCount. If an error occurs when attempting to query the
* row count then FALSE is returned.
*
* In: hdbc, pszQuery
*
* Out: piRowCount
*
* In/Out:
*
*/
BOOL DSTGetQueryRowCount (HDBC hdbc,
                          const char* pszQuery,
                          UDWORD* piRowCount)
{
  /* locals */
  BOOL bStatus = FALSE;
  HSTMT hstmt;

  DST_LOG_QUIET_ME ("DSTGetQueryRowCount");

  assert (pszQuery != NULL && piRowCount != NULL);



  /* execute the query */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {

    if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
      (char*) pszQuery)))
    {
      /* set the output param to 0 rows */
      *piRowCount = 0;

      while (DSTIsSuccess (DSTFetch (hstmt)))
      {
        /* increment the row count */
        (*piRowCount) ++;
      }

      bStatus = TRUE;
    }

    DSTFreeStmt (hstmt);
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTGetTableRowCount ()
*
* Description: Returns the row count for the specified table via
* piRowCount. The pszTableName parameter can be schema qualified.
* If an error occurs when attempting to query the row count then
* FALSE is returned. The pszOwnerName parameter can be NULL.
*
* In: hdbc, pszOwnerName, pszTableName
*
* Out: piRowCount
*
* In/Out:
*
*/
BOOL DSTGetTableRowCount (HDBC hdbc,
                          const char* pszOwnerName,
                          const char* pszTableName,
                          UDWORD* piRowCount)
{
  /* locals */
  BOOL bStatus = FALSE;
  DSTRING stmt;
  HSTMT hstmt;

  DST_LOG_QUIET_ME ("DSTGetTableRowCount");

  assert (pszTableName != NULL && piRowCount != NULL);


  /* construct the statement */
  if (pszOwnerName == NULL)
  {
    STRInit (&stmt, "SELECT COUNT (*) FROM \"%s\"");
    STRInsertCString (&stmt, pszTableName);
  }
  else
  {
    STRInit (&stmt, "SELECT COUNT (*) FROM \"%s\".\"%s\"");
    STRInsertCString (&stmt, pszOwnerName);
    STRInsertCString (&stmt, pszTableName);
  }


  /* execute the query */
  DSTAllocStmt (hdbc, &hstmt);

  if (DSTIsSuccess (DSTExecDirectStmt (hstmt,
    STRC (stmt))))
  {
    if (DSTIsSuccess (DSTFetch (hstmt)))
    {
      /* success? */
      if (DSTIsSuccess (DSTGetColAsUnsignedInt (hstmt,
        1, piRowCount)))
      {
        bStatus = TRUE;
      }
    }
  }

  DSTFreeStmt (hstmt);

  STRFree (&stmt);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTCreateUniqueUid ()
*
* Description: This functions returns a unique user id string
* based on the current platform, pid and date/time. If the
* global property DST_OVERRIDE_UNIQUE_UID is set then the
* value of this property is returned as the UID instead.
*
* In:
*
* Out: pUid
*
* In/Out:
*
* Global Properties:
* PID
* DST_OVERRIDE_UNIQUE_UID
*
*/
void DSTCreateUniqueUid (DSTRING* pUid)
{
  /* locals */
  time_t iTime;
  struct tm* ptmTime;
  char szTime [48];

  /* never accept an invalid pointer */
  assert (pUid != NULL);

  /* clear the output param. */
  STRCopyCString (pUid, "");

  /* has the UID been overridden? */
  PROPGetAsString ("DST_OVERRIDE_UNIQUE_UID", pUid);
  STRTrim (pUid, STR_WHITESPACE_CHARS);

  if (STRC_LENGTH (*pUid) == 0)
  {
    /* construct a unique UID ... */
    DSTRING value;
    STRInit (&value, "");

    /* get the user prefix */
    DSTGetUidPrefix (pUid);

    /* append the PID */
    PROPGetAsString ("PID", &value);
    STRAppendCString (pUid, "_");
    STRAppend (pUid, value);

    /* append the current date-time */
    time (&iTime);
    ptmTime = localtime (&iTime);
    strftime (szTime, 48, "_%y%m%d_%H%M%S", ptmTime);

    /* append the time identifier to the platform identifier */
    STRAppendCString (pUid, szTime);

    STRFree (&value);
  }


  /* the UID cannot exceed a length of 30 characters */
  /* so truncate it if necessary */
  if (STRC_LENGTH (*pUid) > 30)
    STRSetCharAt (pUid, '\0', 30);

  /* always return an upper case UID */
  STRToUpper (*pUid);

  return;
}

/*
* Function: DSTGetUidPrefix ()
*
* Description: This functions returns a user prefix string that
* is used by the DSTCreateUniqueUid() function to generate a user 
* name. The prefix returned is the value of SB_PLATFORM, or if it 
* is not defined, then the the value of PROGRAM_FILENAME is 
* returned instead. If DST_OVERRIDE_UNIQUE_UID is defined then 
* the value of this is returned instead of SB_PLATFORM or 
* PROGRAM_FILENAME.
*
* In:
*
* Out: pUidPrefix
*
* In/Out:
*
* Global Properties:
* DST_OVERRIDE_UNIQUE_UID
* SB_PLATFORM
* PROGRAM_FILENAME
*/
void DSTGetUidPrefix (DSTRING* pUidPrefix)
{

  /* has the UID been overridden? */
  if (PROPIsDefined ("DST_OVERRIDE_UNIQUE_UID"))
  {
    PROPGetAsString ("DST_OVERRIDE_UNIQUE_UID", pUidPrefix);
  }
  else if (PROPIsDefined ("SB_PLATFORM"))
  {
    PROPGetAsString ("SB_PLATFORM", pUidPrefix);
  }
  else
  {
    size_t iNumTokens;
    PROPGetAsString ("PROGRAM_FILENAME", pUidPrefix);

    /* strip out the leading path */
    iNumTokens = STRGetNumTokens (*pUidPrefix, "\\/");
    STRGetTokenAt (*pUidPrefix, iNumTokens, "\\/", pUidPrefix);

    /* strip out a trailing extension (.exe) */
    STRGetTokenAt (*pUidPrefix, 1, ".", pUidPrefix);
  }


  return;
}

/*
* Function: DSTGetTempFileName ()
*
* Description: Returns a unique filename string that can be 
* used to work with temporary files.
*
* In:
*
* Out: pFileName
*
* In/Out:
*
* Global Properties:
*/
void DSTGetTempFileName (DSTRING* pFileName)
{
  DST_LOG_QUIET_ME ("DSTGetTempFileName");

  DSTCreateUniqueUid (pFileName);
  STRAppendCString (pFileName, ".tmp");
  
  DST_LOG_UNREPORT_ME;
  return;
}


/*
* Function: DSTSpawnProcess ()
*
* Description: This function will execute the
* process specified in pszShellCommand. The command must
* be a shell command that is compatible with the target
* platform and environment. If bWaitForReturn == TRUE then
* this function does not return until the process returns.
* If the process is not found in the current PATH then
* FALSE is returned, otherwise TRUE is returned.
*
* In:  pszShellCommand, bWaitForReturn
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSpawnProcess (const char* pszShellCommand,
                      BOOL bWaitForReturn)
{
  /* locals */
  BOOL bStatus = TRUE;
  DSTRING shellCommand, message;
  SDWORD iReturnStatus = 0;



  /* report the call */
  DST_LOG_REPORT_ME ("DSTSpawnProcess");


  /* init. dynamic strings */
  STRInit (&shellCommand, pszShellCommand);
  STRInit (&message, "");


  /* if the last character in the shell command is '&' then get rid of it */
  STRTrim (&shellCommand, STR_WHITESPACE_CHARS "&");

#ifndef SB_P_OS_NT

  if (!bWaitForReturn)
  {
    /* add the " &" to the end of the shell command in order for the process */
    /* to execute asynchronously */
    STRAppendCString (&shellCommand, " &");
  }

#endif



  STRCopyCString (&message, "Spawning process: %s");
  STRInsertString (&message, shellCommand);
  DST_LOG (STRC (message));



  /* execute the process on Windows */
#ifdef SB_P_OS_NT

  if (bWaitForReturn)
  {
    iReturnStatus = system (STRC (shellCommand));
  }
  else
  {
    static STARTUPINFO sInfo =
      {sizeof (STARTUPINFO), NULL, NULL, NULL,
      0, 0, 0, 0, 0, 0, 0,
      STARTF_USESHOWWINDOW, SW_SHOWMINIMIZED, 0,
      NULL, NULL, NULL, NULL};

    static PROCESS_INFORMATION pInfo;

    if (CreateProcess (NULL, (char*) STRC (shellCommand),
      NULL, NULL, FALSE, HIGH_PRIORITY_CLASS | CREATE_NEW_CONSOLE, NULL, NULL,
      &sInfo, &pInfo) != 0)
    {
      iReturnStatus = EXIT_SUCCESS;
    }
  }

#else

  /* execute on UNIX */
  iReturnStatus = system (STRC (shellCommand));

#endif

  /* check the return status */
  if (iReturnStatus != EXIT_SUCCESS)
  {
    DST_LOG_ERROR_HEADER ("Process failed to execute successfully.");
    bStatus = FALSE;

    STRCopyCString (&message, "Exit status: %d");
    STRInsertInt (&message, iReturnStatus);
    DST_LOG_ERROR (STRC (message));
  }

  /* record the command */
  STRPrependCString (&shellCommand, TTISQL_HOST " ");
  DSTPostTestCmd (TEST_CMD_TYPE_HOST,
    STRC (shellCommand), SQL_NULL_HDBC,
    bStatus ? SQL_SUCCESS : SQL_ERROR);

  /* free dynamic strings */
  STRFree (&shellCommand);
  STRFree (&message);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTIsRepAgentRunning ()
*
* Description: Returns TRUE if the replication agent
* is running for the database.
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsRepAgentRunning (HDBC hdbc)
{
  BOOL bIsRunning = FALSE;

  /* is the rep. agent running? */
  if (DSTGetConnectionCount (hdbc, "replication") > 0)
    bIsRunning = TRUE;

  return bIsRunning;
}


/*
* Function: DSTIsCacheAgentRunning ()
*
* Description: Returns TRUE if the cache agent
* is running for the database.
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsCacheAgentRunning (HDBC hdbc)
{
  BOOL bIsRunning = FALSE;

  /* is the cache agent running? */
  if (DSTGetConnectionCount (hdbc,
    "cache agent") > 0)
  {
    bIsRunning = TRUE;
  }

  return bIsRunning;
}




/*
* Function: DSTSetCacheUidPwd ()
*
* Description: Sets the Oracle user properties for
* cache operations. the pszOraUID and pszOraPWD parameters
* can be NULL. In this case the autofresh functions
* are not available to the application.
*
* In: hdbc, pszOraUID, pszOraPWD
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetCacheUidPwd (HDBC hdbc,
                        const char* pszOraUID,
                        const char* pszOraPWD)
{
  /* locals */
  BOOL bStatus = TRUE;


  DST_LOG_REPORT_ME ("DSTSetCacheUidPwd");

  /* validate params */
  assert (hdbc != SQL_NULL_HDBC);



#ifdef DST_DIRECT
  {
    DSTRING sqlSet;
    SDWORD iErrCode = tt_ErrNone;

    STRInit (&sqlSet, "CALL ttCacheUidPwdSet ('%s', '%s')");


    /* determine what format the call should be in */
    if (pszOraUID == NULL || pszOraPWD == NULL)
    {
      if (!DSTIsSuccess (DSTExecute (hdbc,
        "CALL ttCacheUidPwdSet (NULL, NULL)")))
      {
        DSTGetLastSqlError (NULL, &iErrCode, NULL);

        /* if the error occurred due to a running rep. agent */
        /* then ignore it */
        if (iErrCode != tt_ErrOcNoRepAgentErr)
          bStatus = FALSE;

      }
    }
    else
    {
      STRInsertCString (&sqlSet, pszOraUID);
      STRInsertCString (&sqlSet, pszOraPWD);

      if (!DSTIsSuccess (DSTExecute (hdbc,
        STRC (sqlSet))))
      {
        DSTGetLastSqlError (NULL, &iErrCode, NULL);

        /* if the error occurred due to a running rep. agent */
        /* then ignore it */
        if (iErrCode != tt_ErrOcNoRepAgentErr)
          bStatus = FALSE;

      }
    }

    STRFree (&sqlSet);
  }

#else

  /* client/server version calls ttAdmin from the command line */
  {
    /* locals */
    DSTRING message, cmd, datastore, connStr;

    /* get the connection string attributes */
    STRInit (&connStr, "");
    DSTGetRequiredConnStr (hdbc, &connStr);

    /* get the name of the database */
    STRInit (&datastore, "");
    DSTGetDataStoreName (hdbc, &datastore);

    /* create the command line */
    STRInit (&cmd,
      "ttAdmin -cacheUidPwdSet ");

    if (pszOraUID == NULL)
    {
      STRAppendCString (&cmd, "-cacheUid NULL ");
    }
    else
    {
      STRAppendCString (&cmd, "-cacheUid \"%s\" ");
      STRInsertCString (&cmd, pszOraUID);
    }

    if (pszOraPWD == NULL)
    {
      STRAppendCString (&cmd, "-cachePwd NULL ");
    }
    else
    {
      STRAppendCString (&cmd, "-cachePwd \"%s\" ");
      STRInsertCString (&cmd, pszOraPWD);
    }

    STRAppendCString (&cmd, "-connStr \"%s\"");
    STRInsertString (&cmd, connStr);


    /* report params */
    STRInit (&message, "Setting cache uid/pwd for database \"%s\" ...");
    STRInsertString (&message, datastore);
    DST_LOG (STRC (message));

    /* execute the command */
    bStatus = DSTSpawnProcess (STRC (cmd), TRUE);


    if (!bStatus)
      DST_LOG_ERROR ("ttAdmin -cacheUidPwdSet failed.");
    else
      DST_LOG ("ttAdmin -cacheUidPwdSet succeeded.");

    STRFree (&connStr);
    STRFree (&cmd);
    STRFree (&message);
    STRFree (&datastore);
  }

#endif /* DST_DIRECT */



  DST_LOG_UNREPORT_ME;
  return bStatus;
}




/*
* Function: DSTStartCacheAgent ()
*
* Description: Starts the Oracle agent for the DSN
* associated with the hdbc parameter. If the
* agent is started successfully then TRUE is returned,
* otherwise FALSE is returned. The pszOraUID and pszOraPWD
* parameters are optional and can be NULL. In this case
* the autofresh functions are not available to the
* application.
*
* In: hdbc, pszOraUID, pszOraPWD
*
* Out:
*
* In/Out:
*
*/
BOOL DSTStartCacheAgent (HDBC hdbc,
                         const char* pszOraUID,
                         const char* pszOraPWD)
{
  /* locals */
  BOOL bStatus = TRUE;
  SDWORD iLastErr = tt_ErrNone;
  SDWORD iNumControlAttempts = 0;


  /* set the login credentials */
  bStatus = DSTSetCacheUidPwd (hdbc,
    pszOraUID, pszOraPWD);


  do
  {
    /* start the agent */
    if (!DSTIsSuccess (DSTExecute (hdbc,
      "CALL ttCacheStart ()")))
    {
      bStatus = FALSE;
      DSTGetLastSqlError (NULL, &iLastErr, NULL);

      /* if the last error was due to 'DS' level */
      /* locking then change the lock level to 'ROW' */
      if (iLastErr == tt_ErrBadLockingLevel)
      {
        DSTExecute (hdbc,
          "CALL ttLockLevel ('ROW')");

        /* reset the status */
        bStatus = TRUE;
      }
      else if (iLastErr == tt_ErrUtilAlreadyRunning)
      {
        /* this case can occur due to a race conditions where the agent */
        /* has been started successfully but the call to */
        /* DSTIsCacheAgentRunning() still returns FALSE */

        /* reset the status and break */
        bStatus = TRUE;
        break;
      }
      else
      {
        /* the last error was something else so return FALSE and break */
        break;
      }

    }

    iNumControlAttempts ++;
  } 
  /* is the cache agent still running? */
  while (!DSTIsCacheAgentRunning (hdbc) &&
    iNumControlAttempts <= DST_MAX_AGENT_CONTROL_ATTEMPTS);


  return bStatus;
}

/*
* Function: DSTStopCacheAgent ()
*
* Description: Stops the cache agent for the hdbc. If cache grid
* is attached then the grid will be detached before stopping the
* agent. If the cache agent does not stop within 
* DST_ASYNC_TIMEOUT seconds defined in dst.h then it 
* will be killed by the main daemon.
*
*
* In: hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTStopCacheAgent (HDBC hdbc)
{
  return DSTStopCacheAgentWithTimeout (hdbc, 
    DST_ASYNC_TIMEOUT);
}


/*
* Function: DSTStopCacheAgentWithTimeout ()
*
* Description: Stops the cache agent for the hdbc. If cache grid
* is attached then the grid will be detached before stopping the
* agent. The call to stop the cache agent will timeout after 
* iTimeoutSecs. If iTimeoutSecs == 0 then the call will wait 
* forever for the agent to stop.
*
*
* In: hdbc, iTimeoutSec
*
* Out:
*
* In/Out:
*
*/
BOOL DSTStopCacheAgentWithTimeout (HDBC hdbc, SDWORD iTimeoutSecs)
{
  /* locals */
  BOOL bStatus = TRUE;
  SDWORD iLastErr = tt_ErrNone;
  SDWORD iNumControlAttempts = 0;
  DSTRING sql;

  STRInit (&sql, "CALL ttCacheStop (%d)");
  STRInsertInt (&sql, iTimeoutSecs);


  do
  {

    /* stop the agent */
    if (!DSTIsSuccess (DSTExecute (hdbc, STRC (sql))))
    {
      bStatus = FALSE;
      DSTGetLastSqlError (NULL, &iLastErr, NULL);

      /* if the last error was due to 'DS' level */
      /* locking then change the lock level to 'ROW' */
      if (iLastErr == tt_ErrBadLockingLevel)
      {
        DSTExecute (hdbc, 
          "CALL ttLockLevel ('ROW')");

        /* reset the status */
        bStatus = TRUE;
      }
      else if (iLastErr == tt_ErrUtilAlreadyStopped)
      {
        /* this case can occur due to a race conditions where the agent */
        /* has been stopped successfully but the call to */
        /* DSTIsCacheAgentRunning() still returns TRUE */

        /* reset the status and break */
        bStatus = TRUE;
        break;
      }
      else
      {
        /* the last error was something else so return FALSE and break */
        break;
      }

    }

    iNumControlAttempts ++;

  } 
  /* is the cache agent really stopped? */
  while (DSTIsCacheAgentRunning (hdbc) &&
    iNumControlAttempts <= DST_MAX_AGENT_CONTROL_ATTEMPTS);


  STRFree (&sql);

  return bStatus;
}


/*
* Function: DSTGetCacheSql ()
*
* Description: Returns the SQL script generated by invoking
* the ttCacheSqlGet () built in procedure.
*
* In: hdbc, pszFeature, pszCacheGroupName, iInstallFlag
*
* Out: pSqlScript
*
* In/Out:
*
*/
BOOL DSTGetCacheSql (HDBC hdbc,
                     const char* pszFeature,
                     const char* pszCacheGroupName,
                     SDWORD iInstallFlag,
                     DSTRING* pSqlScript)
{
  BOOL bStatus = TRUE;
  DSTRING sql;
  HSTMT hstmt;

  DST_LOG_REPORT_ME ("DSTGetCacheSql");

  assert (hdbc != SQL_NULL_HDBC &&
    pszFeature != NULL &&
    (iInstallFlag == 0 || iInstallFlag == 1) &&
    pSqlScript != NULL);


  /* build the call */
  STRInit (&sql, "CALL ttCacheSqlGet ('%s', '%s', %d)");
  STRInsertCString (&sql, pszFeature);

  if (pszCacheGroupName == NULL)
    STRInsertCString (&sql, "");
  else
    STRInsertCString (&sql, pszCacheGroupName);

  STRInsertInt (&sql, iInstallFlag);


  /* clear the output buffer */
  STRCopyCString (pSqlScript, "");

  /* execute */
  DSTAllocStmt (hdbc, &hstmt);

  if (!DSTIsSuccess (DSTExecDirectStmt (hstmt,
    STRC (sql))))
  {
    bStatus = FALSE;
  }
  else
  {
    /* get the result set */
    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsString (hstmt, 1, &sql);

      /* append the SQL to the output buffer */
      /* but don't append  comments  */
      if (!STRBeginsWithCString (sql, "--"))
        STRAppend (pSqlScript, sql);

    }
  }

  DSTFreeStmt (hstmt);

  STRFree (&sql);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTSetAgingOff ()
*
* Description: Turns row aging off for all tables
* in the database.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetAgingOff (HDBC hdbc)
{
  BOOL bStatus = TRUE;
  DSTRING sqlScript, owner, name;
  HSTMT hstmt;
  SDWORD iNumFailures = 0;

  DST_LOG_QUIET_ME ("DSTSetAgingOff");

  STRInit (&sqlScript, "");
  STRInit (&owner, "");
  STRInit (&name, "");

  DSTAllocStmt (hdbc, &hstmt);

  /* order by SYS.ALL_OBJECTS.OBJECT_ID to make sure the parent-child */
  /* execution order is correct */
  if (!DSTIsSuccess (DSTExecDirectStmt (hstmt,
    "SELECT RTRIM (TBLOWNER), RTRIM (TBLNAME) "
    "FROM SYS.TABLES, SYS.ALL_OBJECTS "
    "WHERE OBJECT_TYPE = 'TABLE' "
    "AND RTRIM (TBLOWNER) = SYS.ALL_OBJECTS.OWNER "
    "AND RTRIM (TBLNAME) = OBJECT_NAME "
    "AND TBLOWNER != 'TTREP' AND TBLOWNER != 'SYS' "
    "AND (SYS25 & 2048 != 0 OR SYS25 & 4096 != 0) "
    "AND (SYS25 & 6144 != 0 AND SYS25 & 16384 != 0) "
    "ORDER BY OBJECT_ID ASC ")))
  {
    bStatus = FALSE;
  }
  else
  {

    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsString (hstmt, 1, &owner);
      DSTGetColAsString (hstmt, 2, &name);

      STRAppendCString (&sqlScript,
        "ALTER TABLE %s.%s SET AGING OFF;\n");

      DSTDoubleQuoteIdentifier (&owner);
      DSTDoubleQuoteIdentifier (&name);

      STRInsertString (&sqlScript, owner);
      STRInsertString (&sqlScript, name);
    }

    /* execute the script */
    DSTExecuteFromBuffer (hdbc, &sqlScript,
      NULL, &iNumFailures);

    if (iNumFailures > 0)
      bStatus = FALSE;
  }


  DSTFreeStmt (hstmt);

  STRFree (&sqlScript);
  STRFree (&owner);
  STRFree (&name);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTSetAgingOn ()
*
* Description: Turns row aging on for all tables
* in the database.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTSetAgingOn (HDBC hdbc)
{
  BOOL bStatus = TRUE;
  DSTRING sqlScript, owner, name;
  HSTMT hstmt;
  SDWORD iNumFailures = 0;

  DST_LOG_QUIET_ME ("DSTSetAgingOn");

  STRInit (&sqlScript, "");
  STRInit (&owner, "");
  STRInit (&name, "");

  DSTAllocStmt (hdbc, &hstmt);

  /* order by SYS.ALL_OBJECTS.OBJECT_ID to make sure the parent-child */
  /* execution order is correct */
  if (!DSTIsSuccess (DSTExecDirectStmt (hstmt,
    "SELECT RTRIM (TBLOWNER), RTRIM (TBLNAME) "
    "FROM SYS.TABLES, SYS.ALL_OBJECTS "
    "WHERE OBJECT_TYPE = 'TABLE' "
    "AND RTRIM (TBLOWNER) = SYS.ALL_OBJECTS.OWNER "
    "AND RTRIM (TBLNAME) = OBJECT_NAME "
    "AND TBLOWNER != 'TTREP' AND TBLOWNER != 'SYS' "
    "AND (SYS25 & 2048 != 0 OR SYS25 & 4096 != 0) "
    "AND (SYS25 & 6144 = 0 OR SYS25 & 16384 = 0) "
    "ORDER BY OBJECT_ID DESC ")))
  {
    bStatus = FALSE;
  }
  else
  {

    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsString (hstmt, 1, &owner);
      DSTGetColAsString (hstmt, 2, &name);

      STRAppendCString (&sqlScript,
        "ALTER TABLE %s.%s SET AGING ON;\n");

      DSTDoubleQuoteIdentifier (&owner);
      DSTDoubleQuoteIdentifier (&name);

      STRInsertString (&sqlScript, owner);
      STRInsertString (&sqlScript, name);
    }

    /* execute the script */
    DSTExecuteFromBuffer (hdbc, &sqlScript,
      NULL, &iNumFailures);

    if (iNumFailures > 0)
      bStatus = FALSE;
  }


  DSTFreeStmt (hstmt);


  STRFree (&sqlScript);
  STRFree (&owner);
  STRFree (&name);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTSetStatsPollSec ()
*
* Description: Sets the polling interval for the ttStats collection agent.
* A value of 0 turns off ttStats collection. The old polling interval is 
* optionally returned in piOldPollSec.
*
* In:  hdbc, iNewPollSec
*
* Out: piOldPollSec.
*
* In/Out:
*
*/
BOOL DSTSetStatsPollSec (HDBC hdbc, SDWORD iNewPollSec, SDWORD* piOldPollSec)
{
  BOOL bStatus = TRUE;
  HSTMT hstmt = SQL_NULL_HSTMT;

  DST_LOG_QUIET_ME ("DSTSetStatsPollSec");

  /* return the current polling interval */
  if (piOldPollSec != NULL)
  {
    bStatus = DSTGetScalarResultAsInt (hdbc, 
      "SELECT VALUE FROM ttStatsConfigGet WHERE PARAM = 'POLLSEC'", 
      piOldPollSec);
  }


  /* set the new interval */
  if (bStatus && DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DSTSetParamAsInt (hstmt, 1, &iNewPollSec);
    
    bStatus = DSTIsSuccess (DSTExecDirectStmt (hstmt, 
      "CALL ttStatsConfig ('POLLSEC', ?)"));

    DSTFreeStmt (hstmt);
  }
  else
    bStatus = FALSE;


  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTDropAllObjects ()
*
* Description: This functions attempts to drop all user objects in the entire 
* database (or a specified schema) including replication schemes, cache groups, 
* materialized views, tables, regular views, sequences, synonyms and XLA 
* bookmarks. It will also shutdown running replication and cache agents 
* (but only when necessary to drop an object). If all targeted user objects are 
* dropped successfully then TRUE is returned.
*
* This procedure will not drop users or public synonyms or any system owned or 
* managed objects. The objects to drop can be restricted to a specific schema 
* specified in the pszSchema parameter. Set pszSchema to NULL to drop all user 
* objects in the entire database.
*
* By default this procedure will also not drop objects in the special 
* "TEST_RESULTS" or "TEST_DATA" accounts unless pszSchema is set expecitly to 
* drop objects in these accounts.
*
*
* In:  hdbc, pszSchema
*
* Out:
*
* In/Out:
*
*/
BOOL DSTDropAllObjects (const HDBC hdbc, const char* pszSchema)
{
  DSTRING objOwner, objName, objType, sql;
  DSTRING schema, schemaList, schemaItems, ttQuery;
  DSTRING mssg;
  size_t iNumSchemaItems = 0;
  DSTRING dropScript;
  HSTMT hstmtQuery;
  SDWORD iDropCount = 0, iErrCount = 0, iStmtCount = 0;
  BOOL bStatus = TRUE;
  SQLRETURN rc;
  BOOL bRepAgentStopped = FALSE, bCacheAgentStopped = FALSE;
  UDWORD iRepSchemeCount = 0, iCacheGroupCount = 0;


  DST_CONN_STATE_SAVE (hdbc);

  char szTTQuery [] =
    {
    "SELECT TYPE, OWNER, NAME, "
      "CASE TYPE "
        "WHEN 'XLA BOOKMARK' THEN 0 "
        "WHEN 'PACKAGE' THEN 40 "
        "WHEN 'PACKAGE BODY' THEN 60 "
        "WHEN 'REPLICATION' THEN 80 "
        "WHEN 'CACHE GROUP' THEN 100 "
        "WHEN 'MATERIALIZED VIEW' THEN 120 "
        "WHEN 'MATERIALIZED VIEW LOG ON ' THEN 140 "
        "WHEN 'VIEW' THEN 160 "
        "WHEN 'TABLE' THEN 180 "
        "WHEN 'SEQUENCE' THEN 200 "
        "WHEN 'SYNONYM' THEN 220 "
      "END AS TYPE_ORDER, "
      "OBJECT_ORDER "
      "FROM "

      "(SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'XLA BOOKMARK' AS TYPE, "
      "NULL AS OWNER, "
      "ID AS NAME "
      "FROM SYS.TRANSACTION_LOG_API WHERE INUSE != 0x01) XLA_BOOKMARKS "

      "UNION "

      "SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'REPLICATION' AS TYPE, "
      "RTRIM (REPLICATION_OWNER) AS OWNER, "
      "RTRIM (REPLICATION_NAME) AS NAME "
      "FROM TTREP.REPLICATIONS "
      "WHERE RTRIM (REPLICATION_NAME) != '_AWTREPSCHEME') REP_SCHEMES "

      "UNION "

      "SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'CACHE GROUP' AS TYPE, "
      "RTRIM (CGOWNER) AS OWNER, "
      "RTRIM (CGNAME) AS NAME "
      "FROM SYS.CACHE_GROUP) CACHE_GROUPS "

      "UNION "

      "SELECT TYPE, OWNER, NAME, TBLID AS OBJECT_ORDER "
      "FROM (SELECT "
      "'MATERIALIZED VIEW' AS TYPE, "
      "RTRIM (TBLOWNER) AS OWNER, "
      "RTRIM (TBLNAME) AS NAME, "
      "TBLID "
      "FROM SYS.TABLES "
      "WHERE RTRIM (TBLOWNER) NOT IN ('TTREP', 'SYS', 'GRID') "
      "AND MVID != 0) MATERIALIZED_VIEWS "

      "UNION "

      "SELECT TYPE, OWNER, NAME, TBLID AS OBJECT_ORDER "
      "FROM (SELECT "
      "'MATERIALIZED VIEW LOG ON ' AS TYPE, "
      "RTRIM (TBLOWNER) AS OWNER, "
      "RTRIM (TBLNAME) AS NAME, "
      "TBLID "
      "FROM SYS.TABLES "
      "WHERE TBLID IN (SELECT TO_NUMBER (SUBSTR (TBLNAME, 8)) FROM SYS.TABLES "
      "WHERE TBLNAME LIKE 'MVLOG$%') "
      "AND RTRIM (TBLOWNER) NOT IN ('TTREP', 'SYS', 'GRID')) MATERIALIZED_VIEW_LOGS "

      "UNION "

      "SELECT TYPE, OWNER, NAME, TBLID AS OBJECT_ORDER "
      "FROM (SELECT "
      "'TABLE' AS TYPE, "
      "RTRIM (TBLOWNER) AS OWNER, "
      "RTRIM (TBLNAME) AS NAME, "
      "TBLID "
      "FROM SYS.TABLES "
      "WHERE RTRIM (TBLOWNER) NOT IN ('TTREP', 'SYS', 'GRID') "
      "AND TBLNAME NOT LIKE 'MV%$%' AND MVID = 0 "
      "AND TBLNAME NOT LIKE 'CD$%' "
      "AND NOT EXISTS (SELECT 1 FROM SYS.CACHE_GROUP "
      "WHERE INSTR (SYS4, CONCAT (CONCAT (TRIM (TBLOWNER),'.'),TRIM (TBLNAME))) != 0)) "
      "TABLES "

      "UNION "

      "SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'VIEW' AS TYPE, "
      "RTRIM (OWNER) AS OWNER, "
      "RTRIM (NAME) AS NAME "
      "FROM SYS.VIEWS "
      "WHERE TBLID IS NULL AND "
      "RTRIM (OWNER) NOT IN ('TTREP', 'SYS', 'GRID')) VIEWS "

      "UNION "

      "SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'SEQUENCE' AS TYPE, "
      "RTRIM (OWNER) AS OWNER, "
      "RTRIM (NAME) AS NAME "
      "FROM SYS.SEQUENCES "
      "WHERE RTRIM (OWNER) NOT IN ('TTREP', 'SYS', 'GRID') "
      "AND NAME NOT LIKE 'MVSEQ$%') SEQUENCES "

      "UNION "

      "SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'SYNONYM' AS TYPE, "
      "OWNER AS OWNER, "
      "SYNONYM_NAME AS NAME "
      "FROM SYS.ALL_SYNONYMS "
      "WHERE OWNER NOT IN ('TTREP', 'SYS', 'GRID', 'PUBLIC')) SYNONYMS "

      "UNION "

      "SELECT TYPE, OWNER, NAME, 0 AS OBJECT_ORDER "
      "FROM (SELECT "
      "'PACKAGE BODY' AS TYPE, "
      "OWNER AS OWNER, "
      "OBJECT_NAME AS NAME "
      "FROM SYS.ALL_OBJECTS "
      "WHERE OWNER NOT IN ('TTREP', 'SYS', 'GRID', 'PUBLIC') "
      "AND OBJECT_TYPE = 'PACKAGE BODY') PACKAGE_BODIES ) "

    "WHERE (("
      
      "OWNER LIKE "
        "CASE WHEN CAST (:schema AS VARCHAR2 (32)) IS NULL THEN '%' "
        "ELSE CAST (:schema AS VARCHAR2 (32)) END "

      "AND (OWNER != '" DST_TEST_DATA_SCHEMA "' OR "
      "'" DST_TEST_DATA_SCHEMA "' LIKE CAST (:schema AS VARCHAR2 (32))) "
      
      "AND (OWNER != '" DST_TEST_RESULTS_SCHEMA "' OR "
      "'" DST_TEST_RESULTS_SCHEMA "' LIKE CAST (:schema AS VARCHAR2 (32))) ) "
    
      "OR OWNER IS NULL) "

      /* optional condition */
      "%s"

    "ORDER BY TYPE_ORDER ASC, OBJECT_ORDER DESC "};


  /* when executing against Oracle, only objects in the Oracle user's schema */
  /* can be dropped */
  char szORAQuery [] =
    {
    "SELECT "
    "OBJECT_TYPE AS TYPE, USER AS OWNER, OBJECT_NAME AS NAME, 0 AS DROP_ORDER "
    "FROM USER_OBJECTS "
    "WHERE OBJECT_NAME NOT LIKE 'SYS_%' "
    "AND OBJECT_NAME NOT LIKE 'BIN%'"};

  DST_LOG_REPORT_ME ("DSTDropAllObjects");
  DSTLogODBCHandle ("HDBC", hdbc);

  STRInit (&sql, "");
  
  /* report the target schemas */
  STRInit (&mssg, "Target schema(s): %s");

  if (pszSchema == NULL)
    STRInsertCString (&mssg, "<All Schemas>");
  else
    STRInsertCString (&mssg, pszSchema);

  DST_LOG (STRC (mssg));


  /* dropping cache groups requires READ_COMMITTED isolation */
  if (DSTGetIsolation (hdbc) == SQL_TXN_SERIALIZABLE)
    DSTSetIsolation (hdbc, SQL_TXN_READ_COMMITTED);



  /* passthrough and Pro*C connections due not work with these functions */
  if (DSTGetPassThrough (hdbc) != 3 && !DSTIsProCConn (hdbc))
  {

    /* stop running rep. agent if necessary */
    if (DSTIsRepAgentRunning (hdbc))
    {
      if (!DSTIsSuccess (DSTExecute (hdbc, "CALL ttRepStop ()")))
        bStatus = FALSE;
      else
        bRepAgentStopped = TRUE;
    }

    /* stop running the cache agent if necessary */
    if (DSTIsCacheAgentRunning (hdbc))
    {
      if (!DSTStopCacheAgent (hdbc))
        bStatus = FALSE;
      else
        bCacheAgentStopped = TRUE;
    }

    /* if the agents could not be stopped then abort */
    if (!bStatus)
    {
      DST_LOG_ERROR ("Unable to stop agent(s).");
      DST_CONN_STATE_RESTORE (hdbc);

      STRFree (&sql);
      STRFree (&mssg);
      DST_LOG_UNREPORT_ME;
      return FALSE;
    }
  }

  /* init string buffers */
  STRInit (&objType, "");
  STRInit (&objOwner, "");
  STRInit (&objName, "");
  STRInit (&dropScript, "");
  STRInit (&ttQuery, szTTQuery);
  STRInit (&schema, "");
  STRInit (&schemaList, "");
  STRInit (&schemaItems, "");



  DSTAllocStmt (hdbc, &hstmtQuery);

  /* restrict the dropped objects to a specific schema? */

  /* The pszSchema argument may be NULL, a single item or a */
  /* comma delimeted list of items. */
  if (pszSchema != NULL)
  {
    STRCopyCString (&schemaItems, pszSchema);

    /* a list? */
    iNumSchemaItems = STRGetNumTokens (schemaItems, "," STR_WHITESPACE_CHARS);

    if (iNumSchemaItems > 1)
    {
      size_t iItemIndex = 0;

      /* create an IN list for items at position 2 and greater */
      for (iItemIndex = 2; iItemIndex <= iNumSchemaItems; iItemIndex ++)
      {
        STRGetTokenAt (schemaItems, iItemIndex, "," STR_WHITESPACE_CHARS, &schema);
        STRAppendCString (&schemaList, "'%s', ");
        STRInsertString (&schemaList, schema);
      }

      STRChop (&schemaList, 2);

      /* get the first item in the list */
      STRGetTokenAt (schemaItems, 1, "," STR_WHITESPACE_CHARS, &schema);
    }
  }


  /* note that normally only the first parameter needs to be set */
  /* since the others are named the same, however, OCI */
  /* complains about this for some reason (even for Oracle connections) */
  if (pszSchema == NULL)
  {
    DSTSetParamAsNull (hstmtQuery, 1);
    DSTSetParamAsNull (hstmtQuery, 2);
    DSTSetParamAsNull (hstmtQuery, 3);
    DSTSetParamAsNull (hstmtQuery, 4);

    /* clear the optional condition */
    STRInsertCString (&ttQuery, "");
  }
  else if (iNumSchemaItems == 1)
  {
    DSTSetParamAsString (hstmtQuery, 1, pszSchema);
    DSTSetParamAsString (hstmtQuery, 2, pszSchema);
    DSTSetParamAsString (hstmtQuery, 3, pszSchema);
    DSTSetParamAsString (hstmtQuery, 4, pszSchema);

    /* clear the optional condition */
    STRInsertCString (&ttQuery, "");
  }
  else
  {
    /* handle a list of schema items */

    /* set the parameter */
    DSTSetParamAsString (hstmtQuery, 1, STRC (schema));
    DSTSetParamAsString (hstmtQuery, 2, STRC (schema));
    DSTSetParamAsString (hstmtQuery, 3, STRC (schema));
    DSTSetParamAsString (hstmtQuery, 4, STRC (schema));

    /* add an IN list to the query */
    STRInsertCString (&ttQuery, " OR OWNER = ANY (%s) ");
    STRInsertString (&ttQuery, schemaList);
  }

  do
  {
    /* initialize this iteration */
    iStmtCount = 0;
    iDropCount = 0;
    iErrCount = 0;
    STRCopyCString (&dropScript, "");

    /* execute the object query */
    DST_LOG_QUIET;

    if (DSTGetPassThrough (hdbc) == 3)
      rc = DSTExecDirectStmt (hstmtQuery, szORAQuery);
    else
      rc = DSTExecDirectStmt (hstmtQuery, STRC (ttQuery));

    DST_LOG_RESTORE;

    /* abort? */
    if (!DSTIsSuccess (rc))
    {
      iErrCount = 1;
      break;
    }

    /* iterate thru the objects */
    while (DSTIsSuccess (DSTFetch (hstmtQuery)))
    {
      DSTGetColAsString (hstmtQuery, 1, &objType);
      DSTGetColAsString (hstmtQuery, 2, &objOwner);
      DSTGetColAsString (hstmtQuery, 3, &objName);

      STRCopyCString (&sql, "DROP %s \"%s\".\"%s\";\n");
      STRInsertString (&sql, objType);
      STRInsertString (&sql, objOwner);
      STRInsertString (&sql, objName);


      /* an ACTIVE-STANDBY pair is a special case */
      if (STREqualsCString (objName, "_ACTIVESTANDBY"))
      {
        STRCopyCString (&sql, "DROP ACTIVE STANDBY PAIR;\n");
      }

      /* an XLA bookmark is a special case */
      if (STREqualsCString (objType, "XLA BOOKMARK"))
      {
        STRCopyCString (&sql, "CALL ttXlaBookmarkDelete ('%s');\n");
        STRInsertString (&sql, objName);
      }

      /* append the SQL to the script */
      STRAppend (&dropScript, sql);
      iStmtCount ++;
    }

    DSTCloseStmtCursor (hstmtQuery);

    /* execute all statements in the script */
    if (iStmtCount > 0)
    {
      DSTExecuteFromBuffer (hdbc, &dropScript,
        &iDropCount, &iErrCount);
    }

  }
  while (iDropCount > 0);

  DSTFreeStmt (hstmtQuery);

  STRFree (&schema);
  STRFree (&schemaList);
  STRFree (&schemaItems);
  STRFree (&objType);
  STRFree (&objOwner);
  STRFree (&objName);
  STRFree (&sql);
  STRFree (&dropScript);
  STRFree (&ttQuery);
  STRFree (&mssg);


  /* restore previously running agents only if cache groups and/or */
  /* rep. schemes still exist in the store */
  if (DSTGetPassThrough (hdbc) != 3)
  {
    DSTGetQueryRowCount (hdbc, 
      "SELECT * FROM SYS.CACHE_GROUP", &iCacheGroupCount);

    if (bCacheAgentStopped && iCacheGroupCount >= 1)
      DSTExecute (hdbc, "CALL ttCacheStart ()");

    DSTGetQueryRowCount (hdbc, 
      "SELECT * FROM TTREP.REPLICATIONS", &iRepSchemeCount);

    if (bRepAgentStopped && iRepSchemeCount >= 1)
      DSTExecute (hdbc, "CALL ttRepStart ()");
  }


  /* return success if no errors were encountered on the */
  /* last iteration */
  if (iErrCount == 0)
  {
    if (DSTGetPassThrough (hdbc) == 3)
      DST_LOG ("All Oracle user objects dropped.");
    else
      DST_LOG ("All user objects dropped.");

    bStatus = TRUE;
  }
  else
  {
    if (DSTGetPassThrough (hdbc) == 3)
      DST_LOG_ERROR ("Failure to drop all Oracle user objects.");
    else
      DST_LOG_ERROR ("Failure to drop all user objects.");

    bStatus = FALSE;
  }

  DST_CONN_STATE_RESTORE (hdbc);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTDropObject ()
*
* Description: This functions attempts to drop the specified object
* in the database. If pszObjectOwner is NULL then the object is
* searched for in the current schema. Valid object types include
* replication schemes, cache groups, materialized views, tables,
* regular views, sequences and XLA bookmarks. If the object was dropped
* successfully then TRUE is returned.
*
* In:  hdbc, pszObjectOwner, pszObjectName
*
* Out:
*
* In/Out:
*
*/
BOOL DSTDropObject (const HDBC hdbc,
                    const char* pszObjectOwner,
                    const char* pszObjectName)
{
  BOOL bStatus = FALSE;
  HSTMT hstmt;
  DSTRING sql;

  char szTTQuery [] =
  {
    "SELECT 'DROP ' || OBJECT_TYPE || ' ' || '\"' || OWNER || '\".\"' || OBJECT_NAME || '\"' "
    "FROM ALL_OBJECTS "
    "WHERE OWNER NOT IN ('SYS', 'TTREP', 'GRID') "
    "AND OWNER = (CASE WHEN CAST (:owner AS VARCHAR2 (30)) IS NULL THEN USER ELSE :owner END) "
    "AND OBJECT_NAME = :name"
  };

  DST_CONN_STATE_SAVE (hdbc);
  DST_LOG_REPORT_ME ("DSTDropObject");

  STRInit (&sql, "");

  DSTAllocStmt (hdbc, &hstmt);

  if (pszObjectOwner == NULL)
    DSTSetParamAsNull (hstmt, 1);
  else
    DSTSetParamAsString (hstmt, 1, pszObjectOwner);

  DSTSetParamAsString (hstmt, 3, pszObjectName);

  DST_LOG_QUIET;
  DSTExecDirectStmt (hstmt, szTTQuery);
  DST_LOG_RESTORE;

  if (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 1, &sql);

    if (DSTIsSuccess (DSTExecute (hdbc, STRC (sql))))
      bStatus = TRUE;
  }
  else
    DST_LOG ("Object not found.");

  DSTFreeStmt (hstmt);

  STRFree (&sql);

  DST_CONN_STATE_RESTORE (hdbc);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}



/*
* Function: DSTRefreshAllViews ()
*
* Description: This function refreshes all asynchronous materialized
* views in the database. If all views are refreshed successfully then
* TRUE is returned.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
BOOL DSTRefreshAllViews (const HDBC hdbc)
{
  BOOL bStatus = TRUE;
  HSTMT hstmt;
  DSTRING sql;

  DST_LOG_QUIET_ME ("DSTRefreshAllViews");

  STRInit (&sql, "");

  DSTAllocStmt (hdbc, &hstmt);

  if (DSTGetPassThrough (hdbc) < 3)
  {
    if (!DSTIsSuccess (DSTExecDirectStmt (hstmt,
      "SELECT 'REFRESH MATERIALIZED VIEW \"' || "
        "RTRIM (OWNER) || '\".\"' || RTRIM (NAME) || '\"' "
      "FROM SYS.VIEWS "
      "WHERE TBLID IS NOT NULL AND (SYS4 & 8192 != 0 OR SYS4 & 16384 <> 0) ")))
    {
      bStatus = FALSE;
    }
  }
  /* this is an Oracle connection */
  else if (!DSTIsSuccess (DSTExecDirectStmt (hstmt,
    "SELECT 'CALL DBMS_MVIEW.REFRESH (''' || OBJECT_NAME || ''', ''C'')' "
    "FROM USER_OBJECTS "
    "WHERE OBJECT_TYPE = 'MATERIALIZED VIEW'")))
  {
    bStatus = FALSE;
  }


  if (bStatus)
  {
    DST_LOG_VERBOSE;

    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsString (hstmt, 1, &sql);

      if (!DSTIsSuccess (DSTExecute (hdbc, STRC (sql))))
        bStatus = FALSE;
    }

    DST_LOG_RESTORE;
  }

  DSTFreeStmt (hstmt);

  STRFree (&sql);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTGetConnectionCount ()
*
* Description: This procedure calls ttDataStoreStatus ()
* to determine how many connections are active for the
* data source associated with the hdbc. You can choose
* to query only certain connection types by setting the
* pszTypes parameter to a combination of the following:
*
* "replication"
* "subdaemon"
* "cache agent"
* "application"
*
* If pszTypes is NULL then all connection types are
* queried.
*
* In:  hdbc, pszTypes
*
* Out:
*
* In/Out:
*
*/
SDWORD DSTGetConnectionCount (HDBC hdbc, char* pszTypes)
{
  /* locals */
  HSTMT hstmt;
  SDWORD iConnCount = 0;
  DSTRING connType, path, targetTypes;

  DST_LOG_QUIET_ME ("DSTGetConnectionCount");

  /* validate args. */
  assert (hdbc != SQL_NULL_HDBC);

  /* this procedure is not supported in TimesTen Grid */
  if (!DSTIsGrid (hdbc))
  {
    /* init. buffers */
    STRInit (&connType, DST_NULL_STRING);
    STRInit (&path, "");
    STRInit (&targetTypes, "");

    /* are there explicit connection types to query? */
    if (pszTypes != NULL)
    {
      STRCopyCString (&targetTypes, pszTypes);
      STRToLower (targetTypes);
    }

    /* get the full path to this database */
    DSTGetDataStorePath (hdbc, &path);

    /* call ttDataStoreStatus () */
    if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
    {
      DSTPrepareStmt (hstmt, "CALL ttDataStoreStatus (:p1)");
      DSTSetParamAsString (hstmt, 1, STRC (path));
      DSTExecPreparedStmt (hstmt);

      while (DSTIsSuccess (DSTFetch (hstmt)))
      {
        DSTGetColAsString (hstmt, 4, &connType);
        STRTrim (&connType, STR_WHITESPACE_CHARS);

        if (pszTypes == NULL)
        {
          iConnCount ++;
        }
        else if (STRHasSequence (targetTypes,
          STRC (connType), NULL))
        {
          iConnCount ++;
        }
      }

      DSTFreeStmt (hstmt);
    }

    STRFree (&path);
    STRFree (&connType);
    STRFree (&targetTypes);
  }

  DST_LOG_UNREPORT_ME;
  return iConnCount;
}

/*
* Function: DSTKillProcess ()
*
* Description: This procedure attempts to kill the process ID specified in iPid.
* If the process is killed successfully then TRUE is returned.
*
* In: iPid
*
* Out:
*
* In/Out:
*
* Global Properties:
* 
*/
BOOL DSTKillProcess (DWORD iPid)
{
  BOOL bStatus = FALSE;
  DSTRING mssg;

  DST_LOG_REPORT_ME ("DSTKillProcess");

  STRInit (&mssg, "Killing process %u ...");
  STRInsertUnsignedInt (&mssg, iPid);
  DST_LOG (STRC (mssg));

#ifdef WIN32
  {
    HANDLE hProc;
    hProc = OpenProcess (SYNCHRONIZE|PROCESS_TERMINATE, TRUE, iPid);
              
    if (TerminateProcess (hProc,0) == 0)
    {
      bStatus = FALSE;
      DST_LOG_ERROR ("Failed to kill process.");
    }
    else
    {
      bStatus = TRUE;
      DST_LOG ("Succeeded.");
    }
  }
#else
  if (kill (iPid, SIGKILL) != 0)
  {
    bStatus = FALSE;
    DST_LOG_ERROR ("Failed to kill process.");
  }
  else
  {
    bStatus = TRUE;
    DST_LOG ("Succeeded.");
  }
#endif

  STRFree (&mssg);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTKillRandDBProcess ()
*
* Description: This procedure queries GV$DATASTORE_STATUS and randomly selects 
* a connected process to kill based on the input restrictions.
* 
* The types of processes to kill can be restricted by including one of these 
* tokens in the pszConnType parameter:
* 
* "replication"
* "subdaemon"
* "cache agent"
* "application"
* "GCW"
*
* A connection name in the pszConnName parameter can also be use to restrict 
* the set of processes to kill.
* 
* The type and name parameter values are case insensitive and selected via a 
* LIKE condition with a preceding and trailing '%' search pattern. If 
* pszConnType and pszConnName are NULL then all connections are targeted.
*
* An optional set of WHERE clause predicates can be included in pszWherePred
* to further restrict the result based on the columns in GV$DATASTORE_STATUS.
* This SQL fragment must begin with the AND or OR conjunctive.
*
* This procedure will not kill itself. If a connected process is selected and 
* killed successfully then TRUE is returned.
*
* In: hdbc, pszConnType, pszConnName, pszWherePred
*
* Out:
*
* In/Out:
*
* Global Properties:
* PID
* 
*/
BOOL DSTKillRandDBProcess (HDBC hdbc, const char* pszConnType, 
  const char* pszConnName, const char* pszWherePred)
{
  /* locals */
  BOOL bStatus = FALSE;
  SDWORD iThisPid = 0;
  HSTMT hstmt;
  DSTRING connType, connName, targetType;
  DSTRING sql, mssg;
  BOOL bIsGrid = FALSE;

  char pszGridQuery [] =
    "SELECT "
    "  * "
    "FROM GV$DATASTORE_STATUS "
  
    "WHERE  "
    "(TRIM (LOWER (CONTYPE)) LIKE '%' || TRIM (LOWER (:conntype)) || '%' "
    "  AND  "
    "TRIM (LOWER (CONNECTION_NAME)) LIKE '%' || TRIM (LOWER (:connname)) || '%')"
    " %s "
    "ORDER BY PID";


  DST_LOG_REPORT_ME ("DSTKillRandDBProcess");

  /* validate args. */
  assert (hdbc != SQL_NULL_HDBC);

  PROPGetAsInt ("PID", &iThisPid);


  /* init. buffers */
  STRInit (&connType, "");
  STRInit (&connName, "");
  STRInit (&targetType, "");
  STRInit (&mssg, "");
  STRInit (&sql, "");

  STRCopyCString (&mssg, "Selecting random process where CONNTYPE like '%s' "
    "and CONNECTION_NAME like '%s' ...");

  if (pszConnType == NULL)
    STRInsertCString (&mssg, "");
  else
    STRInsertCString (&mssg, pszConnType);


  if (pszConnName == NULL)
    STRInsertCString (&mssg, "");
  else
    STRInsertCString (&mssg, pszConnName);

  DST_LOG (STRC (mssg));



  /* is this a grid database */
  bIsGrid = DSTIsGrid (hdbc);


  DST_LOG_QUIET;


  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    SDWORD iConnCount = 0;

    /* count the possible victims */
    STRCopyCString (&sql, "SELECT COUNT (*) FROM (%s)");
    STRInsertCString (&sql, pszGridQuery);
    
    /* are additional SQL predicates specified? */
    if (pszWherePred != NULL)
      STRInsertCString (&sql, pszWherePred);
    else
      STRInsertCString (&sql, "");


    if (pszConnType == NULL)
      DSTSetParamAsNull (hstmt, 1);
    else
      DSTSetParamAsString (hstmt, 1, pszConnType);

    if (pszConnName == NULL)
      DSTSetParamAsNull (hstmt, 2);
    else
      DSTSetParamAsString (hstmt, 2, pszConnName);


    if (DSTIsSuccess (DSTExecDirectStmt (hstmt, STRC (sql))))
    {
      if (DSTIsSuccess (DSTFetch (hstmt)))
        DSTGetColAsInt (hstmt, 1, &iConnCount);

      DSTCloseStmtCursor (hstmt);
    }


    if (iConnCount == 0)
    {
      DST_LOG_RESTORE;
      DST_LOG ("No available connected processes to kill.");
    }
    else
    {
      /* select the victim */
      SDWORD iPid = 0;
      SDWORD iElementId = 0;
      SDWORD iTargetConn = (DSTRand () % iConnCount) + 1;

      STRCopyCString (&sql, pszGridQuery);

      /* are additional SQL predicates specified? */
      if (pszWherePred != NULL)
        STRInsertCString (&sql, pszWherePred);
      else
        STRInsertCString (&sql, "");


      /* in grid mode the global status is used */
      DSTExecDirectStmt (hstmt, STRC (sql));

      iConnCount = 0;

      while (DSTIsSuccess (DSTFetch (hstmt)))
      {
        iConnCount ++;

        DSTGetColAsInt (hstmt, 2, &iPid);
          
        DSTGetColAsString (hstmt, 4, &connType);
        STRTrim (&connType, STR_WHITESPACE_CHARS);
 
        DSTGetColAsString (hstmt, 6, &connName);
        STRTrim (&connName, STR_WHITESPACE_CHARS);

        if (bIsGrid)
          DSTGetColAsInt (hstmt, 8, &iElementId);


        /* is this the victim? */
        if (iConnCount == iTargetConn)
        {
          DST_LOG_RESTORE;

          /* close the cursor and free the statement handle to avoid hangs */
          /* when killing a GCW associated with this connection */
          DSTFreeStmt (hstmt);
          hstmt = SQL_NULL_HSTMT;

          if (iPid == iThisPid)
          {
            bStatus = FALSE;
            DST_LOG ("Aborting to avoid killing this process.");
            break;
          }

          STRCopyCString (&mssg, "Killing %s process %s (%d) ");
          STRInsertString (&mssg, connType);
          STRInsertString (&mssg, connName);
          STRInsertInt (&mssg, iPid);

          if (bIsGrid)
          {
            STRAppendCString (&mssg, "on element ID #%d ...");
            STRInsertInt (&mssg, iElementId);
          }
          else
            STRAppendCString (&mssg, "...");

          DST_LOG (STRC (mssg));
          bStatus = DSTKillProcess (iPid);

          break;
        }
      }
    }

    if (hstmt != SQL_NULL_HSTMT)
      DSTFreeStmt (hstmt);
  }



  STRFree (&sql);
  STRFree (&mssg);
  STRFree (&connName);
  STRFree (&connType);
 

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTFindDaemonCoreFiles ()
*
* Description: This procedure attempts to find daemon process core files located
* in the info/ directory of TimesTen installations. If at least one core file is
* found then this procedure returns TRUE.
* 
* In:
*
* Out:
*
* In/Out:
*
* Global Properties:
* TIMESTEN_HOME
*
*/
BOOL DSTFindDaemonCoreFiles ()
{
#ifndef WIN32
  BOOL bFoundCoreFile = FALSE;
  DSTRING ttHomeDir, ttInfoDir, ttBaseDir, ttCoreDir, coreFile, mssg;
  DIR *dir, *dirBase;
  struct dirent *ent, *entBase;
#endif

  DST_LOG_REPORT_ME ("DSTFindDaemonCoreFiles");

#ifdef WIN32
  DST_LOG ("This procedure is not supported on Windows platforms.");
  DST_LOG_UNREPORT_ME;
  return FALSE;
#else

  if (!PROPIsDefined ("TIMESTEN_HOME"))
  {
    DST_LOG ("The TIMESTEN_HOME environment variable is not defined.");
    DST_LOG_UNREPORT_ME;
    return FALSE;
  }


  STRInit (&ttHomeDir, "");
  STRInit (&ttInfoDir, "");
  STRInit (&ttBaseDir, "");
  STRInit (&ttCoreDir, "");
  STRInit (&coreFile, "");
  STRInit (&mssg, "");

  PROPGetAsString ("TIMESTEN_HOME", &ttHomeDir);



  /* search for core files in TimesTen installations that may be */
  /* located in the same directory where this installation is located */
  STRCopy (&ttBaseDir, ttHomeDir);
  STRAppendCString (&ttBaseDir, "/../");

  if ((dirBase = opendir (STRC (ttBaseDir))) != NULL)
  {
    /* find info directories */
    while ((entBase = readdir (dirBase)) != NULL)
    {
      struct stat inode;

      STRCopy (&ttInfoDir, ttBaseDir);
      STRAppendCString (&ttInfoDir, entBase->d_name);

      if (strcmp (entBase->d_name, ".") != 0 &&
          strcmp (entBase->d_name, "..") != 0 &&
          stat (STRC (ttInfoDir), &inode) == 0 &&
          S_ISDIR (inode.st_mode))
      {
        /* candidate directory? */
        STRAppendCString (&ttInfoDir, "/info/");
        STRCopyCString (&mssg, "\nSearching %s ...");
        STRInsertString (&mssg, ttInfoDir);

        /* search the info directory of the TT home directory first */
        if ((dir = opendir (STRC (ttInfoDir))) != NULL)
        {
          DST_LOG (STRC (mssg));

          /* iterate through all the files and directories within directory */
          while ((ent = readdir (dir)) != NULL)
          {
            STRCopyCString (&coreFile, ent->d_name);

            if (STRBeginsWithCString (coreFile, "core"))
            {
              struct stat inode;

              STRCopy (&ttCoreDir, ttInfoDir);
              STRAppend (&ttCoreDir, coreFile);


              /* is this a directory */
              if (stat (STRC (ttCoreDir), &inode) == 0 &&
                  S_ISDIR (inode.st_mode))
              {
                /* directory named core found */;
                continue;
              }


              bFoundCoreFile = TRUE;
              STRCopyCString (&mssg, "Core file found: %s%s");

              STRInsertString (&mssg, ttInfoDir);
              STRInsertString (&mssg, coreFile);

              DST_LOG_ERROR (STRC (mssg));
            }
          }

          closedir (dir);
        }
      }
    }

    closedir (dirBase);
  }


  if (!bFoundCoreFile)
    DST_LOG ("No core files found.");


  STRFree (&mssg);
  STRFree (&coreFile);
  STRFree (&ttCoreDir);
  STRFree (&ttBaseDir);
  STRFree (&ttInfoDir);
  STRFree (&ttHomeDir);

  DST_LOG_UNREPORT_ME;
  return bFoundCoreFile;
#endif
}

/*
* Function: DSTIsCommonSHMKEY  ()
*
* Description: This procedure calls ttDataStoreStatus () to
* determine if all the connections to the database associated
* with hdbc use the same Shared Memory KEY. Sets shmID with the
* shmKEY associated with the subdaemon
*
* In:  hdbc, shmID
*
* Out:
*
* In/Out:
*
*/
BOOL DSTIsCommonSHMKEY  (HDBC hdbc, DSTRING* shmID)
{
  /* locals */
  BOOL bStatus = TRUE;
  HSTMT hstmt;
  DSTRING message, connType, path;
  SDWORD iConnCount = 0;

  DST_LOG_REPORT_ME ("DSTIsCommonSHMKEY");

  /* validate args. */
  assert (hdbc != SQL_NULL_HDBC);

  STRInit (&message, "");
  STRInit (&connType, "");
  STRInit (&path, "");

  /* get the full path to this database */
  DSTGetDataStorePath (hdbc, &path);

  /* call ttDataStoreStatus () */
  DSTAllocStmt (hdbc, &hstmt);
  DSTSetParamAsString (hstmt, 1, STRC (path));
  DSTExecDirectStmt (hstmt, "{CALL ttDataStoreStatus (:p1)}");


  /* get the shmkeys and compare the applications' to the subdaemon's */
  while (DSTIsSuccess (DSTFetch (hstmt)))
  {
    DSTGetColAsString (hstmt, 4, &connType);
    STRToLower (connType);
    STRTrim (&connType, STR_WHITESPACE_CHARS);

    if (STREqualsCString (connType, "subdaemon"))
    {
      DSTGetColAsString (hstmt, 5, shmID);
      STRTrim (shmID, STR_WHITESPACE_CHARS);
      STRCopyCString (&message, "subdaemon SHMKEY:\t");
      STRAppend (&message, *shmID);
      DST_LOG(STRC (message));
      iConnCount ++;
    }
    else if (STREqualsCString (connType, "application"))
    {
      DSTGetColAsString (hstmt, 5, &connType);
      STRTrim (&connType, STR_WHITESPACE_CHARS);
      STRCopyCString (&message, "application SHMKEY:\t");
      STRAppend (&message, connType);
      DST_LOG(STRC (message));

      if (!STREquals (*shmID, connType))
      {
        bStatus = FALSE;
      }

      iConnCount ++;
    }

  }

  DSTFreeStmt (hstmt);

  STRFree (&message);
  STRFree (&connType);
  STRFree (&path);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

/*
* Function: DSTRegisterFailoverCallback  ()
*
* Description: This call registers a callback function and a pointer to a user 
* defined data item that will be called when a failover event occurs for a 
* client/server connection. The callback function and the pointer to the data 
* item can be NULL. If the callback function is NULL that tells the driver that 
* a callback will not occur when a failover event occurs. If registration is 
* successful then TRUE is returned.
*
* In:  hdbc, callbackFunc, pUserInfo
*
* Out:
*
* In/Out:
*
*/
BOOL DSTRegisterFailoverCallback (HDBC hdbc,
                                  ttFailoverCallbackFcn_t callbackFunc,
                                  SQLPOINTER pUserInfo)
{
  BOOL bStatus = TRUE;
  SQLRETURN rc;
  ttFailoverCallback_t failoverCallback;

  DST_LOG_REPORT_ME ("DSTRegisterFailoverCallback");
  DSTLogODBCHandle ("HDBC", hdbc);


  if ((void*)callbackFunc == NULL)
    DST_LOG ("Callback function set to NULL.");


  /* register the callback */
  failoverCallback.appHdbc = hdbc;
  failoverCallback.foCtx = pUserInfo;
  failoverCallback.callbackFcn = callbackFunc;

  rc = SQLSetConnectOption (hdbc, TT_REGISTER_FAILOVER_CALLBACK,
    (SQLULEN) &failoverCallback);

  if (rc == SQL_ERROR)
  {
    bStatus = FALSE;
    DST_LOG_SQL_ERROR (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLSetConnectOption () failed.");
  }
  else if (rc == SQL_SUCCESS_WITH_INFO)
  {
    DST_LOG_SQL_INFO (SQL_NULL_HENV, hdbc, SQL_NULL_HSTMT,
      "SQLSetConnectOption () info.");
  }


  DST_LOG_UNREPORT_ME;
  return bStatus;
}


/*
* Function: DSTRegisterFailoverStatsCallback  ()
*
* Description: This is a specific version of DSTRegisterFailoverCallback() used 
* to collect statistics on failover events.
*
* In:  hdbc, statsCallbackFunc, pStats
*
* Out:
*
* In/Out:
*
*/
BOOL DSTRegisterFailoverStatsCallback (HDBC hdbc,
                                       DSTFailoverStatsCallbackFunc statsCallbackFunc,
                                       struct DSTFOSTATS* pStats)
{
  return DSTRegisterFailoverCallback (hdbc,
    (ttFailoverCallbackFcn_t) statsCallbackFunc,
    (SQLPOINTER) pStats);
}


/*
* Function: DSTFailoverStatsCallback  ()
*
* Description: This is a callback function that can be registered with the 
* client/server driver in order to collect statistics on failover events. An 
* instance of the DSTFOSTATS structure is updated by the function to record the 
* statistics.
*
* In:  hdbc, pFOStats, iFOType, iFOEvent
*
* Out:
*
* In/Out:
*
*/
SQLRETURN DSTFailoverStatsCallback (HDBC hdbc,
                                    struct DSTFOSTATS *pFOStats,
                                    SQLUINTEGER iFOType,
                                    SQLUINTEGER iFOEvent)

{
  SQLRETURN rc = SQL_SUCCESS;
  DSTRING mssg;

  /* this is the only failover type supported */
  assert (iFOType == TT_FO_SESSION);

#ifdef DST_INCLUDE_COREXEL
  DSTSetLogThreadId ((sbThread) hdbc);
#endif
  DST_LOG ("DSTFailoverStatsCallback ()...");

  STRInit (&mssg, "");

  /* update the stats structure */
  if (pFOStats != NULL)
  {
    pFOStats->iCallCount ++;
    pFOStats->iLastFoEvent = iFOEvent;

    /* display the call count */
    STRCopyCString (&mssg, "Call count: %d");
    STRInsertInt (&mssg, pFOStats->iCallCount);
    DST_LOG (STRC (mssg));
  }


  /* display the event type */
  STRCopyCString (&mssg, "Failover event: ");

  switch (iFOEvent)
  {
    case TT_FO_BEGIN:
      STRAppendCString (&mssg, "TT_FO_BEGIN (Failover begin...)");
      break;

    case TT_FO_END:
      STRAppendCString (&mssg, "TT_FO_END (Failover success)");
      break;

    case TT_FO_ABORT:
      STRAppendCString (&mssg, "TT_FO_ABORT (Failover failed, "
        "TTC_TIMEOUT interval exceeded)");
      break;

    case TT_FO_REAUTH:
      STRAppendCString (&mssg, "TT_FO_REAUTH (Not supported)");
      break;

    case TT_FO_ERROR:
      STRAppendCString (&mssg, "TT_FO_ERROR (Failover error, retrying...)");
      break;

    default:
      assert (FALSE);
  }

  DST_LOG (STRC (mssg));

  STRFree (&mssg);
  return rc;
}


/*
* Function: DSTGetLogHoldFileCount ()
*
* Description: This procedure determines how many log files are being held for 
* the database associated with the hdbc. Log files are held due to long running 
* transactions, unreplicated transactions, persistent XLA bookmarks, XA 
* transactions and incremental backups. Checkpoints are not counted as log 
* holds.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
SDWORD DSTGetLogHoldFileCount (HDBC hdbc)
{
  /* locals */
  SDWORD iLogFile = 0;
  SDWORD iOldestLogHoldFile = 0;
  SDWORD iCurrentLogFile = 0;
  HSTMT hstmt;
  DSTRING holdType;

  DST_LOG_QUIET_ME ("DSTGetLogHoldFileCount");

  STRInit (&holdType, "");

  /* get the log file number of the transaction record */
  /* most recently forced to disk */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DSTExecDirectStmt (hstmt, "CALL ttBookMark ()");

    if (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsInt (hstmt, 3, &iCurrentLogFile);
      iOldestLogHoldFile = iCurrentLogFile;
    }

    DSTCloseStmtCursor (hstmt);


    /* get the log file number of the oldest log hold */
    /* that is not a checkpoint */
    DSTExecDirectStmt (hstmt, "CALL ttLogHolds ()");

    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsString (hstmt, 3, &holdType);

      if (!STRBeginsWithCString (holdType, "Checkpoint"))
      {
        DSTGetColAsInt (hstmt, 1, &iLogFile);

        if (iLogFile < iOldestLogHoldFile)
          iOldestLogHoldFile = iLogFile;
      }
    }

    DSTFreeStmt (hstmt);
  }

  STRFree (&holdType);

  DST_LOG_UNREPORT_ME;
  return (iCurrentLogFile - iOldestLogHoldFile) + 1;
}

/*
* Function: DSTGetRetainedLogFileCount ()
*
* Description: This procedure determines how many log files are being retained 
* for the database associated with the hdbc. Log files are retained due to long 
* running transactions, unreplicated transactions, persistent XLA bookmarks, XA 
* transactions, incremental backups and checkpoints.
*
* In:  hdbc
*
* Out:
*
* In/Out:
*
*/
SDWORD DSTGetRetainedLogFileCount (HDBC hdbc)
{
  /* locals */
  SDWORD iLogFile = 0;
  SDWORD iOldestLogHoldFile = 0;
  SDWORD iCurrentLogFile = 0;
  HSTMT hstmt;

  DST_LOG_QUIET_ME ("DSTGetRetainedLogFileCount");


  /* get the log file number of the transaction record */
  /* most recently forced to disk */
  if (DSTIsSuccess (DSTAllocStmt (hdbc, &hstmt)))
  {
    DSTExecDirectStmt (hstmt, "CALL ttBookMark ()");

    if (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsInt (hstmt, 3, &iCurrentLogFile);
      iOldestLogHoldFile = iCurrentLogFile;
    }

    DSTCloseStmtCursor (hstmt);


    /* get the log file number of the oldest log hold */
    DSTExecDirectStmt (hstmt, "CALL ttLogHolds ()");

    while (DSTIsSuccess (DSTFetch (hstmt)))
    {
      DSTGetColAsInt (hstmt, 1, &iLogFile);

      if (iLogFile < iOldestLogHoldFile)
        iOldestLogHoldFile = iLogFile;
    }

    DSTFreeStmt (hstmt);
  }

  DST_LOG_UNREPORT_ME;
  return (iCurrentLogFile - iOldestLogHoldFile) + 1;
}



/*
* Function: DSTMalloc  ()
*
* Description: This function is used to malloc () memory.
* If malloc () fails then this function reports the failure
* to STDERR with the file and line number of the call.
*
* In: iLineno, pszSourceFile, size
*
* Out:
*
* In/Out:
*
*/
void* DSTMalloc (int iLineno, const char* pszSourceFile, size_t size)
{
  /* locals */
  void* pPtr;


  pPtr = malloc (size);

  /* did the malloc () fail? */
  if (pPtr == NULL)
  {
    fprintf (stderr, DST_MSSG_MALLOC_FAILURE);

#ifdef TT64
    fprintf (stderr, "Bytes requested: %lu \nSource file: %s \nLine number: %d \n\n",
      size, pszSourceFile, iLineno);
#else
    fprintf (stderr, "Bytes requested: %u \nSource file: %s \nLine number: %d \n\n",
      size, pszSourceFile, iLineno);
#endif

    fflush (NULL);
  }


  return pPtr;
}


/*
* Function: DSTUseXATxns  ()
*
* Description: Returns true if the global property
* DST_USE_XA_TRANSACTIONS is set to true.
*
* In:
*
* Out:
*
* In/Out:
*
* Global Properties:
* DST_USE_XA_TRANSACTIONS
*
*/
BOOL DSTUseXATxns ()
{
  static BOOL bUseXA = FALSE;
  static BOOL bFirstCall = TRUE;


  /* this should only be called once */
  if (bFirstCall)
  {
    PROPGetAsBool ("DST_USE_XA_TRANSACTIONS", &bUseXA);
    bFirstCall = FALSE;
  }

  return bUseXA;
}

/*
* Function: DSTRollbackOnCommitFailure  ()
*
* Description: Returns true if the global property
* DST_ROLLBACK_ON_COMMIT_FAILURE is set to true.
*
* In:
*
* Out:
*
* In/Out:
*
* Global Properties:
* DST_ROLLBACK_ON_COMMIT_FAILURE
*
*/
BOOL DSTRollbackOnCommitFailure ()
{
  static BOOL bRollbackOnCommitFailure = FALSE;
  static BOOL bFirstCall = TRUE;


  /* this should only be called once */
  if (bFirstCall)
  {
    PROPGetAsBool ("DST_ROLLBACK_ON_COMMIT_FAILURE",
      &bRollbackOnCommitFailure);

    bFirstCall = FALSE;
  }

  return bRollbackOnCommitFailure;
}


/*
* Function: DSTUseSqlExecuteTxns ()
*
* Description: Returns true if the global property
* DST_USE_SQL_EXECUTE_TRANSACTIONS is set to true.
*
* In:
*
* Out:
*
* In/Out:
*
* Global Properties:
* DST_USE_SQL_EXECUTE_TRANSACTIONS
*
*/
BOOL DSTUseSqlExecuteTxns ()
{
  static BOOL bUseSqlExecuteTxns = FALSE;
  static BOOL bFirstCall = TRUE;


  /* this should only be called once */
  if (bFirstCall)
  {
    PROPGetAsBool ("DST_USE_SQL_EXECUTE_TRANSACTIONS",
      &bUseSqlExecuteTxns);

    bFirstCall = FALSE;
  }

  return bUseSqlExecuteTxns;
}




#ifdef DST_INCLUDE_COREXEL

/*
* Function: DSTCreateThread ()
*
* Description: Creates a new thread and starts execution
* at the specified function address.
*
* In: pStartFunc, pArgs
*
* Out: pThreadID
*
* In/Out:
*
*
*/

BOOL DSTCreateThread (DSTThreadStartFunc pStartFunc,
                      void* pArgs, sbThread* pThreadID)
{
  BOOL bStatus = TRUE;
  DSTRING mssg;
  int iErrNo = 0;

  DST_LOG_REPORT_ME ("DSTCreateThread");

  STRInit (&mssg, "");


  /* launch the thread */
  iErrNo = sbThreadCreate (pStartFunc, 0, pArgs, pThreadID);

  if (iErrNo != 0)
  {
    bStatus = FALSE;

    STRCopyCString (&mssg, "Thread failed to start. Error code: %d");
    STRInsertInt (&mssg, iErrNo);
    DST_LOG_ERROR (STRC (mssg));
  }
  else
  {
    STRCopyCString (&mssg, "Thread ID %u started successfully.");
    STRInsertUnsignedInt (&mssg, (UDWORD) *pThreadID);
    DST_LOG (STRC (mssg));
  }


  STRFree (&mssg);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}




/*
* Function: DSTJoinThread ()
*
* Description: Joins to an existing thread specified by the
* thread ID.
*
* In: threadID
*
* Out:
*
* In/Out:
*
*
*/
BOOL DSTJoinThread (sbThread threadID)
{
  BOOL bStatus = TRUE;
  DSTRING mssg;
  int iErrNo = 0;

  DST_LOG_REPORT_ME ("DSTJoinThread");


  STRInit (&mssg, "Joining thread ID %u ... ");
  STRInsertUnsignedInt (&mssg, (UDWORD) threadID);
  DST_LOG (STRC (mssg));


  /* join the thread */
  iErrNo = sbThreadJoin (threadID, NULL, NULL);


  if (iErrNo != 0)
  {
    bStatus = FALSE;

    STRCopyCString (&mssg, "Failed to join thread ID %u. Error code: %d");
    STRInsertUnsignedInt (&mssg, (UDWORD) threadID);
    STRInsertInt (&mssg, iErrNo);
    DST_LOG_ERROR (STRC (mssg));
  }
  else
  {
    STRCopyCString (&mssg, "Thread ID %u joined successfully.");
    STRInsertUnsignedInt (&mssg, (UDWORD) threadID);
    DST_LOG (STRC (mssg));
  }


  STRFree (&mssg);

  DST_LOG_UNREPORT_ME;
  return bStatus;
}

#endif /* DST_INCLUDE_COREXEL */


/*
* Function: DSTLock/Unlock ()
*
* Description: Provides a mutually exclusive lock among threads
* in a single process. The pointer to the CRITICAL_SECTION pointer 
* variable must be initialized to NULL before its first use in order
* to allocate the associated structure.
*
* In: 
*
* Out:
*
* In/Out: ppLock
*
*
*/
void DSTLock (CRITICAL_SECTION** ppLock)
{

  /* allocate the critical section structure */
  if (*ppLock == NULL)
  {
    *ppLock = (CRITICAL_SECTION*) DST_MALLOC (sizeof (CRITICAL_SECTION));

    if (*ppLock == NULL)
      return;

    /* initialize */
#ifdef DST_INCLUDE_COREXEL
    sbInitializeCriticalSection (*ppLock);
#endif
  }

  /* acquire the lock */
#ifdef DST_INCLUDE_COREXEL
  sbEnterCriticalSection (*ppLock);
#endif

}

void DSTUnlock (CRITICAL_SECTION* pLock)
{

  /* release the lock */
#ifdef DST_INCLUDE_COREXEL
  sbLeaveCriticalSection (pLock);
#endif

}




/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */


