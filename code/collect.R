# collect.R


# These procedures incrementally collect data for test runs defined in the STF
# TEST_RUN table.



# Top level procedure to run collections for all active test runs. If the
# collection procedure is successful then TRUE is returned.
collect <- function () {

  props <- getSTFProps ()

  loginfo ("[%s] Running collection procedures for all active test runs",
           "collect")

  # default return code
  rc <- TRUE



  tryCatch ({

    # acquire a connection to the STF database and get a list of active
    # test runs
    stf.conn <- getSTFConn ()
    test.runs <- getActiveTestRuns (stf.conn)

    
    # order by id - this is necessary with classic collection so
    # that the sample interval is approximately the same for every
    # collection
    test.runs <- arrange(test.runs, TEST_RUN_ID)
    
    
    
    loginfo ("[%s] Found %s active test run(s)",
             "collect", nrow (test.runs))

    if (nrow (test.runs) > 0) {

      for (i in 1:nrow (test.runs)) {

        # parallel collection?
        if (as.integer (props$stf.collect.parallel)) {

          # execute the collection procedure in parallel for this test run
          if (!collectTestRunParallel (stf.conn, test.runs [i,]))
            rc <- FALSE

        } else {

          # execute the collection procedure sequentially for this test run
          if (!collectTestRun (stf.conn, test.runs [i,]))
            rc <- FALSE

        }

      }
    }

  },

  error = function (e) {

    rc <<- FALSE
    logerror ("[%s] %s ", "collect", e [[1]])
  },

  finally = {

    # clean up
    try (dbRollback (stf.conn), silent = TRUE)
    try (dbDisconnect (stf.conn), silent = TRUE)
  })



  loginfo ("[%s] Done", "collect")

  return (rc)
}


# Perform collection tasks for a given (active) test run. If collect.logs is
# FALSE then remote instance log file collections will not execute. If
# collect.events is FALSE then new events will not be detected.
collectTestRun <- function (stf.conn, test.run,
                            collect.logs = TRUE,
                            collect.events = TRUE,
                            update.refreshed = TRUE) {

  
  stf.props <- getSTFProps ()
  
  loginfo ("[%s] Running collection procedures for TEST_RUN_ID = %s",
           "collectTestRun", test.run$TEST_RUN_ID)

  if (!collect.logs) {

    loginfo ("[%s] Remote log file collection will not occur",
             "collectTestRun", test.run$TEST_RUN_ID)
  }

  if (!collect.events) {

    loginfo ("[%s] Event detection will not occur",
             "collectTestRun", test.run$TEST_RUN_ID)
  }


  # default return code
  rc <- TRUE

  # collection stats
  collect.count <- 0
  fail.count <- 0


  tryCatch ({


    tryCatch ({

      # Acquire the target TimesTen connection required by the collection
      # procedures.
      tt.conn <- getTTCConn (test.run$DEPLOYMENT_NAME,
                             test.run$DB_USER,
                             test.run$DB_PWD)

    },

    error = function (e) {

      logerror ("[%s] Connection attempt failed for TEST_RUN_ID = %s",
                "collectTestRun",
                test.run$TEST_RUN_ID)

      # if the connection to the database fails then set the instance status
      # to Unknown
      deployment <- getDeployment(stf.conn,
                                  deployment.id = test.run$DEPLOYMENT_ID)

      setInstanceStatesUnknown (stf.conn, test.run)
      
      stop (e [[1]])
    })


    # if the test run deployment type is undefined then update it
    if (is.na (test.run$DEPLOYMENT_TYPE)) {

      # "CLASSIC" or "GRID" database?
      db.type <- getTTDbType (tt.conn)


      loginfo ("[%s] Setting DEPLOYMENT_TYPE = %s for TEST_RUN_ID = %s",
               "collectTestRun", db.type, test.run$TEST_RUN_ID)

      setTestRunDeploymentType (stf.conn, test.run$TEST_RUN_NAME, db.type)

    } else {

      db.type <- test.run$DEPLOYMENT_TYPE
    }

    
    
    # Configure TimesTen database statistics
    configureTTStatsCollect (stf.props, tt.conn)
    


    # run the collection procedures

    # NOTE: Each of these procedures may fail with exceptions which should be
    #       caught and handled here so that each collection procedure has a
    #       chance to execute.

    # This first group of collection procedures requires a connect to the
    # target store. The second group below, which collects file system records,
    # does not.

    # Collect instance states - this should occur first in case there are
    # dependencies in the remaining collectors.
    collect.count <- collect.count + 1

    tryCatch ({

      collectInstances (stf.conn, test.run, tt.conn)
    },

    error = function (e) {

      fail.count <<- fail.count + 1
      rc <<- FALSE
      logerror ("[%s] %s ", "collectTestRun", e [[1]])

      # clean up
      try (dbRollback (stf.conn), silent = TRUE)
    })


    # these collection procedures are specific to classic databases
    if (db.type == "CLASSIC") {

      # ------------------------------------------------------------------------
      # collect classic database metrics
      collect.count <- collect.count + 1

      tryCatch ({

        collectClsscDBMetrics (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })


      # ------------------------------------------------------------------------
      # collect classic generic metrics
      collect.count <- collect.count + 1

      tryCatch ({

        collectClsscGenericMetrics (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })



      # ------------------------------------------------------------------------
      # collect classic log hold data
      collect.count <- collect.count + 1

      tryCatch ({

        collectClsscLogHolds (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })


    } # end classic database specific collections



    # ------------------------------------------------------------------------
    # collect configurations
    collect.count <- collect.count + 1

    tryCatch ({

      collectConfigs (stf.conn, test.run, tt.conn)
    },

    error = function (e) {

      fail.count <<- fail.count + 1
      rc <<- FALSE
      logerror ("[%s] %s ", "collectTestRun", e [[1]])

      # clean up
      try (dbRollback (stf.conn), silent = TRUE)
    })



    # ------------------------------------------------------------------------
    # collect the state of current connections
    collect.count <- collect.count + 1

    tryCatch ({

      collectConns (stf.conn, test.run, tt.conn)
    },

    error = function (e) {

      fail.count <<- fail.count + 1
      rc <<- FALSE
      logerror ("[%s] %s ", "collectTestRun", e [[1]])

      # clean up
      try (dbRollback (stf.conn), silent = TRUE)
    })



    # these procedure are specific to grid
    if (db.type == "GRID") {



      # ------------------------------------------------------------------------
      # collect DB metrics
      collect.count <- collect.count + 1

      tryCatch ({

        collectDBMetrics (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })



      # ------------------------------------------------------------------------
      # collect log hold data
      collect.count <- collect.count + 1

      tryCatch ({

        collectLogHolds (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })



      # ------------------------------------------------------------------------
      # collect CPU metrics
      collect.count <- collect.count + 1

      tryCatch ({

        collectCpuMetrics (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })



      # ------------------------------------------------------------------------
      # collect memory metrics
      collect.count <- collect.count + 1

      tryCatch ({

        collectMemMetrics (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })



      # ------------------------------------------------------------------------
      # collect disk history
      collect.count <- collect.count + 1

      tryCatch ({

        collectDiskHist (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })


      # ------------------------------------------------------------------------
      # collect generic metrics
      collect.count <- collect.count + 1

      tryCatch ({

        collectGenericMetrics (stf.conn, test.run, tt.conn)
      },

      error = function (e) {

        fail.count <<- fail.count + 1
        rc <<- FALSE
        logerror ("[%s] %s ", "collectTestRun", e [[1]])

        # clean up
        try (dbRollback (stf.conn), silent = TRUE)
      })


    }






    # close the target TimesTen connection
    loginfo ("[%s] Closing target TimesTen connection for TEST_RUN_ID = %s",
             "collectTestRun", test.run$TEST_RUN_ID)

    try (dbRollback (tt.conn), silent = TRUE)
    try (dbDisconnect (tt.conn), silent = TRUE)


  },

  # catch the error when the target TimesTen connection fails to be acquired
  error = function (e) {

    rc <<- FALSE
    logerror ("[%s] %s ", "collectTestRun", e [[1]])
  })





  # Collect log files from the instance file systems. These collection
  # procedures do not require a TimesTen connection to the target system.
  if (collect.logs) {

    collect.count <- collect.count + 1

    tryCatch ({

      collectLogs (stf.conn, test.run)
    },

    error = function (e) {

      fail.count <<- fail.count + 1
      rc <<- FALSE
      logerror ("[%s] %s ", "collectTestRun", e [[1]])

      # clean up
      try (dbRollback (stf.conn), silent = TRUE)
    })

  }

  # Collect events for the latest test run data. This should execute as the last
  # collection procedure since it scans the latest test run data sets.
  if (collect.events) {

    collect.count <- collect.count + 1

    tryCatch ({

      collectTestRunEvents (stf.conn, test.run$TEST_RUN_ID)
    },

    error = function (e) {

      fail.count <<- fail.count + 1
      rc <<- FALSE
      logerror ("[%s] %s ", "collectTestRun", e [[1]])

      # clean up
      try (dbRollback (stf.conn), silent = TRUE)
    })

  }


  # results
  loginfo ("[%s] %s of %s collection procedures succeeded",
           "collectTestRun",
           collect.count - fail.count,
           collect.count)

  # if at least 1 collection procedure succeeded then update the refresh date
  if (update.refreshed &&
      (collect.count - fail.count) > 0) {

    # update the time of the latest collection
    setTestRunRefreshed (stf.conn, test.run$TEST_RUN_ID)
  }

  loginfo ("[%s] Done", "collectTestRun")

  return (rc)
}



# Perform collection tasks for a given (active) test run using parallel
# collection processes. This is not currently supported on Windows. If all 
# collection procedure succeed then TRUE is returned.
collectTestRunParallel <- function (stf.conn, test.run) {


  stf.props <- getSTFProps ()
  child.timeout <- as.integer (stf.props$stf.collect.parallel.timeout.secs)

  # default return code
  rc <- TRUE


  loginfo ("[%s] Running parallel collection procedures for TEST_RUN_ID = %s",
           "collectTestRunParallel", test.run$TEST_RUN_ID)



  # Note that all collection procedures must establish their own database
  # connections because mcparallel() uses fork().
  tryCatch ({

   
    # run the file system based collection procedures
    collect.logs.job <- mcparallel ({

      tryCatch ({

        # This avoids a logging module bug/exception when a forked process 
        # attempts to write to the xterm console.
        configureLogging (getSTFProps (), console = FALSE)

        # a new connection is required when forking
        conn <- getSTFConn ()

        collectLogs (conn, test.run)
      },

      error = function (e) {
        logerror ("[%s] %s ", "collectTestRunParallel", e [[1]])
        stop (e [[1]])
      },

      finally = {
        try (dbRollback (conn), silent = TRUE)
        try (dbDisconnect (conn), silent = TRUE)
      })

    },
    
    silent = FALSE,
    mc.interactive = TRUE,
    detached = FALSE)


    
    # run the SQL based collection procedures
    collect.sql.job <- mcparallel ({
      
      tryCatch ({
        
        # This avoids a logging module bug/exception when a forked process 
        # attempts to write to the xterm console.
        configureLogging (getSTFProps (), console = FALSE)
        
        # a new connection is required when forking        
        conn <- getSTFConn ()
        
        collectTestRun (conn, test.run,
                        collect.logs = FALSE,
                        collect.events = FALSE,
                        update.refreshed = FALSE)
      },
      
      error = function (e) {
        logerror ("[%s] %s ", "collectTestRunParallel", e [[1]])
        stop (e [[1]])
      },
      
      finally = {
        try (dbRollback (conn), silent = TRUE)
        try (dbDisconnect (conn), silent = TRUE)
      })
      
    },
    
    silent = FALSE,
    mc.interactive = TRUE,
    detached = FALSE)
    
    
    
    

    # wait for the SQL collection job to complete
    loginfo (
      "[%s] Waiting for collectTestRun (), PID = %s, timeout = %s seconds ...",
      "collectTestRunParallel",
      collect.sql.job$pid,
      child.timeout)

    collect.sql.job.result <- mccollect (
      collect.sql.job,

      wait = ifelse (child.timeout == 0, TRUE, FALSE),
      timeout = child.timeout)
    
    
    # process results
    if (!mccollectResults (collect.sql.job.result,
                           throw.exception = FALSE)) {
      
      logerror (
        "[%s] An exception occurred during parallel SQL collection",
        "collectTestRunParallel")
      
      rc <- FALSE      
    }
    
    # timeout then kill
    if (is.null (collect.sql.job.result)) {

      logerror (
        "[%s] The collectTestRun() child process PID %s timed out",
        "collectTestRunParallel",
        collect.sql.job$pid)

      killProcess (collect.sql.job$pid)

      rc <- FALSE
    }
    
    
    
    
    # wait for the instance log collection job to complete
    loginfo ("[%s] Waiting for collectLogs (), PID = %s ...",
             "collectTestRunParallel",
             collect.logs.job$pid)
    

    collect.logs.job.result <- mccollect (
      collect.logs.job,

      wait = ifelse (child.timeout == 0, TRUE, FALSE),
      timeout = child.timeout)

    
    # process results
    if (!mccollectResults (collect.logs.job.result,
                           throw.exception = FALSE)) {
      
      logerror (
        "[%s] An exception occurred during parallel instance log collection",
        "collectTestRunParallel")
      
      rc <- FALSE      
    }
    
    
    # timeout then kill
    if (is.null (collect.logs.job.result)) {

      logerror (
        "[%s] The collectLogs() child process PID %s timed out",
        "collectTestRunParallel",
         collect.logs.job$pid)

      killProcess (collect.logs.job$pid)

      rc <- FALSE
    }


    
    # detect new events after collection has completed
    collectTestRunEvents (stf.conn, test.run$TEST_RUN_ID)

  },

  error = function (e) {
    rc <<- FALSE

    logerror ("[%s] Parallel collection procedure failed: %s",
              "collectTestRunParallel", e [[1]])

    try (dbRollback (stf.conn))
  },

  finally = {

    # update the time of the latest collection
    setTestRunRefreshed (stf.conn, test.run$TEST_RUN_ID)
  })

  
  
  if (rc) {
    
    loginfo ("[%s] Collection procedures complete",
             "collectTestRunParallel")
  } else {
    
    logerror ("[%s] Some collection procedure(s) failed",
              "collectTestRunParallel")    
  }
  
  return (rc)
}




# Update the INSTANCE_HISTORY table with the latest instance states
collectInstances <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting instance info for TEST_RUN_ID = %s",
           "collectInstances", test.run$TEST_RUN_ID)


  # refresh the deployment for this test run
  rc <- refreshDeployment (test.run$DEPLOYMENT_ID)


  if (!rc) {

    # this procedure throws an exception on error
    stop ("refreshDeployment() failed")

  } else {

    # If the deployment refresh was successful then the DB_INSTANCE table
    # will contain the latest info. Merge this info into the test run's
    # INSTANCE_HIST table.


    # retrieve the latest instance info from the DB_INSTANCE table
    db.instances <- getInstances (stf.conn, test.run$DEPLOYMENT_ID)

    # drop columns that should not be compared
    db.instances <- db.instances [,
      !(names (db.instances) %in% c ("DEPLOYMENT_ID"))]


    if (nrow (db.instances) > 0) {

      # Is anything different about the current data set versus the previous
      # data set stored in the INSTANCE_HIST table for this test run? If there
      # isn't any difference then don't insert.
      db.instances.prev <- getTestRunInstances (stf.conn, test.run$TEST_RUN_ID)

      # drop columns that should not be compared
      db.instances.prev <- db.instances.prev [,
        !(names (db.instances.prev) %in% c ("TEST_RUN_ID", "COLLECTED_AT"))]

      prev.row.count <- nrow (db.instances.prev)
      diff.row.count <- nrow (dplyr::union (db.instances, db.instances.prev))

      if (prev.row.count == diff.row.count) {

        loginfo ("[%s] No instance state changes detected",
                 "collectInstances")

      } else {

        # prepare the data set for insert - the COLLECTED_AT timestamp must be
        # the same for every instance record (of this particular sample) in
        # order to support the history function of this table
        db.instances$TEST_RUN_ID <- test.run$TEST_RUN_ID
        db.instances$COLLECTED_AT <- Sys.time ()

        db.instances <- select (db.instances,
                                TEST_RUN_ID,
                                COLLECTED_AT,
                                DB_HOST,
                                DB_PORT,
                                DB_NAME,
                                DB_STATE,
                                INSTANCE_DIR,
                                INSTANCE_NAME,
                                ELEMENTID,
                                REPSETID,
                                DATASPACEID,
                                DATASTORE)

        # write the new info
        dbWriteTable (stf.conn, "INSTANCE_HIST", db.instances, append = TRUE)
        dbCommit (stf.conn)


        # update the ODBC configuration files so that the alternate server list
        # is updated based on the instance changes
        createODBCConfig ()

        loginfo ("[%s] New instance info records committed",
                 "collectInstances")
      }
    }

  }

}


collectConfigs <- function (stf.conn, test.run, tt.conn) {



  loginfo ("[%s] Collecting configuration info for TEST_RUN_ID = %s",
           "collectConfigs", test.run$TEST_RUN_ID)


  # Retrieve the current local database configuration. Note that
  # TATP.TTSCALEOUTCONFIGGETINTERNAL may not exist in early 18.1 releases. So
  # if this fails then try again without this procededure.


  tryCatch ({

    configs <- dbGetQuery (tt.conn,
                           "
      SELECT
        *
      FROM (
        SELECT
          'ttConfiguration' AS SOURCE,
          PARAMNAME AS PARAM,
          PARAMVALUE AS VALUE
        FROM ttConfigurationInternal

        UNION

        SELECT
          'ttDBConfig' AS SOURCE,
          PARAM AS PARAM,
          VALUE AS VALUE
        FROM ttDBConfigGetInternal

        UNION

        SELECT
          'ttScaleoutConfig' AS SOURCE,
          PARAM AS PARAM,
          VALUE AS VALUE
        FROM ttScaleoutConfigGetInternal

        UNION

        SELECT
          'ttStatsConfig' AS SOURCE,
          PARAM AS PARAM,
          VALUE AS VALUE
        FROM V$STATS_CONFIG)

      WHERE VALUE IS NOT NULL
      ORDER BY 1,2,3
    ")

  },

  error = function (e) {
    logerror ("[%s] %s ", "collectConfigs", e [[1]])
    logwarn ("[%s] %s ", "collectConfigs",
             "Executing again without ttScaleoutConfigGetInternal() ...")

    # try again without TTSCALEOUTCONFIGGETINTERNAL
    configs <<- dbGetQuery (tt.conn,
      "
      SELECT
        *
      FROM (
        SELECT
          'ttConfiguration' AS SOURCE,
          PARAMNAME AS PARAM,
          PARAMVALUE AS VALUE
        FROM ttConfigurationInternal

        UNION

        SELECT
          'ttDBConfig' AS SOURCE,
          PARAM AS PARAM,
          VALUE AS VALUE
        FROM ttDBConfigGetInternal

        UNION

        SELECT
          'ttStatsConfig' AS SOURCE,
          PARAM AS PARAM,
          VALUE AS VALUE
        FROM V$STATS_CONFIG)

      WHERE VALUE IS NOT NULL
      ORDER BY 1,2,3      ")

  })



  if (nrow (configs) > 0) {

    # Is anything different about the current data set versus the previous
    # data set?.
    configs.prev <- getTestRunDbConfigs (stf.conn, test.run$TEST_RUN_ID)

    # drop columns that should not be compared
    configs.prev <- configs.prev [,
      !(names (configs.prev) %in% c ("TEST_RUN_ID"))]

    prev.row.count <- nrow (configs.prev)
    diff.row.count <- nrow (dplyr::union (configs, configs.prev))

    if (prev.row.count == diff.row.count) {

      loginfo ("[%s] No configuration changes detected",
               "collectConfigs")

    } else {

      loginfo ("[%s] Collected %s new configuration records",
               "collectConfigs", nrow (configs))

      # delete the previous configuration records
      dbGetQuery (stf.conn,
                   sprintf ("DELETE FROM DB_CONFIG WHERE TEST_RUN_ID = '%s'",
                            test.run$TEST_RUN_ID))

      # prepare the latest data set for insert
      configs$TEST_RUN_ID <- test.run$TEST_RUN_ID

      configs <- select (configs,
                         TEST_RUN_ID,
                         SOURCE,
                         PARAM,
                         VALUE)

      # write the new info
      dbWriteTable (stf.conn, "DB_CONFIG", configs, append = TRUE)
      dbCommit (stf.conn)

      loginfo ("[%s] New configuration records committed",
               "collectConfigs")
    }
  }


}



# Update the CONN_HISTORY table with the latest instance connections
collectConns <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting connection info for TEST_RUN_ID = %s",
           "collectConns", test.run$TEST_RUN_ID)


  # retrieve connection info
  db.conns <- dbGetQuery (tt.conn,
    "
    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID,
      PID,
      CONNID,
      TRIM (CONTYPE) AS CONTYPE,
      TRIM (CONNECTION_NAME) AS CONNECTION_NAME

    FROM V$DATASTORE_STATUS

    WHERE TRIM (CONTYPE) IN ('server', 'application',
      'cache agent', 'replication') AND

      DATASTORE IN (
        SELECT

          -- This removes a trailing '.<elementid>' for data stores that are
          -- located on remote nodes.
          SUBSTR (PARAMVALUE, 1,
            CASE
              WHEN INSTR (PARAMVALUE, '.') = 0 THEN LENGTH (PARAMVALUE)
              ELSE INSTR (PARAMVALUE, '.') - 1
            END) PARAMVALUE

        FROM V$CONFIGURATION
        WHERE PARAMNAME = 'DataStore')
    ")

  loginfo ("[%s] Collected %s new connection records",
           "collectConns", nrow (db.conns))



  if (nrow (db.conns) > 0) {


    # Is anything different about the current data set versus the previous
    # data set stored in the CONN_HIST table for this test run? If there
    # isn't any difference then don't insert.
    db.conns.prev <- getTestRunConns (stf.conn, test.run$TEST_RUN_ID)

    # drop columns that should not be compared
    db.conns.prev <- db.conns.prev [,
      !(names (db.conns.prev) %in% c ("TEST_RUN_ID", "COLLECTED_AT"))]

    prev.row.count <- nrow (db.conns.prev)
    diff.row.count <- nrow (dplyr::union (db.conns, db.conns.prev))

    if (prev.row.count == diff.row.count) {

      loginfo ("[%s] No connection state changes detected",
               "collectConns")

    } else {

      # prepare the data set for insert - the COLLECTED_AT timestamp must be
      # the same for every connection record (of this particular sample) in
      # order to support the history function of this table
      db.conns$TEST_RUN_ID <- test.run$TEST_RUN_ID
      db.conns$COLLECTED_AT <- Sys.time ()

      db.conns <- select (db.conns,
                          TEST_RUN_ID,
                          COLLECTED_AT,
                          ELEMENTID,
                          PID,
                          CONNID,
                          CONTYPE,
                          CONNECTION_NAME)

      # write the new info
      dbWriteTable (stf.conn, "CONN_HIST", db.conns, append = TRUE)
      dbCommit (stf.conn)

      loginfo ("[%s] New connection records committed",
               "collectConns")
    }

  }

}


# Update the TTSTATS_ELEMENT_METRICS table with database metrics.
collectDBMetrics <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting database metrics for TEST_RUN_ID = %s",
           "collectDBMetrics", test.run$TEST_RUN_ID)


  # get the last time element metrics were collected for this test run
  last.collect.date <- getLastCollectDate (stf.conn,
                                           test.run$TEST_RUN_ID,
                                           "TTSTATS_ELEMENT_METRICS")

  loginfo ("[%s] Last database metrics collection date = %s",
           "collectDBMetrics", last.collect.date$COLLECTED_AT)


  query <-
    "
    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID,

      ID AS COLLECT_ID,
      COLLECTED_AT,

      TRIM (METRIC_NAME) AS METRIC_NAME,
      METRIC_VALUE

    FROM GV$TTSTATS_ELEMENT_METRICS

    WHERE TRIM (METRIC_NAME) IN
      (
      'db.table.rows_read',
      'db.table.rows_updated',
      'db.table.rows_inserted',
      'db.table.rows_deleted',

      'stmt.executes.count',
      'stmt.executes.selects',
      'stmt.executes.inserts',
      'stmt.executes.updates',
      'stmt.executes.deletes',

      'stmt.prepares.count',

      'stmt.global.executes.count',

      'lock.locks_granted.wait' ,
      'lock.timeouts',
      'lock.deadlocks',

      'lock.locks_granted.immediate',
      'lock.locks_acquired.table_scans',
      'lock.locks_acquired.dml',
      
      'txn.rollbacks',
      'txn.commits.durable',
      'txn.commits.nondurable',

      'txn.forget.used',
      'txn.forget.alloc',
      'txn.initiated.tm.count',
      'txn.participated.remote.count',
      'txn.epoch',
      'txn.voted.no',
      'txn.commit.dml.on.success.bundle',

      'connections.established.count',
      'connections.disconnected',
      'connections.established.direct',
      'connections.established.client_server',

      'channel.invalidations',
      'channel.recv.messages',
      'channel.send.messages',

      'tunnel.enqueued.requests',
      'tunnel.false.hangs',
      'tunnel.hangs',
      'tunnel.processed.requests',
      'tunnel.timedout.requests',

      'ckpt.completed',
      'ckpt.completed.fuzzy',

      'log.file.reads',
      'log.file.writes',
      'log.recovery.bytes.read',

      'cg.autorefresh.cycles.completed',
      'cg.autorefresh.cycles.failed',
      'cg.autorefresh.deletes.rows',
      'cg.autorefresh.inserts.rows',
      'cg.autorefresh.updates.rows',
      
      'zzinternal.db.index.range.btree.split',
      'zzinternal.db.index.range.btree.merge',
      'zzinternal.db.index.range.btree.balance',
      'zzinternal.db.index.range.btree.restart',
      'zzinternal.db.index.range.btree.right_most_insert',
      'zzinternal.db.index.range.btree.lock_wait',
      'zzinternal.db.index.range.btree.node_allocated',
      'zzinternal.db.index.range.btree.node_allocated_freelist',
      'zzinternal.db.index.range.btree.node_unlinked',
      
      'db.index.range.deletes',
      'db.index.range.inserts.count',
      'db.index.range.inserts.recovery_rebuild',
      'db.index.range.rows_fetched.count',
      'db.index.range.rows_fetched.repl',
      'db.index.range.scans.count',
      'db.index.range.scans.repl',

      'db.index.temporary.scans.count',  
      'db.index.temporary.scans.repl',
      'db.index.temporary.rows_fetched.count',
      'db.index.temporary.rows_fetched.repl',
            
      'zzinternal.db.index.range.gc.freed',
      'zzinternal.db.index.range.gc.signalled',

      'zzinternal.stmt.executes.updates.inplace',
      'zzinternal.stmt.executes.updates.versioned',
      'zzinternal.stmt.executes.updates.versioned.disabled_ipu',
      'zzinternal.stmt.executes.updates.versioned.multi_partitions',
      'zzinternal.stmt.executes.updates.versioned.index',
      
      'defrag.fragmented.pages',
      'defrag.rows.moved',
      
      'zzinternal.log.strand_switches.strand_full',
      'zzinternal.log.flusher_deadlock_retry',
      'zzinternal.log.failed.waiting.lwn.published',
      'zzinternal.log.system.new.lwn',
      'zzinternal.log.system.new.lwn.missed',
      'zzinternal.log.insert.new.lwn',
      'zzinternal.log.insert.new.lwn.missed',
      
      'txn.autonomous.begin',
      'txn.autonomous.end',
      'txn.autonomous.active',
      'txn.autonomous.uncaught_exceptions',
      'txn.autonomous.internal_errors',
      'txn.autonomous.max_depth',
      
      'db.tempblk.alloc',
      'db.tempblk.free',
      'db.tempblk.unlink',
      'db.tempblk.compact'

      )
    "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # TimesTen DATE format: YYYY-MM-DD HH:MI:SS
    query <- paste0 (query, "\n AND COLLECTED_AT > ",
                     getPOSIXctAsSQL (last.collect.date$COLLECTED_AT))
  }




  # query GV$TTSTATS_ELEMENT_METRICS for specific metrics
  db.metrics <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new database metrics records",
           "collectDBMetrics", nrow (db.metrics))


  if (nrow (db.metrics) > 0) {

    # insert the new records into TTSTATS_ELEMENT_METRICS
    db.metrics$TEST_RUN_ID <- test.run$TEST_RUN_ID
    db.metrics <- select (db.metrics,
                          TEST_RUN_ID,
                          ELEMENTID,
                          COLLECT_ID,
                          COLLECTED_AT,
                          METRIC_NAME,
                          METRIC_VALUE)


    dbWriteTable (stf.conn, "TTSTATS_ELEMENT_METRICS",
                  db.metrics, append = TRUE)
    dbCommit (stf.conn)

    loginfo ("[%s] New database metrics records committed", "collectDBMetrics")

  }
}




# Update the TTSTATS_CPU_HIST table with CPU metrics.
collectCpuMetrics <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting OS metrics for TEST_RUN_ID = %s",
           "collectCpuMetrics", test.run$TEST_RUN_ID)


  # get the last time CPU metrics were collected for this test run
  last.collect.date <- getLastCollectDate (stf.conn,
                                           test.run$TEST_RUN_ID,
                                           "TTSTATS_CPU_HIST")

  loginfo ("[%s] Last CPU history collection date = %s",
           "collectCpuMetrics", last.collect.date$COLLECTED_AT)


  query <-
    "
    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID,
      ID AS COLLECT_ID,
      COLLECTED_AT,
      CPU_UTIL

    FROM GV$TTSTATS_CPU_HIST CPU
    "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # TimesTen DATE format: YYYY-MM-DD HH:MI:SS
    query <- paste0 (query, "\n WHERE COLLECTED_AT > ",
                     getPOSIXctAsSQL (last.collect.date$COLLECTED_AT))
  }




  # query GV$TTSTATS_CPU_HIST for specific metrics
  os.metrics <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new CPU records",
           "collectCpuMetrics", nrow (os.metrics))




  if (nrow (os.metrics) > 0) {

    # insert the new records into TTSTATS_CPU_HIST
    os.metrics$TEST_RUN_ID <- test.run$TEST_RUN_ID
    os.metrics <- select (os.metrics,
                          TEST_RUN_ID,
                          ELEMENTID,
                          COLLECT_ID,
                          COLLECTED_AT,
                          CPU_UTIL)


    dbWriteTable (stf.conn, "TTSTATS_CPU_HIST", os.metrics, append = TRUE)
    dbCommit (stf.conn)

    loginfo ("[%s] New CPU records committed", "collectCpuMetrics")
  }
}



# Update the TTSTATS_VMEM_HIST table with OS memory metrics.
collectMemMetrics <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting OS memory metrics for TEST_RUN_ID = %s",
           "collectMemMetrics", test.run$TEST_RUN_ID)


  # get the last time CPU metrics were collected for this test run
  last.collect.date <- getLastCollectDate (stf.conn,
                                           test.run$TEST_RUN_ID,
                                           "TTSTATS_VMEM_HIST")

  loginfo ("[%s] Last OS memory history collection date = %s",
           "collectMemMetrics", last.collect.date$COLLECTED_AT)


  query <-
    "
    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID,
      ID AS COLLECT_ID,
      COLLECTED_AT,
      PER_MEM_AVAIL

    FROM GV$TTSTATS_VMEM_HIST VMEM
    "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # TimesTen DATE format: YYYY-MM-DD HH:MI:SS
    query <- paste0 (query, "\n WHERE COLLECTED_AT > ",
                     getPOSIXctAsSQL (last.collect.date$COLLECTED_AT))
  }




  # query GV$TTSTATS_VMEM_HIST for specific metrics
  mem.metrics <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new OS memory records",
           "collectMemMetrics", nrow (mem.metrics))




  if (nrow (mem.metrics) > 0) {

    # insert the new records into TTSTATS_VMEM_HIST
    mem.metrics$TEST_RUN_ID <- test.run$TEST_RUN_ID
    mem.metrics <- select (mem.metrics,
                          TEST_RUN_ID,
                          ELEMENTID,
                          COLLECT_ID,
                          COLLECTED_AT,
                          PER_MEM_AVAIL)


    dbWriteTable (stf.conn, "TTSTATS_VMEM_HIST", mem.metrics, append = TRUE)
    dbCommit (stf.conn)

    loginfo ("[%s] New OS memory records committed", "collectMemMetrics")
  }
}




# Update the TTSTATS_GENERIC_HIST table with various metrics.
collectGenericMetrics <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting generic metrics for TEST_RUN_ID = %s",
           "collectGenericMetrics", test.run$TEST_RUN_ID)


  # get the last time CPU metrics were collected for this test run
  last.collect.date <- getLastCollectDate (stf.conn,
                                           test.run$TEST_RUN_ID,
                                           "TTSTATS_GENERIC_HIST")

  loginfo ("[%s] Last generic history collection date = %s",
           "collectGenericMetrics", last.collect.date$COLLECTED_AT)


  query <-
    "
    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID,
      ID AS COLLECT_ID,
      COLLECTED_AT,
      NAME1 || ' ' || NAME2 || ' ' || NAME3 AS METRIC_NAME,
      INT_VALUE AS METRIC_VALUE

    FROM GV$TTSTATS_GENERIC_HIST

    WHERE NAME3 NOT IN ('high_water', 'efficiency')
    "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # TimesTen DATE format: YYYY-MM-DD HH:MI:SS
    query <- paste0 (query, "\n AND COLLECTED_AT > ",
                     getPOSIXctAsSQL (last.collect.date$COLLECTED_AT))
  }




  # query GV$TTSTATS_GENERIC_HIST for specific metrics
  gen.metrics <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new generic records",
           "collectGenericMetrics", nrow (gen.metrics))




  if (nrow (gen.metrics) > 0) {

    # insert the new records into TTSTATS_GENERIC_HIST
    gen.metrics$TEST_RUN_ID <- test.run$TEST_RUN_ID
    gen.metrics <- select (gen.metrics,
                           TEST_RUN_ID,
                           ELEMENTID,
                           COLLECT_ID,
                           COLLECTED_AT,
                           METRIC_NAME,
                           METRIC_VALUE)


    dbWriteTable (stf.conn, "TTSTATS_GENERIC_HIST", gen.metrics, append = TRUE)
    dbCommit (stf.conn)

    loginfo ("[%s] New generic records committed", "collectGenericMetrics")
  }
}




# Update the TTSTATS_LOGHOLD_HIST table with transaction log info.
collectLogHolds <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting log hold metrics for TEST_RUN_ID = %s",
           "collectLogHolds", test.run$TEST_RUN_ID)


  # get the last time log holds were collected for this test run
  # and exclude GV$BOOKMARK log hold records - where COLLECT_ID IS NOT NULL
  last.collect.date <- dbGetQueryWithTZ (
    stf.conn, sprintf (
      "SELECT
        MAX (COLLECTED_AT) AS COLLECTED_AT
      FROM TTSTATS_LOGHOLD_HIST
      WHERE TEST_RUN_ID = %s AND
        COLLECT_ID IS NOT NULL", test.run$TEST_RUN_ID))

  loginfo ("[%s] Last log hold history collection date = %s",
           "collectLogHolds", last.collect.date$COLLECTED_AT)


  query <-
  "
  WITH LOG_HIST AS (

      SELECT

        HOLD_LFN,
        HOLD_LFO,

        DESCRIPTION,
        LOG_TYPE,
        COLLECTED_AT,
        ELEMENTID,

        ID AS COLLECT_ID

      FROM GV$TTSTATS_LOGHOLD_HIST

        -- date predicate
        %s
    ),

    LOG_CURRENT AS (

      SELECT

        WRITELFN AS HOLD_LFN,
        WRITELFO AS HOLD_LFO,

        'Log Write' AS DESCRIPTION,
        'W' AS LOG_TYPE,

        SYSDATE COLLECTED_AT,
        ELEMENTID,

        /* NULL indicates that these records are not from */
        /* the GV$TTSTATS_LOGHOLD_HIST table. */
        NULL AS COLLECT_ID

      FROM GV$BOOKMARK
  
      UNION
  
      SELECT
  
        FORCELFN AS HOLD_LFN,
        FORCELFO AS HOLD_LFO,
  
        'Log Force' AS DESCRIPTION,
        'F' AS LOG_TYPE,
  
        SYSDATE COLLECTED_AT,
        ELEMENTID,
  
        /* NULL indicates that these records are not from */
        /* the GV$TTSTATS_LOGHOLD_HIST table. */
        NULL AS COLLECT_ID
  
      FROM GV$BOOKMARK
    )

    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID AS ELEMENTID,
      COLLECT_ID AS COLLECT_ID,
      COLLECTED_AT AS COLLECTED_AT,

      HOLD_LFN AS HOLD_LFN,
      HOLD_LFO AS HOLD_LFO,
      LOG_TYPE AS LOG_TYPE,

      DESCRIPTION AS DESCRIPTION

    FROM (
      SELECT * FROM LOG_HIST
      UNION
      SELECT * FROM LOG_CURRENT)

  "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # TimesTen DATE format: YYYY-MM-DD HH:MI:SS
    query <- sprintf (query, paste0 ("WHERE COLLECTED_AT > ",
                      getPOSIXctAsSQL (last.collect.date$COLLECTED_AT)))
  } else {

    # no date constraint
    query <- sprintf (query, "")
  }




  # query GV$TTSTATS_LOGHOLD_HIST for specific metrics
  log.metrics <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new log hold records",
           "collectLogHolds", nrow (log.metrics))




  if (nrow (log.metrics) > 0) {

    # insert the new records into TTSTATS_LOGHOLD_HIST
    log.metrics$TEST_RUN_ID <- test.run$TEST_RUN_ID
    log.metrics <- select (log.metrics,
                           TEST_RUN_ID,
                           ELEMENTID,
                           COLLECT_ID,
                           COLLECTED_AT,
                           HOLD_LFN,
                           HOLD_LFO,
                           LOG_TYPE,
                           DESCRIPTION)


    dbWriteTable (stf.conn, "TTSTATS_LOGHOLD_HIST", log.metrics,
                  append = TRUE)

    dbCommit (stf.conn)

    loginfo ("[%s] New log hold records committed",
             "collectLogHolds")
  }
}




# Update the TTSTATS_DISK_HIST table with disk metrics.
collectDiskHist <- function (stf.conn, test.run, tt.conn) {


  loginfo ("[%s] Collecting disk metrics for TEST_RUN_ID = %s",
           "collectDiskHist", test.run$TEST_RUN_ID)


  # get the last time the disk history was collected for this test run
  last.collect.date <- getLastCollectDate (stf.conn,
                                           test.run$TEST_RUN_ID,
                                           "TTSTATS_DISK_HIST")

  loginfo ("[%s] Last disk history collection date = %s",
           "collectDiskHist", last.collect.date$COLLECTED_AT)


  query <-
    "
    SELECT /*+ TT_PartialResult (1) */

      ELEMENTID,
      ID AS COLLECT_ID,
      COLLECTED_AT,
      PERCENT_USED,
      IO_MB_RATE

    FROM GV$TTSTATS_DISK_HIST DISK
    "

  # when a previous collection exists constrain the current query based on the
  # last collection time
  if (!is.na (last.collect.date$COLLECTED_AT)) {

    # TimesTen DATE format: YYYY-MM-DD HH:MI:SS
    query <- paste0 (query, "\n WHERE COLLECTED_AT > ",
                     getPOSIXctAsSQL (last.collect.date$COLLECTED_AT))
  }




  # query GV$TTSTATS_DISK_HIST for specific metrics
  disk.hist <- dbGetQuery (tt.conn, query)


  loginfo ("[%s] Collected %s new disk history records",
           "collectDiskHist", nrow (disk.hist))




  if (nrow (disk.hist) > 0) {

    # insert the new records into TTSTATS_VMEM_HIST
    disk.hist$TEST_RUN_ID <- test.run$TEST_RUN_ID
    disk.hist <- select (disk.hist,
                         TEST_RUN_ID,
                         ELEMENTID,
                         COLLECT_ID,
                         COLLECTED_AT,
                         PERCENT_USED,
                         IO_MB_RATE)


    dbWriteTable (stf.conn, "TTSTATS_DISK_HIST", disk.hist, append = TRUE)
    dbCommit (stf.conn)

    loginfo ("[%s] New disk history records committed", "collectDiskHist")
  }
}




# This is a utility function that process results from the mccollect function
# used to retrieve results from asynchronously executed code via the mcparallel
# function. The function takes the return value from mcccollect (results) and 
# logs all return values. If a job return value indicates an exception then this
# function will return FALSE, otherwise TRUE. If throw.exception is TRUE then
# the first exception detected in the results will be thrown.
mccollectResults <- function (results, 
                              throw.exception = FALSE) {
  
  rc <- TRUE
  first.exception <- NA
  
  
  # no results?
  if (is.null (results)) {

    logwarn ("[%s] No mccollect results",
             "mccollectResults")

    return (rc)
  }


  # iterate through the jobs
  loginfo ("[%s] Processing %s mccollect result(s) ...", 
           "mccollectResults", length (results))
  
  
  
  for (i in 1:length (results)) {
    
    name <- names (results) [i]
    result <- results [[i]]
    
    
    # did an exception occur?
    if (class (result) == "try-error") {
      
      logerror ("[%s] Job Name/PID: %s, Exception: %s", 
                "mccollectResults", 
                name, result [[1]])  
      
      # save the first exception
      if (is.na (first.exception)) {
        first.exception <- result
      }
      
      rc <- FALSE
      
    } else {
      
      loginfo ("[%s] Job Name/PID: %s, Result: %s", 
               "mccollectResults", 
               name, 
               ifelse (is.null (result), "NULL" , result))      
    }
    
  } # end result loop
  
  
  
  # throw an exception?
  if (throw.exception && !is.na (first.exception)) {
    stop (first.exception)
  }
  
  
  
  return (rc)
}


  
  

