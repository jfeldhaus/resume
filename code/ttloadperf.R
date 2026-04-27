
# This script will execute and time ttLoadFromOracle and LOAD CACHE group
# operations using the TATP benchmark SUBSCRIBERS table. Various parallel
# load settings are used to determine the optimal configuration.


Sys.setenv(TZ = "America/Los_Angeles")
Sys.setenv(ORA_SDTZ = "America/Los_Angeles")


library ("ggplot2")
library ("stringr")
library ("dplyr")
library ("tidyr")
library ("lubridate")
library ("ROracle")
library ("logging")
library ("tictoc")


# Return a TimesTen client connection based on a client/server DSN name.
getTTCConn <- function (dsn, usr, pwd) {

  dbname <- paste0 ("localhost/", dsn, ":timesten_client")

  loginfo ("Acquiring TimesTen connection to %s ",
           dbname)


  drv <- dbDriver ("Oracle")

  ttdb <- dbConnect (drv,
                     username = usr,
                     password = pwd,
                     dbname = dbname)
  return (ttdb)
}



# This procedure will execute the load test configuration and return a result
# data frame.
execLoads <- function (load.config) {


  loginfo ("Executing loads ...")
  print (load.config)


  # add result columns to the load configuration
  load.results <- load.config

  load.results$LOAD_FROM_ORA_STMT <- NA
  load.results$LOAD_FROM_ORA_EXEC_SECS <- NA
  load.results$LOAD_FROM_ORA_EXEC_TIME <- as.POSIXct(NA)
  load.results$LOAD_FROM_ORA_ROW_COUNT <- NA
  
  load.results$LOAD_CACHE_STMT <- NA
  load.results$LOAD_CACHE_EXEC_SECS <- NA
  load.results$LOAD_CACHE_EXEC_TIME <- as.POSIXct(NA)
  load.results$LOAD_CACHE_ROW_COUNT <- NA






  # LOAD CACHE loop
  for (rownum in 1:nrow (load.config)) {


    loginfo (sprintf ("Initializing LOAD CACHE iteration #%s ...",
                      rownum))
    this.load <- load.config [rownum,]


    tt.conn <- getTTCConn (this.load$DSN [rownum], 
                           this.load$UID [rownum], 
                           this.load$PWD [rownum])
    
    loginfo ("Unloading cache and check pointing ...")
    dbSendQuery (tt.conn,
                 sprintf ("UNLOAD CACHE GROUP %s COMMIT EVERY %s ROWS ",
                          this.load$LOAD_CACHE_GROUP [rownum],
                          this.load$COMMIT_ROWS [rownum]))

    dbCommit (tt.conn)


    # These checkpoints may fail due to TT0625 when another background
    # checkpoint is in progress.
    try ({

      dbSendQuery (tt.conn, "Call ttCkpt")
      dbSendQuery (tt.conn, "Call ttCkpt")},

      silent = TRUE)




    config.stmt <- sprintf ("CALL ttDbConfig ('_ParallelLoadSamplingSize', %s)",
                            this.load$SAMPLING_SIZE [rownum])

    loginfo (config.stmt)
    dbSendQuery (tt.conn, config.stmt)





    # build the LOAD CACHE statement
    load.options <- paste0 (

      sprintf ("COMMIT EVERY %s ROWS PARALLEL %s READERS %s ",
               this.load$COMMIT_ROWS [rownum],
               this.load$FETCHERS [rownum] + this.load$INSERTERS [rownum],
               this.load$FETCHERS [rownum]),

      sprintf ("_tt_bulkFetch 4096 _tt_bulkInsert %s", 
               this.load$COMMIT_ROWS [rownum]))

    
    
    load.cache.stmt <- 
      sprintf ("LOAD CACHE GROUP %s WHERE ROWNUM <= %s %s",
               this.load$LOAD_CACHE_GROUP [rownum],
               as.integer (this.load$TABLE_LOAD_ROW_LIMIT [rownum] / 
                             this.load$FETCHERS [rownum]),
               load.options)
    
    
    # execute the LOAD CACHE statement
    loginfo (load.cache.stmt)
    load.results$LOAD_CACHE_STMT [rownum] <- load.cache.stmt
    load.results$LOAD_CACHE_EXEC_TIME [rownum] <- Sys.time()

    tic ()
    result <- dbGetQuery(tt.conn, load.cache.stmt)
    timing <- toc (quiet = FALSE)
    loginfo (timing)

    dbCommit (tt.conn)


    # update the load.result
    load.results$LOAD_CACHE_EXEC_SECS [rownum] <- timing$toc - timing$tic
    loginfo ("Elapsed minutes: %s",
             load.results$LOAD_CACHE_EXEC_SECS [rownum] / 60)


    # report the row load count
    rows.loaded <- dbGetQuery(tt.conn,
                              sprintf ("SELECT COUNT (*) FROM %s",
                                       this.load$LOAD_CACHE_TABLE [rownum]))
    
    load.results$LOAD_FROM_CACHE_ROW_COUNT [rownum] <- rows.loaded
    loginfo ("Total rows loaded: %s", rows.loaded)

    saveRDS (load.results, file = this.load$OUTPUT_FILE [rownum])
    dbDisconnect (tt.conn)
    
  } # end LOAD CACHE loop


  
  # ttLoadFromOracle() loop
  for (rownum in 1:nrow (load.config)) {
    
    
    loginfo (sprintf ("Initializing ttLoadFromOracle iteration #%s ...",
                      rownum))
    this.load <- load.config [rownum,]
    
    
    tt.conn <- getTTCConn (this.load$DSN [rownum], 
                           this.load$UID [rownum], 
                           this.load$PWD [rownum])
    
    loginfo ("Truncating and check pointing ...")
    dbSendQuery (tt.conn, sprintf ("TRUNCATE TABLE %s",
                                   this.load$LOAD_ORA_TABLE [rownum]))
    
    dbCommit (tt.conn)
    
    # These checkpoints may fail due to TT0625 when another background
    # checkpoint is in progress.
    try ({
      
      dbSendQuery (tt.conn, "Call ttCkpt")
      dbSendQuery (tt.conn, "Call ttCkpt")},
      
      silent = TRUE)
    
    
    config.stmt <- sprintf ("CALL ttDbConfig ('_ParallelLoadSamplingSize', %s)",
                            this.load$SAMPLING_SIZE [rownum])
    
    loginfo (config.stmt)
    dbSendQuery (tt.conn, config.stmt)
    
    
    
    
    
    # build the ttLoadFromOracle() statement
    load.options <- sprintf ("'readers = %s; DirectLoad = %s'",
                             this.load$FETCHERS [rownum],
                             ifelse (this.load$DIRECT_LOAD [rownum], "Y", "N"))
    
    load.ora.stmt <- sprintf ("CALL ttLoadFromOracle (, '%s', '%s', %s, %s)",
                              this.load$LOAD_ORA_TABLE [rownum],
                              
                              sprintf ("SELECT * FROM %s WHERE ROWNUM <= %s",
                                       this.load$LOAD_CACHE_TABLE [rownum],
                                       this.load$TABLE_LOAD_ROW_LIMIT [rownum]),
                              
                              this.load$FETCHERS [rownum] + 
                                this.load$INSERTERS [rownum],
                              
                              load.options)
    
    
    # execute the ttLoadFromOracle() statement
    loginfo (load.ora.stmt)
    load.results$LOAD_FROM_ORA_STMT [rownum] <- load.ora.stmt
    load.results$LOAD_FROM_ORA_EXEC_TIME [rownum] <- Sys.time()
    
    tic ()
    result <- dbGetQuery(tt.conn, load.ora.stmt)
    timing <- toc (quiet = FALSE)
    loginfo (timing)
    
    dbCommit (tt.conn)
    
    
    # update the load.result
    load.results$LOAD_FROM_ORA_EXEC_SECS [rownum] <- timing$toc - timing$tic
    loginfo ("Elapsed minutes: %s",
             load.results$LOAD_FROM_ORA_EXEC_SECS [rownum] / 60)
    
    
    # report the row load count
    rows.loaded <- dbGetQuery(tt.conn,
                              sprintf ("SELECT COUNT (*) FROM %s",
                                       this.load$LOAD_ORA_TABLE [rownum]))
    
    load.results$LOAD_FROM_ORA_ROW_COUNT [rownum] <- rows.loaded
    loginfo ("Total rows loaded: %s", rows.loaded)
    
    saveRDS (load.results, file = this.load$OUTPUT_FILE [rownum])    
    dbDisconnect (tt.conn)
    
  } # end ttLoadFromOracle() loop
  
  
  



  return (load.results)
}



summarizeLoadResults <- function (load.results) {
  
  cache.loads <- select (load.results,
                         STMT = LOAD_CACHE_STMT,
                         FETCHERS, 
                         INSERTERS,
                         EXEC_SECS = LOAD_CACHE_EXEC_SECS)
  
  cache.loads$LOAD_TYPE <- "LOAD CACHE"
  
  
  ora.loads <- select (load.results,
                       STMT = LOAD_FROM_ORA_STMT,
                       FETCHERS, 
                       INSERTERS,
                       EXEC_SECS = LOAD_FROM_ORA_EXEC_SECS)
  
  ora.loads$LOAD_TYPE <- "ttLoadFromOracle"
  
  loads <- union (ora.loads, cache.loads)
  loads$EXEC_MINS <- round (loads$EXEC_SECS / 60)
  
  loads <- arrange (loads, FETCHERS, INSERTERS)
  
  return (loads)
}



plot1 <- function (load.summary) {
  
  ggplot (load.summary,
          aes (x = as.factor (FETCHERS),
               y = EXEC_MINS,
               color = LOAD_TYPE,
               fill = LOAD_TYPE)) +
    
    geom_col () +
    coord_flip() +
    
    facet_wrap ( ~ LOAD_TYPE + INSERTERS ,
                 labeller = labeller (.default = label_both,
                                      .multi_line = TRUE),
                 ncol = 3,
                 scales = "fixed") +
    
    scale_x_discrete (position = "top") +
    
    
    ggtitle ("Parallel Load Performance") +
    
    xlab ("FETCHERS") +
    ylab ("Elapsed Minutes") + 
    theme(text = element_text (size = 18))  
  
}


plot2 <- function (load.summary) {
  
  ggplot (load.summary,
          aes (x = as.factor (FETCHERS),
               y = EXEC_MINS,
               color = LOAD_TYPE,
               fill = LOAD_TYPE)) +
    
    geom_col (position = "dodge") + 
    
    facet_wrap ( ~ INSERTERS ,
                 labeller = labeller (.default = label_both,
                                      .multi_line = TRUE),
                 ncol = 3,
                 scales = "fixed") +
    
    scale_x_discrete (position = "top") +
    
    
    ggtitle ("Parallel Load Performance") +
    
    xlab ("FETCHERS") +
    ylab ("Elapsed Minutes") + 
    theme(text = element_text (size = 18))  
  
  
}



getExecConfig <- function () {
  
  
  
  # results are saved to this file everytime a load completes
  results.file <- "load-run-3.results"
  
  
  # constants
  tt.dsn <- "USERTEST_denac628"
  tt.uid <- "TATP"
  tt.pwd <- "ttguest"
  
  
  load.ora.table <- "SUBSCRIBER_ORA_LOAD"
  load.cache.table <- "SUBSCRIBER"
  load.cache.group <- "TATP"
  
  
  
  # Loads will be limited to 110M rows. This is based on a rough measurement of
  # the time required to load rows for one hour without parallelism on denac628.
  # load.row.limit <- as.integer (110000000)
  
  # debug 
  # load.row.limit <- as.integer (10000000)
  load.row.limit <- as.integer (10000)
  
  
  direct.load <- TRUE
  commit.every.n.rows <- as.integer (1024)
  
  
  parallel.load.sampling.size <- 50
  
  # this is the sequence of fetcher and inserter combinations to test
  # fetchers <-  c (1, 2, 1, 2, 1, 4, 4, 2, 4)
  # inserters <- c (1, 1, 2, 2, 4, 1, 2, 4, 4)
  
  fetchers <- 1:7
  inserters <- 7:1
  
  
  load.config <- data.frame (OUTPUT_FILE = results.file,
                             
                             DSN= tt.dsn,
                             UID = tt.uid,
                             PWD = tt.pwd,
               