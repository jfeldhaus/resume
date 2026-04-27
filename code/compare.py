#!/usr/bin/python

# compare.py
# Compare tables for two different databases


import sys
import time
import subprocess

import argparse
import logging

import cx_Oracle


# configure logging
LOG_LEVEL = logging.INFO
LOG_FORMAT = '[%(asctime)s] %(message)s'

logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
log = logging.getLogger()




# get the command line arguments
parser = argparse.ArgumentParser(
    description="Compare tables between two databases")

parser.add_argument("-connstrsrc", type=str, required=True,
    help="OCI connection string for the source database")

parser.add_argument("-connstrtgt", type=str, required=True,
    help="OCI connection string for the target database")


parser.add_argument("-uid", type=str, required=True,
    help="Username")

parser.add_argument("-pwd", type=str, required=True,
    help="Password")


parser.add_argument("-compdata", required=False, action='store_true',
    help="Compare table data in addition to row counts")


args = parser.parse_args()




# main execution procedure
def main():
 
  log.info("Starting ...")
  
  log.info("")
  log.info("Connecting to source: %s" % (args.connstrsrc))
  
  conn_src = cx_Oracle.connect(args.uid, args.pwd, args.connstrsrc) 
  log.info("Source connection succeeded") 


  log.info("")
  log.info("Connecting to target: %s" % (args.connstrtgt))
  
  conn_tgt = cx_Oracle.connect(args.uid, args.pwd, args.connstrtgt) 
  log.info("Target connection succeeded")



  # get comparison queries
  log.info("")
  count_stmts = get_table_query_stmts(conn_src)
  target_tables = set(
      (table[0].upper(), table[1].upper()) for table in get_tables(conn_tgt))

  status = True
  
  for stmt in count_stmts:
    table_identifier = (stmt[0].upper(), stmt[1].upper())

    if table_identifier not in target_tables:
      log.warning("Skipping table %s.%s: Not present in target" %
                  (stmt[0], stmt[1]))
      log.warning("")
      
      continue

    
    # compare table row counts
    status_count = compare_count_stmt(stmt[2], conn_src, conn_tgt)
    
    if not status_count:
      status = False

    # compare table data
    if args.compdata == True:
      status_data = compare_data_stmt(stmt[3], conn_src, conn_tgt)
    
      if not status_data:
        status = False

  

  # clean up
  conn_src.close()
  conn_tgt.close()


  # final result
  log.info("")
  
  if not status:
    log.error("Comparison failed")
    sys.exit(1)
  else:
    log.info("Comparison passed")
    sys.exit(0)

  return



# Compare query contents for the source and target. If the comparison passes
# then True is returned, otherwise False.
def compare_data_stmt(stmt, conn_src, conn_tgt):
  
  status = True
  
  # get the list of column names returned by the query
  cursor = conn_src.cursor()
  cursor.execute(stmt)
  
  column_names = ""
            
  for column in cursor.description:
    column_names = column_names + "\"" + column[0] + "\", "
          
  cursor.close()
          
  # construct the ordered version of the statement
  stmt = stmt + " ORDER BY " + column_names
  stmt = stmt.rstrip(", ")
  log.info(stmt)        
  
  
  # compare the column values
  cursor_src = conn_src.cursor()
  cursor_src.execute(stmt)
  
  cursor_tgt = conn_tgt.cursor()
  cursor_tgt.execute(stmt)
  
  
  row_count = 0
  
  while True:
    row_src = cursor_src.fetchone()
    row_tgt = cursor_tgt.fetchone()
    
    if row_src is None or row_tgt is None:
      break
    
    row_count = row_count + 1
  
    if compare_rows(row_src, row_tgt) is False:
      log.error("FAILED: Comparison failed at row %s\n" % (row_count))
      log.error("")
      
      status = False
      break
  
  
  cursor_src.close()
  cursor_tgt.close()

  if status:
    log.info("Processed %s rows" % row_count)
    log.info("OK")
    log.info("")
    
  return (status)



# Compare the values between two fetched rows. If the row values match then
# return True, otherwise False.
def compare_rows(row_src, row_tgt):
  
  if len(row_src) != len(row_tgt):
    log.error("Incorrect column count")
    return(False)
  
  index = 0
  
  while index < len(row_src):
    
    value_src = row_src[index]
    value_tgt = row_tgt[index]
  
    if value_src != value_tgt:
      log.error("Inconsistent value at column %s" % (index + 1))
      log.error("Source: \"%s\"" % (value_src))
      log.error("Target: \"%s\"" % (value_tgt))
      return(False)
        
    index = index +1
    
  
  return(True)
  
  

# compare the table row counts for the source and target
def compare_count_stmt(stmt, conn_src, conn_tgt):

  log.info(stmt)
  
  cursor = conn_src.cursor()
  cursor.execute(stmt)
  row_count_src = cursor.fetchone()[0]
  cursor.close() 

  
  cursor = conn_tgt.cursor()
  cursor.execute(stmt)
  row_count_tgt = cursor.fetchone()[0]
  cursor.close() 
  
  log.info("Source row count: %s, Target row count: %s" % 
           (row_count_src, row_count_tgt))

  if row_count_src != row_count_tgt:
    log.error("FAILED: Row counts do not match")
    log.error("")
    return(False)
  else:
    log.info("OK")
    log.info("")

  return (True)
 


# get SQL queries to compare tables 
def get_table_query_stmts(conn):
  
  cursor = conn.cursor()
  
  # exclude JSON index materialized views
  cursor.execute(       
      "SELECT "
      "  USER AS \"USER\", "
      "  OBJECT_NAME AS \"TABLE\", "
      "  'SELECT COUNT (*) FROM \"' || USER ||  '\".\"' || OBJECT_NAME || '\"' AS COUNT_STMT, "
      "  'SELECT * FROM \"' || USER ||  '\".\"' || OBJECT_NAME || '\"' AS DATA_STMT "
      "FROM USER_OBJECTS "
      "WHERE OBJECT_TYPE = 'TABLE' "
      "AND OBJECT_NAME NOT LIKE 'JSM$%' "
      "ORDER BY 2, 1")
 
  count_stmts = cursor.fetchall()
  cursor.close() 
  
  return (count_stmts)
 


def get_tables(conn):
  
  cursor = conn.cursor()
  
  # exclude JSON index materialized views
  cursor.execute(      
      "SELECT "
      "  USER, OBJECT_NAME "
      "FROM USER_OBJECTS "
      "WHERE OBJECT_TYPE = 'TABLE' "
      "AND OBJECT_NAME NOT LIKE 'JSM$%'")
 
  tables = cursor.fetchall()

  cursor.close() 
  return (tables)

  


# execute
if __name__ == "__main__":
    main()
