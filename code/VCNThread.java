// cnThread.java



// IMPORTS
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;



public class cnThread extends Thread
{
  
  // ATTRIBUTES
  private Connection m_conn = null;
  private cnManager m_manager;

  private String m_threadName;
  private int m_threadId;
  private int m_txns;
  private boolean m_status;
  private Random m_rand;
  private TTTimer m_timer;

  private boolean m_ready = false;
  private boolean m_start = false;


  // this is a list of the name and number of all transactions and their 
  // probabilities
  LinkedHashMap<String, Integer> m_txnMap;
  LinkedHashMap<String, Integer> m_txnProbsMap;

  private CallableStatement m_stmt_dels;
  private CallableStatement m_stmt_puts;

  private CallableStatement m_stmt_del_ipv6s;
  private CallableStatement m_stmt_put_ipv6s;

  private CallableStatement m_stmt_del_lan_s; 
  private CallableStatement m_stmt_put_lan_s;

  private CallableStatement m_stmt_del_sec_lists; 
  private CallableStatement m_stmt_put_sec_lists;

  private CallableStatement m_stmt_del_cns;
  private CallableStatement m_stmt_put_cns;

  private CallableStatement m_stmt_del_garp;
  private CallableStatement m_stmt_put_garp;

  private CallableStatement m_stmt_del_nsg_assoc;
  private CallableStatement m_stmt_put_nsg_assoc;

  private CallableStatement m_stmt_del_float_pvt_ips;
  private CallableStatement m_stmt_put_float_pvt_ips;

  private CallableStatement m_stmt_del_float_pvt_ip_atts;
  private CallableStatement m_stmt_put_float_pvt_ip_atts;

  private CallableStatement m_stmt_del_float_ips;
  private CallableStatement m_stmt_put_float_ips;

  private CallableStatement m_stmt_put_cache_status;

  private CallableStatement m_stmt_del_float_ip_atts;
  private CallableStatement m_stmt_put_float_ip_atts;

  private CallableStatement m_stmt_del_nets;
  private CallableStatement m_stmt_put_nets;

  private CallableStatement m_stmt_del_net_sec_group;
  private CallableStatement m_stmt_put_net_sec_group;

  private CallableStatement m_stmt_query_get;
  private CallableStatement m_stmt_query_get_net_sec_groups;
  private CallableStatement m_stmt_query_get_prim_pub_ip;
  private CallableStatement m_stmt_query_get_prim_priv_ip;

  private CallableStatement m_stmt_query_get_ipv6_addr;

  // OPERATIONS
  
  // ----------------------------------------
  // cnThread ()
  public cnThread (cnManager manager, int threadId, int txns,
    LinkedHashMap<String, Integer> txnMap,
    LinkedHashMap<String, Integer> txnProbsMap)
  {
    
    // initialize the thread name with the xid data
    super ("cnThread");
    
    // initialize attributes
    m_manager = manager;
    m_threadId = threadId;
    m_txns = txns;

    m_threadName = "cnThread " + m_threadId;
    m_rand = new Random ();

    m_timer = new TTTimer ();   


    // the transaction list and probabilties
    m_txnMap = txnMap;
    m_txnProbsMap = txnProbsMap;


    // the status of the branch is 'success' unless something
    // unexpected occurs
    m_status = true;
  
    // report the creation of this TxnBranch
    m_manager.log (m_threadName, "cnThread created.");
    
    return;
  }
  

  
  // ----------------------------------------
  // checkoutConn ()
  // Checks a connection out from the cnManager
  private void checkoutConn () throws SQLException
  {
    // checkout a connection
    do
    {        
      m_conn = m_manager.checkoutConn (m_threadName);

      // sleep before trying again
      if (m_conn == null)
      {
        try
        {
          this.sleep (25);
        }
        catch (java.lang.InterruptedException ex) 
        {
          m_status = false;
          m_manager.logErr (m_threadName, ex.getMessage ()); 
          break;
        }
      }
    }
    while (m_conn == null);


    // turn off autocommit for 2-safe
    m_conn.setAutoCommit (false);

    return;
  }
  
  
  // ----------------------------------------
  // checkinConn ()
  // Checks a connection back into the cnManager
  private void checkinConn () throws SQLException
  {
    m_conn.setAutoCommit (true);
    m_manager.checkinConn (m_threadName, m_conn);    
    m_conn = null;
 
    return;
  }
  
  
  // ----------------------------------------
  // run ()
  // Execution of the cnThread begins here
  public void run ()
  {
    
    // report the beginning of the thread
    m_manager.log (m_threadName, "Running...");
    
    // get the set of transaction codes
    Object[] codes = m_txnMap.values().toArray();
    
    // convert probabilites to double and convert the values to fractions
    Object [] objectProbs = m_txnProbsMap.values().toArray();
    double [] probs = new double [objectProbs.length];

    for (int index = 0; index < objectProbs.length; index++) 
    {
      probs[index] = Double.valueOf ((Integer) objectProbs[index]);
      probs[index] = probs[index] / 100;
    }        


    // execute PLSQL operations
    try
    {
      checkoutConn ();
      prepareCalls ();

      // indicate that the thread is ready to execute
      m_ready = true;

      // wait for the start flag
      m_manager.log (m_threadName, "Waiting to start ...");
      
      while (m_start == false)
      {
        Thread.sleep (1);
      }


      for (int iteration = 0; iteration < m_txns; iteration ++)
      {
        m_manager.log (m_threadName, "Iteration #" + iteration);



        // get the random transaction code
        int index = getRandomIndex (probs, m_rand);
        int txnCode = (int) codes [index];



        m_manager.log (m_threadName, "Executing transaction code " + 
          txnCode + " ...");

        try
        {

          switch (txnCode) 
          {
            // VNICS_DATA
            case 16:
              dels ();
              break;
            
            case 26:
              puts ();
              break;

            // IPV6S
            case 7:
              del_ipv6s ();
              break;

            case 14:
              put_ipv6s ();
              break;

            // lan_S
            case 9:
              del_lan_s ();
              break;
            
            case 22:
              put_lan_s ();
              break;

            // sec_LISTS_REGION
            case 11:
              del_sec_lists ();
              break;
              
            case 21:
              put_sec_lists ();
              break;

            // cnS
            case 4:
              del_cns ();
              break;

            case 18:
              put_cns ();
              break;

            // nic_GARP_INFO
            case 3:
              del_garp ();
              break;
            
            case 15:
              put_garp ();
              break;

            // NSG_ASSOCIATIONS
            case 12:
              del_nsg_assoc ();
              break;

            case 13:  
              put_nsg_assoc ();
              break;

            // FLOATING_PVT_IPS
            case 6:
              del_float_pvt_ips ();
              break;

            case 25:  
              put_float_pvt_ips ();
              break;

            // FLOATING_PVT_IP_ATTS
            case 5:
              del_float_pvt_ip_atts ();
              break;

            case 17:
              put_float_pvt_ip_atts ();
              break;

            // FLOATING_IPS
            case 2:
              del_float_ips ();
              break;

            case 20:
              put_float_ips ();
              break;

            // CACHE_STATUS
            case 27:
              put_cache_status ();
              break;

            // FLOATING_IP_ATTS
            case 8:
              del_float_ip_atts ();
              break;
            
            case 19:  
              put_float_ip_atts ();
              break;

            // SUBNETS
            case 1:
              del_nets ();
              break;

            case 24:  
              put_nets ();
              break;

            // net_sec_GROUP
            case 10:
              del_net_sec_group ();
              break;
            
            case 23:
              put_net_sec_group ();
              break;
            

            // queries
            case 31:
              query_get ();
              break;

            case 30:
              query_get_net_sec_groups ();
              break;

            case 28:  
              query_get_prim_pub_ip ();
              break;

            case 29:  
              query_get_prim_priv_ip ();
              break;

            case 32:  
              query_get_ipv6_addr ();
              break;

            default:
              throw new Exception ("Invalid transaction code");
          }

        }
        catch (SQLException ex)
        {
          m_manager.handleSQLException (m_threadName, ex);
          
          // if an error occurs in a transaction, we may need to rollback
          m_conn.rollback ();
        }


      } // end transaction loop




      checkinConn ();
    }
    catch (Exception ex)
    {
      // unexpected failure
      m_manager.handleException (m_threadName, ex);
      m_status = false;
    }
    finally
    {
      closeStatements ();

      if (m_conn != null)
      {
        try
        {
          checkinConn ();
        }
        catch (SQLException ex)
        {
          m_manager.handleSQLException (m_threadName, ex);
          m_status = false;
        }
      }
    }

    if (m_status == false)
      m_manager.logErr (m_threadName, "FAILED");
    else
      m_manager.log (m_threadName, "PASSED");
    
    return;
  }
  


  private void closeStatements ()
  {
    closeStatement (m_stmt_dels);
    closeStatement (m_stmt_puts);
    closeStatement (m_stmt_del_ipv6s);
    closeStatement (m_stmt_put_ipv6s);
    closeStatement (m_stmt_del_lan_s);
    closeStatement (m_stmt_put_lan_s);
    closeStatement (m_stmt_del_sec_lists);
    closeStatement (m_stmt_put_sec_lists);
    closeStatement (m_stmt_del_cns);
    closeStatement (m_stmt_put_cns);
    closeStatement (m_stmt_del_garp);
    closeStatement (m_stmt_put_garp);
    closeStatement (m_stmt_del_nsg_assoc);
    closeStatement (m_stmt_put_nsg_assoc);
    closeStatement (m_stmt_del_float_pvt_ips);
    closeStatement (m_stmt_put_float_pvt_ips);
    closeStatement (m_stmt_del_float_pvt_ip_atts);
    closeStatement (m_stmt_put_float_pvt_ip_atts);
    closeStatement (m_stmt_del_float_ips);
    closeStatement (m_stmt_put_float_ips);
    closeStatement (m_stmt_put_cache_status);
    closeStatement (m_stmt_del_float_ip_atts);
    closeStatement (m_stmt_put_float_ip_atts);
    closeStatement (m_stmt_del_nets);
    closeStatement (m_stmt_put_nets);
    closeStatement (m_stmt_del_net_sec_group);
    closeStatement (m_stmt_put_net_sec_group);
    closeStatement (m_stmt_query_get);
    closeStatement (m_stmt_query_get_net_sec_groups);
    closeStatement (m_stmt_query_get_prim_pub_ip);
    closeStatement (m_stmt_query_get_prim_priv_ip);
    closeStatement (m_stmt_query_get_ipv6_addr);

    m_stmt_dels = null;
    m_stmt_puts = null;
    m_stmt_del_ipv6s = null;
    m_stmt_put_ipv6s = null;
    m_stmt_del_lan_s = null;
    m_stmt_put_lan_s = null;
    m_stmt_del_sec_lists = null;
    m_stmt_put_sec_lists = null;
    m_stmt_del_cns = null;
    m_stmt_put_cns = null;
    m_stmt_del_garp = null;
    m_stmt_put_garp = null;
    m_stmt_del_nsg_assoc = null;
    m_stmt_put_nsg_assoc = null;
    m_stmt_del_float_pvt_ips = null;
    m_stmt_put_float_pvt_ips = null;
    m_stmt_del_float_pvt_ip_atts = null;
    m_stmt_put_float_pvt_ip_atts = null;
    m_stmt_del_float_ips = null;
    m_stmt_put_float_ips = null;
    m_stmt_put_cache_status = null;
    m_stmt_del_float_ip_atts = null;
    m_stmt_put_float_ip_atts = null;
    m_stmt_del_nets = null;
    m_stmt_put_nets = null;
    m_stmt_del_net_sec_group = null;
    m_stmt_put_net_sec_group = null;
    m_stmt_query_get = null;
    m_stmt_query_get_net_sec_groups = null;
    m_stmt_query_get_prim_pub_ip = null;
    m_stmt_query_get_prim_priv_ip = null;
    m_stmt_query_get_ipv6_addr = null;
  }


  private void closeStatement (PreparedStatement stmt)
  {
    if (stmt != null)
    {
      try
      {
        stmt.close ();
      }
      catch (SQLException ex)
      {
        m_manager.handleSQLException (m_threadName, ex);
        m_status = false;
      }
    }
  }



  // ----------------------------------------
  // prepareCalls ()
  private void prepareCalls () throws SQLException
  {
    
    m_stmt_dels = m_conn.prepareCall
      ("BEGIN dels (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;");
    
    m_stmt_puts = m_conn.prepareCall
      ("BEGIN puts (:updated_rows, :ls_id, :ls_fencing_token, " + 
      ":nic_id, :nic_subnet_id, :nic_com_id, :nic_lan__id, " + 
      ":nic_public_ip, :nic_overlay_mac, :nic_resource_id, " + 
      ":nic_is_service, :nic_do_json); END;"); 
  

    m_stmt_del_ipv6s = m_conn.prepareCall
      ("BEGIN del_ipv6s (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;");

    m_stmt_put_ipv6s = m_conn.prepareCall
      ("BEGIN put_ipv6s ( " + 
                "        :updated_rows, " + 
                "        :ls_id, " + 
                "        :ls_fencing_token, " + 
                "        :ipv6_id, " + 
                "        :ipv6_id, " + 
                "        :ipv6_subnet_id, " + 
                "        :ipv6_ip_address, " + 
                "        :ipv6_time_created, " + 
                "        :ipv6_do_json); END;");

    
    m_stmt_del_lan_s = m_conn.prepareCall
      ("BEGIN del_lan_s (:lease_valid, :ls_id, :ls_fencing_token, :lan__id); END;");

    m_stmt_put_lan_s = m_conn.prepareCall
      ("BEGIN put_lan_s (" + 
                      " :updated_rows, " +
                      " :ls_id, " +
                      " :ls_fencing_token, " +
                      " :lan__id, " +
                      " :lan__com_id, " +
                      " :lan__cn_id, " +
                      " :lan__route_table_id, " +
                      " :lan__display_name, " +
                      " :lan__virtual_router_mac, " +
                      " :lan__time_created, " +
                      " :lan__do_json); END;");



    m_stmt_del_sec_lists = m_conn.prepareCall
      ("BEGIN del_sec_lists (:lease_valid, :ls_id, :ls_fencing_token, :sl_id); END;");


    m_stmt_put_sec_lists = m_conn.prepareCall
      ("BEGIN  put_sec_lists (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token,  " +
                "        :sl_id, " +
                "        :sl_com_id, " +
                "        :sl_cn_id,  " +
                "        :sl_display_name,  " +
                "        :sl_time_created,  " +
                "        :sl_do_json); END;");


    m_stmt_del_cns = m_conn.prepareCall
      ("BEGIN del_cns (:lease_valid, :ls_id, :ls_fencing_token, :cn_id); END;");
          
    m_stmt_put_cns = m_conn.prepareCall
      ("BEGIN put_cns (" + 
                "        :updated_rows, " + 
                "        :ls_id, " + 
                "        :ls_fencing_token, " + 
                "        :cn_id, " + 
                "        :default_route_table_id, " + 
                "        :default_sec_list_id, " + 
                "        :default_dhcp_options_id, " + 
                "        :com_id, " + 
                "        :display_name, " + 
                "        :time_created, " + 
                "        :cn_do_json) ; END;");


    m_stmt_del_garp = m_conn.prepareCall 
      ("BEGIN del_garp_info (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;");

    m_stmt_put_garp = m_conn.prepareCall 
      ("BEGIN put_garp_info (" + 
                "        :updated_rows, " + 
                "        :ls_id,  " + 
                "        :ls_fencing_token,  " + 
                "        :nic_id,  " + 
                "        :nic_garp_info_do_json) ; END;");



    m_stmt_del_nsg_assoc = m_conn.prepareCall
      ("BEGIN del_nsg_associations (:lease_valid, :ls_id, :ls_fencing_token, :nsg_id, :nic_id); END;");

    m_stmt_put_nsg_assoc = m_conn.prepareCall
      ("BEGIN put_nsg_associations (" + 
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_id, " +
                "        :nic_id, " +
                "        :association_time, " +
                "        :nsg_assc_do_json)  ; END;");


    m_stmt_del_float_pvt_ips = m_conn.prepareCall
        ("BEGIN del_floating_pvt_ips (" +
         "  :lease_valid, " +
         "  :ls_id, " +
         "  :ls_fencing_token, " +
         "  :fpip_id )  ; END;");

    m_stmt_put_float_pvt_ips = m_conn.prepareCall
        ("BEGIN put_floating_pvt_ips ( " +
          ":updated_rows, " +
          ":ls_id, " +
          ":ls_fencing_token, " +
          ":fpip_id, " +
          ":fpip_subnet_id, " +
          ":fpip_ip_address_int, " +
          ":fpip_time_created, " +
          ":fpip_display_name, " +
          ":fpip_lan__id, " +
          ":fpip_do_json); END;");



    m_stmt_del_float_pvt_ip_atts = m_conn.prepareCall
      ("BEGIN del_floating_pvt_ip_atts (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fpipatt_id); END;");

    m_stmt_put_float_pvt_ip_atts = m_conn.prepareCall
      ("BEGIN put_floating_pvt_ip_atts (" + 
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fpipatt_id, " +
                "        :fpipatt_fpip_id, " +
                "        :fpipatt_id, " +
                "        :fpipatt_nat_ip_addr, " +
                "        :fpiatt_lifecycle_state, " +
                "        :fpipatt_do_json); END;");


    m_stmt_del_float_ips = m_conn.prepareCall
      ("BEGIN del_floating_ips (" +
                "        :lease_valid, " +
                "        :ls_id,  " +
                "        :ls_fencing_token,  " +
                "        :fip_id); END;");

    m_stmt_put_float_ips = m_conn.prepareCall
      ("BEGIN put_floating_ips ( " +
          " :updated_rows, " +
          " :ls_id, " +
          " :ls_fencing_token, " +
          " :fip_id, " +
          " :fip_ip_addr, " +
          " :fip_com_id, " +
          " :fip_scope, " +
          " :fip_availability_domain, " +
          " :fip_lifetime, " +
          " :fip_public_ip_pool_id, " +
          " :fip_do_json); END;");


    m_stmt_put_cache_status = m_conn.prepareCall
      ("BEGIN put_cache_status (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :cs_id, " +
                "        :cs_cursor, " +
                "        :cs_commit_txn_id, " +
                "        :cs_status, " +
                "        :cs_buckets); END;");          


    m_stmt_del_float_ip_atts = m_conn.prepareCall 
      ("BEGIN del_floating_ip_atts (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fipatt_id); END;");


    m_stmt_put_float_ip_atts = m_conn.prepareCall 
      ("BEGIN put_floating_ip_atts (" +
      "   :updated_rows, " +
      "   :ls_id, " +
      "   :ls_fencing_token, " +
      "   :fipatt_id, " +
      "   :fipatt_id, " +
      "   :fipatt_floating_ip_id, " +
      "   :fipatt_floating_private_ip_id, " +
      "   :fipatt_assigned_entity_id, " +
      "   :fipatt_lifecycle_state, " +
      "   :fipatt_do_json); END;");

    m_stmt_del_nets = m_conn.prepareCall 
      ("BEGIN del_nets ( " +
      " :lease_valid, " +
      " :ls_id, " +
      " :ls_fencing_token, " +
      " :sn_id)    ; END;"); 

    m_stmt_put_nets = m_conn.prepareCall
      ("BEGIN put_nets (" +
          ":updated_rows, " +
          ":ls_id, " +
          ":ls_fencing_token, " +
          ":sn_id, " +
          ":sn_com_id, " +
          ":sn_cn_id, " +
          ":sn_route_table_id, " +
          ":sn_dhcp_options_id, " +
          ":sn_dns_label, " +
          ":sn_display_name, " +
          ":sn_time_created, " +
          ":sn_do_json)   ; END;");


    m_stmt_del_net_sec_group = m_conn.prepareCall
      ("BEGIN del_net_sec_group (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_id); END;");

    m_stmt_put_net_sec_group = m_conn.prepareCall
      ("BEGIN put_net_sec_group (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_id, " +
                "        :nsg_dp_id, " +
                "        :nsg_com_id, " +
                "        :nsg_cn_id, " +
                "        :nsg_display_name, " +
                "        :nsg_time_created, " +
                "        :nsg_do_json); END;"); 
    

    m_stmt_query_get = m_conn.prepareCall
      ("SELECT do_json FROM VNICS WHERE id = ?");

    m_stmt_query_get_net_sec_groups = m_conn.prepareCall
      ("SELECT NSG.do_json FROM net_sec_GROUP NSG " + 
                "INNER JOIN NSG_ASSOCIATIONS NSG_ASC " + 
                "ON NSG.id = NSG_ASC.nsg_id " + 
                "WHERE NSG_ASC.nic_id = ?");


    m_stmt_query_get_prim_pub_ip = m_conn.prepareCall
      ("SELECT FIPs.do_json, FIPAs.do_json FROM FLOATING_IP_ATTS FIPAs " +
        "INNER JOIN FLOATING_IPS FIPs " +
        "ON FIPAs.floating_ip_id = FIPs.id " +
        "WHERE FIPAs.floating_private_ip_id = ? " +
        "AND FIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"); 

    m_stmt_query_get_prim_priv_ip = m_conn.prepareCall
        ("SELECT FPIPs.do_json, FPIPAs.do_json FROM FLOATING_PVT_IP_ATTS FPIPAs " +
          "INNER JOIN FLOATING_PVT_IPS FPIPs " +
          "ON FPIPAs.floating_private_ip_id = FPIPs.id " +
          "WHERE FPIPAs.nic_id = ? " +
          "AND FPIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')");


    m_stmt_query_get_ipv6_addr =  m_conn.prepareCall
      ("SELECT do_json, time_created FROM IPV6S " +
        "WHERE nic_id = ? " +
        "AND rownum <= 32 " +
        "ORDER BY time_created ASC");
  

    return;
  }


  
  // ----------------------------------------
  // dels ()
  private void dels () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing dels input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random id from VNICS_DATA
    nic_id = m_manager.getCachedValue (m_threadName, "VNICS_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling dels ...");

    // Prepare to call PLSQL:  
    // "BEGIN dels (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_dels.registerOutParameter (1, Types.INTEGER);
    m_stmt_dels.setObject (2, ls_id);
    m_stmt_dels.setObject (3, ls_fencing_token);
    m_stmt_dels.setObject (4, nic_id);

    m_stmt_dels.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_dels.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_dels.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "dels elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("dels", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "dels lease valid: " + lease_valid); 

    return;
  }
  
  
  // ----------------------------------------
  // puts ()
  private void puts () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    HashMap<String, Object> row;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object nic_id = "0"; 
    Object nic_subnet_id = "0"; 
    Object nic_com_id = "0";
    Object nic_lan__id = "0"; 
    Object nic_public_ip = "0"; 
    Object nic_overlay_mac = 0; 
    Object nic_resource_id = "0"; 
    Object nic_is_service = 0;
    Object nic_do_json = "0"; 



    m_manager.log (m_threadName, "Preparing puts input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VNICS_DATA
    row = m_manager.getCachedRow (m_threadName, "VNICS_DATA");

    nic_id = row.get ("ID"); 
    nic_com_id = row.get ("com_ID");
    nic_subnet_id = row.get ("SUBNET_ID"); 
    nic_lan__id = row.get ("lan__ID"); 
    nic_public_ip = row.get ("PUBLIC_IP"); 
    nic_overlay_mac = row.get ("OVERLAY_MAC"); 
    nic_resource_id = row.get ("RESOURCE_ID"); 
    nic_is_service = row.get ("IS_SERVICE");
    nic_do_json = row.get ("DO_JSON");






    m_manager.log (m_threadName, "Calling puts ...");

    // Prepare to call PLSQL: 
    //   "BEGIN puts (:updated_rows, :ls_id, :ls_fencing_token, " + 
    //   ":nic_id, :nic_subnet_id, :nic_com_id, :nic_lan__id, " + 
    //   ":nic_public_ip, :nic_overlay_mac, :nic_resource_id, " + 
    //   ":nic_is_service, :nic_do_json); END;"

    m_timer.start ();

    m_stmt_puts.registerOutParameter (1, Types.INTEGER);
    m_stmt_puts.setObject (2, ls_id);
    m_stmt_puts.setObject (3, ls_fencing_token);

    m_stmt_puts.setObject (4, nic_id);
    m_stmt_puts.setObject (5, nic_subnet_id);
    m_stmt_puts.setObject (6, nic_com_id);
    m_stmt_puts.setObject (7, nic_lan__id);

    m_stmt_puts.setObject (8, nic_public_ip);
    m_stmt_puts.setObject (9, nic_overlay_mac);
    m_stmt_puts.setObject (10, nic_resource_id);

    m_stmt_puts.setObject (11, nic_is_service);
    m_stmt_puts.setObject (12, nic_do_json);


    m_stmt_puts.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_puts.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_puts.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "puts elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("puts", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "puts updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_ipv6s ()
  private void del_ipv6s () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_ipv6s input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

    // select a random ID from IPV6S_DATA
    nic_id = m_manager.getCachedValue (m_threadName, "IPV6S_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_ipv6s ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_ipv6s (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_ipv6s.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_ipv6s.setObject (2, ls_id);
    m_stmt_del_ipv6s.setObject (3, ls_fencing_token);
    m_stmt_del_ipv6s.setObject (4, nic_id);

    m_stmt_del_ipv6s.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_ipv6s.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_ipv6s.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_ipv6s elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_ipv6s", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_ipv6s lease valid: " + lease_valid); 

    return;
  }
  


  // ----------------------------------------
  // put_ipv6s ()
  private void put_ipv6s () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    HashMap<String, Object> row;

    Object ls_id = "0";
    Object ls_fencing_token = 0;
   
    Object ipv6_id = "0";  
    Object ipv6_id = "0";
    Object ipv6_subnet_id = "0";
    Object ipv6_ip_address = "0"; 
    Object ipv6_time_created = null; 
    Object ipv6_do_json = "0"; 



    m_manager.log (m_threadName, "Preparing put_ipv6s input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from IPV6S_DATA
    row = m_manager.getCachedRow (m_threadName, "IPV6S_DATA");

    ipv6_id = row.get ("ID"); 
    ipv6_id = row.get ("nic_ID");
    ipv6_subnet_id = row.get ("SUBNET_ID");
    ipv6_ip_address = row.get ("IP_ADDRESS");
    ipv6_time_created = row.get ("TIME_CREATED"); 
    ipv6_do_json = row.get ("DO_JSON");


    m_manager.log (m_threadName, "Calling put_ipv6s ...");

    // Prepare to call PLSQL: 
    //   "BEGIN put_ipv6s ( " + 
    //"        :updated_rows, " + 
    //"        :ls_id " + 
    //"        :ls_fencing_token " + 
    //"        :ipv6_id " + 
    //"        :ipv6_id " + 
    //"        :ipv6_subnet_id " + 
    //"        :ipv6_ip_address " + 
    //"        :ipv6_time_created " + 
    //"        :ipv6_do_json); END;"

    m_timer.start ();

    m_stmt_put_ipv6s.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_ipv6s.setObject (2, ls_id);
    m_stmt_put_ipv6s.setObject (3, ls_fencing_token);

    m_stmt_put_ipv6s.setObject (4, ipv6_id);
    m_stmt_put_ipv6s.setObject (5, ipv6_id);
    m_stmt_put_ipv6s.setObject (6, ipv6_subnet_id);
    m_stmt_put_ipv6s.setObject (7, ipv6_ip_address);
    m_stmt_put_ipv6s.setObject (8, ipv6_time_created);
    m_stmt_put_ipv6s.setObject (9, ipv6_do_json);


    m_stmt_put_ipv6s.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_ipv6s.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_ipv6s.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_ipv6s elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_ipv6s", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_ipv6s updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_lan_s ()
  private void del_lan_s () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object lan__id = "0";

    
    m_manager.log (m_threadName, "Preparing del_lan_s input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

    // select a random ID from lan_S_DATA
    lan__id = m_manager.getCachedValue (m_threadName, "lan_S_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_lan_s ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_lan_s (:lease_valid, :ls_id, :ls_fencing_token, :lan__id); END;"
      
    m_timer.start ();

    m_stmt_del_lan_s.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_lan_s.setObject (2, ls_id);
    m_stmt_del_lan_s.setObject (3, ls_fencing_token);
    m_stmt_del_lan_s.setObject (4, lan__id);

    m_stmt_del_lan_s.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_lan_s.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_lan_s.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_lan_s elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_lan_s", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_lan_s lease valid: " + lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_lan_s ()
  private void put_lan_s () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object lan__id = "0";
    Object lan__com_id = "0";
    Object lan__cn_id = "0";
    Object lan__route_table_id = "0";
    Object lan__display_name = "0";
    Object lan__virtual_router_mac = 0;
    Object lan__time_created = null;
    Object lan__do_json = "0";



    m_manager.log (m_threadName, "Preparing put_lan_s input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from lan_S_DATA
    row = m_manager.getCachedRow (m_threadName, "lan_S_DATA");

    lan__id = row.get ("ID"); 
    lan__com_id = row.get ("com_ID");
    lan__cn_id = row.get ("cn_ID");
    lan__route_table_id = row.get ("ROUTE_TABLE_ID");
    lan__display_name = row.get ("DISPLAY_NAME");
    lan__virtual_router_mac = row.get ("VIRTUAL_ROUTER_MAC");
    lan__time_created = row.get ("TIME_CREATED"); 
    lan__do_json = row.get ("DO_JSON");





    m_manager.log (m_threadName, "Calling put_lan_s ...");

    // Prepare to call PLSQL: 
    // "BEGIN put_lan_s (" + 
    //                   " :updated_rows, " +
    //                   " :ls_id, " +
    //                   " :ls_fencing_token, " +
    //                   " :lan__id, " +
    //                   " :lan__com_id, " +
    //                   " :lan__cn_id, " +
    //                   " :lan__route_table_id, " +
    //                   " :lan__display_name, " +
    //                   " :lan__virtual_router_mac, " +
    //                   " :lan__time_created, " +
    //                   " :lan__do_json); END;"

    m_timer.start ();

    m_stmt_put_lan_s.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_lan_s.setObject (2, ls_id);
    m_stmt_put_lan_s.setObject (3, ls_fencing_token);

    m_stmt_put_lan_s.setObject (4, lan__id);
    m_stmt_put_lan_s.setObject (5, lan__com_id);
    m_stmt_put_lan_s.setObject (6, lan__cn_id);

    m_stmt_put_lan_s.setObject (7, lan__route_table_id);
    m_stmt_put_lan_s.setObject (8, lan__display_name);
    m_stmt_put_lan_s.setObject (9, lan__virtual_router_mac);

    m_stmt_put_lan_s.setObject (10, lan__time_created);
    m_stmt_put_lan_s.setObject (11, lan__do_json);

    m_stmt_put_lan_s.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_lan_s.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_lan_s.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_lan_s elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_lan_s", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_lan_s updated_rows: " + updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_sec_lists ()
  private void del_sec_lists () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object sl_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_sec_lists input ...");
    


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");
 
    // select a random ID from sec_LISTS_DATA
    sl_id = m_manager.getCachedValue (m_threadName, "sec_LISTS_DATA", "ID");



    m_manager.log (m_threadName, "Calling del_sec_lists ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_sec_lists (:lease_valid, :ls_id, :ls_fencing_token, :sl_id); END;"
      
    m_timer.start ();

    m_stmt_del_sec_lists.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_sec_lists.setObject (2, ls_id);
    m_stmt_del_sec_lists.setObject (3, ls_fencing_token);
    m_stmt_del_sec_lists.setObject (4, sl_id);

    m_stmt_del_sec_lists.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_sec_lists.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_sec_lists.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_sec_lists elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_sec_lists", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_sec_lists lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_sec_lists ()
  private void put_sec_lists () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_sec_lists (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token  " +
    //             "        :sl_id, " +
    //             "        :sl_com_id, " +
    //             "        :sl_cn_id,  " +
    //             "        :sl_display_name,  " +
    //             "        :sl_time_created,  " +
    //             "        :sl_do_json); END;"

    Object sl_id = "0";
    Object sl_com_id = "0";
    Object sl_cn_id = "0";
    Object sl_display_name = "0";
    Object sl_time_created = null;
    Object sl_do_json = "0";




    m_manager.log (m_threadName, "Preparing put_sec_lists input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");



    // select a random row from sec_LISTS_DATA
    row = m_manager.getCachedRow (m_threadName, "sec_LISTS_DATA");

    sl_id = row.get ("ID"); 
    sl_com_id = row.get ("com_ID");
    sl_cn_id = row.get ("cn_ID");
    sl_display_name = row.get ("DISPLAY_NAME");
    sl_time_created = row.get ("TIME_CREATED"); 
    sl_do_json = row.get ("DO_JSON");



    m_manager.log (m_threadName, "Calling put_sec_lists ...");

    // Prepare to call PLSQL: 
    // "BEGIN put_sec_lists (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token  " +
    //             "        :sl_id, " +
    //             "        :sl_com_id, " +
    //             "        :sl_cn_id,  " +
    //             "        :sl_display_name,  " +
    //             "        :sl_time_created,  " +
    //             "        :sl_do_json); END;"


    m_timer.start ();

    m_stmt_put_sec_lists.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_sec_lists.setObject (2, ls_id);
    m_stmt_put_sec_lists.setObject (3, ls_fencing_token);

    m_stmt_put_sec_lists.setObject (4, sl_id);
    m_stmt_put_sec_lists.setObject (5, sl_com_id);
    m_stmt_put_sec_lists.setObject (6, sl_cn_id);

    m_stmt_put_sec_lists.setObject (7, sl_display_name);

    m_stmt_put_sec_lists.setObject (8, sl_time_created);

    // DEBUG:
    m_stmt_put_sec_lists.setObject (9, sl_do_json);

    // m_stmt_put_sec_lists.setString (9, sl_do_json.toString ());

    // Clob clob = m_conn.createClob();
    // clob.setString (1, sl_do_json.toString());
    // m_stmt_put_sec_lists.setClob (9, clob);


    m_stmt_put_sec_lists.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_sec_lists.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_sec_lists.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_sec_lists elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_sec_lists", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_sec_lists updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_cns ()
  private void del_cns () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object cn_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_cns input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from CNS_DATA
    cn_id = m_manager.getCachedValue (m_threadName, "CNS_DATA", "ID");

   
    m_manager.log (m_threadName, "Calling del_cns ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_cns (:lease_valid, :ls_id, :ls_fencing_token, :cn_id); END;"
      
    m_timer.start ();

    m_stmt_del_cns.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_cns.setObject (2, ls_id);
    m_stmt_del_cns.setObject (3, ls_fencing_token);
    m_stmt_del_cns.setObject (4, cn_id);

    m_stmt_del_cns.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_cns.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_cns.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_cns elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_cns", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_cns lease valid: " + 
      lease_valid); 

    return;
  }

  // ----------------------------------------
  // put_cns ()
  private void put_cns () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_cns (" + 
    //             "        updated_rows, " + 
    //             "        ls_id, " + 
    //             "        ls_fencing_token, " + 
    //             "        cn_id, " + 
    //             "        default_route_table_id, " + 
    //             "        default_sec_list_id, " + 
    //             "        default_dhcp_options_id, " + 
    //             "        com_id, " + 
    //             "        display_name, " + 
    //             "        time_created, " + 
    //             "        cn_do_json) ; END;"

    Object cn_id = "0";
    Object default_route_table_id = "0";
    Object default_sec_list_id = "0";
    Object default_dhcp_options_id = "0";
    Object com_id = "0";
    Object display_name = "0";
    Object time_created = null;
    Object cn_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_cns input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from CNS_DATA
     row = m_manager.getCachedRow (m_threadName, "CNS_DATA");

     cn_id = row.get ("ID"); 
     default_route_table_id = row.get ("DEFAULT_ROUTE_TABLE_ID"); 
     default_sec_list_id = row.get ("DEFAULT_sec_LIST_ID"); 
     default_dhcp_options_id = row.get ("DEFAULT_DHCP_OPTIONS_ID"); 
     com_id = row.get ("com_ID"); 
     display_name = row.get ("DISPLAY_NAME"); 
     time_created = row.get ("TIME_CREATED");  
     cn_do_json = row.get ("DO_JSON");     



    m_manager.log (m_threadName, "Calling put_cns ...");

    // "BEGIN put_cns (" + 
    //             "        updated_rows, " + 
    //             "        ls_id, " + 
    //             "        ls_fencing_token, " + 
    //             "        cn_id, " + 
    //             "        default_route_table_id, " + 
    //             "        default_sec_list_id, " + 
    //             "        default_dhcp_options_id, " + 
    //             "        com_id, " + 
    //             "        display_name, " + 
    //             "        time_created, " + 
    //             "        cn_do_json) ; END;"


    m_timer.start ();

    m_stmt_put_cns.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_cns.setObject (2, ls_id);
    m_stmt_put_cns.setObject (3, ls_fencing_token);

    m_stmt_put_cns.setObject (4, cn_id);
    m_stmt_put_cns.setObject (5, default_route_table_id);
    m_stmt_put_cns.setObject (6, default_sec_list_id);
    m_stmt_put_cns.setObject (7, default_dhcp_options_id);

    m_stmt_put_cns.setObject (8, com_id);
    m_stmt_put_cns.setObject (9, display_name);

    m_stmt_put_cns.setObject (10, time_created);
    m_stmt_put_cns.setObject (11, cn_do_json);

    m_stmt_put_cns.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_cns.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_cns.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_cns elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_cns", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_cns updated_rows: " + updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_garp ()
  private void del_garp () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_garp_info input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from nic_GARP_INFO_DATA
    nic_id = m_manager.getCachedValue(m_threadName, "nic_GARP_INFO_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_garp_info ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_garp_info (:lease_valid, :ls_id, :ls_fencing_token, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_garp.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_garp.setObject (2, ls_id);
    m_stmt_del_garp.setObject (3, ls_fencing_token);
    m_stmt_del_garp.setObject (4, nic_id);

    m_stmt_del_garp.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_garp.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_garp.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_garp_info elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_garp_info", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_garp_info lease valid: " + 
      lease_valid); 

    return;
  }

  // ----------------------------------------
  // put_garp ()
  private void put_garp () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_garp_info " + 
    //             "        :updated_rows, " + 
    //             "        :ls_id,  " + 
    //             "        :ls_fencing_token,  " + 
    //             "        :nic_id,  " + 
    //             "        :nic_garp_info_do_json ; END;"

    Object nic_id = "0";
    Object nic_garp_info_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_garp_info input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from nic_GARP_INFO_DATA
    row = m_manager.getCachedRow (m_threadName, "nic_GARP_INFO_DATA");
    nic_id = row.get ("ID"); 
    nic_garp_info_do_json = row.get ("DO_JSON");    




    m_manager.log (m_threadName, "Calling put_garp_info ...");

    // "BEGIN put_garp_info " + 
    //             "        :updated_rows, " + 
    //             "        :ls_id,  " + 
    //             "        :ls_fencing_token,  " + 
    //             "        :nic_id,  " + 
    //             "        :nic_garp_info_do_json ; END;"



    m_timer.start ();

    m_stmt_put_garp.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_garp.setObject (2, ls_id);
    m_stmt_put_garp.setObject (3, ls_fencing_token);

    m_stmt_put_garp.setObject (4, nic_id);
    m_stmt_put_garp.setObject (5, nic_garp_info_do_json);

    m_stmt_put_garp.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_garp.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_garp.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_garp_info elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_garp_info", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_garp_info updated_rows: " + updated_rows); 



    return;
  }




  // ----------------------------------------
  // del_nsg_assoc ()
  private void del_nsg_assoc () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object nsg_id = "0";
    Object nic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_nsg_associations input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select random row from NSG_ASSOCIATIONS_DATA
    row = m_manager.getCachedRow (m_threadName, "NSG_ASSOCIATIONS_DATA");
    nsg_id = row.get ("NSG_ID");
    nic_id = row.get ("nic_ID"); 



    m_manager.log (m_threadName, "Calling del_nsg_associations ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_nsg_associations (:lease_valid, :ls_id, :ls_fencing_token, :nsg_id, :nic_id); END;"
      
    m_timer.start ();

    m_stmt_del_nsg_assoc.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_nsg_assoc.setObject (2, ls_id);
    m_stmt_del_nsg_assoc.setObject (3, ls_fencing_token);
    m_stmt_del_nsg_assoc.setObject (4, nsg_id);
    m_stmt_del_nsg_assoc.setObject (5, nic_id);

    m_stmt_del_nsg_assoc.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_nsg_assoc.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_nsg_assoc.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_nsg_associations elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_nsg_associations", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_nsg_associations lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_nsg_assoc ()
  private void put_nsg_assoc () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_nsg_associations (" + //
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :nsg_id, " +
    //             "        :nic_id, " +
    //             "        :association_time, " +
    //             "        :nsg_assc_do_json)  ; END;"

    Object nsg_id = "0";
    Object nic_id = "0";
    Object association_time = null;
    Object nsg_assc_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_nsg_associations input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");



    // select a random row from NSG_ASSOCIATIONS_DATA
    row = m_manager.getCachedRow (m_threadName, "NSG_ASSOCIATIONS_DATA");
    nsg_id = row.get ("NSG_ID"); 
    nic_id = row.get ("nic_ID"); 
    association_time = row.get ("ASSOCIATION_TIME"); 
    nsg_assc_do_json = row.get ("DO_JSON");  



    m_manager.log (m_threadName, "Calling put_nsg_associations ...");

    // "BEGIN put_nsg_associations (" + //
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :nsg_id, " +
    //             "        :nic_id, " +
    //             "        :association_time, " +
    //             "        :nsg_assc_do_json)  ; END;"



    m_timer.start ();

    m_stmt_put_nsg_assoc.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_nsg_assoc.setObject (2, ls_id);
    m_stmt_put_nsg_assoc.setObject (3, ls_fencing_token);

    m_stmt_put_nsg_assoc.setObject (4, nsg_id);
    m_stmt_put_nsg_assoc.setObject (5, nic_id);

    m_stmt_put_nsg_assoc.setObject (6, association_time);    
    m_stmt_put_nsg_assoc.setObject (7, nsg_assc_do_json);


    m_stmt_put_nsg_assoc.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_nsg_assoc.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_nsg_assoc.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_nsg_associations elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_nsg_associations", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_nsg_associations updated_rows: " + 
      updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_float_pvt_ips ()
  private void del_float_pvt_ips () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object fpip_id = "0";


    
    m_manager.log (m_threadName, "Preparing del_floating_pvt_ips input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from FLOATING_PVT_IPS_DATA
    fpip_id = m_manager.getCachedValue (m_threadName, "FLOATING_PVT_IPS_DATA", "ID");
   




    m_manager.log (m_threadName, "Calling del_floating_pvt_ips ...");

    // Prepare to call PLSQL:  
    // "BEGIN del_floating_pvt_ips (" +
    //      "  :lease_valid, " +
    //      "  :ls_id, " +
    //      "  :ls_fencing_token, " +
    //      "  :fpip_id )  ; END;"

    m_timer.start ();

    m_stmt_del_float_pvt_ips.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_pvt_ips.setObject (2, ls_id);
    m_stmt_del_float_pvt_ips.setObject (3, ls_fencing_token);
    m_stmt_del_float_pvt_ips.setObject (4, fpip_id);

    m_stmt_del_float_pvt_ips.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_pvt_ips.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_pvt_ips.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_pvt_ips elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_pvt_ips", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_pvt_ips lease valid: " + 
      lease_valid); 

    return;
  }



  // ----------------------------------------
  // put_float_pvt_ips ()
  private void put_float_pvt_ips () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_floating_pvt_ips ( " +
    //       ":updated_rows, " +
    //       ":ls_id, " +
    //       ":ls_fencing_token, " +
    //       ":fpip_id, " +
    //       ":fpip_subnet_id, " +
    //       ":fpip_ip_address_int, " +
    //       ":fpip_time_created, " +
    //       ":fpip_display_name, " +
    //       ":fpip_lan__id, " +
    //       ":fpip_do_json); END;"

    Object fpip_id = "0";
    Object fpip_subnet_id = "0";
    Object fpip_ip_address_int = 0;
    Object fpip_time_created = null;
    Object fpip_display_name = "0";
    Object fpip_lan__id = "0";
    Object fpip_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_floating_pvt_ips input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_PVT_IPS_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_PVT_IPS_DATA");

    fpip_id = row.get ("ID"); 
    fpip_subnet_id = row.get ("SUBNET_ID"); 
    fpip_ip_address_int = row.get ("IP_ADDRESS_INT"); 
    fpip_time_created = row.get ("TIME_CREATED"); 
    fpip_display_name = row.get ("DISPLAY_NAME"); 
    fpip_lan__id = row.get ("lan__ID"); 
    fpip_do_json = row.get ("DO_JSON");      



    m_manager.log (m_threadName, "Calling put_floating_pvt_ips ...");

    // "BEGIN put_floating_pvt_ips ( " +
    //       ":updated_rows, " +
    //       ":ls_id, " +
    //       ":ls_fencing_token, " +
    //       ":fpip_id, " +
    //       ":fpip_subnet_id, " +
    //       ":fpip_ip_address_int, " +
    //       ":fpip_time_created, " +
    //       ":fpip_display_name, " +
    //       ":fpip_lan__id, " +
    //       ":fpip_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_pvt_ips.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_pvt_ips.setObject (2, ls_id);
    m_stmt_put_float_pvt_ips.setObject (3, ls_fencing_token);

    m_stmt_put_float_pvt_ips.setObject (4, fpip_id);
    m_stmt_put_float_pvt_ips.setObject (5, fpip_subnet_id);

    m_stmt_put_float_pvt_ips.setObject (6, fpip_ip_address_int);
    m_stmt_put_float_pvt_ips.setObject (7, fpip_time_created); 
    
    m_stmt_put_float_pvt_ips.setObject (8, fpip_display_name);
    m_stmt_put_float_pvt_ips.setObject (9, fpip_lan__id);

    m_stmt_put_float_pvt_ips.setObject (10, fpip_do_json);


    m_stmt_put_float_pvt_ips.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_pvt_ips.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_pvt_ips.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_pvt_ips elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_pvt_ips", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_pvt_ips updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_float_pvt_ip_atts ()
  private void del_float_pvt_ip_atts () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object fpipatt_id = "0";


    
    m_manager.log (m_threadName, "Preparing del_floating_pvt_ip_atts input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from FLOATING_PVT_IP_ATTS_DATA
    fpipatt_id = m_manager.getCachedValue (m_threadName, "FLOATING_PVT_IP_ATTS_DATA", "ID");



    m_manager.log (m_threadName, "Calling del_floating_pvt_ip_atts ...");

    // Prepare to call PLSQL:  
    // "BEGIN ; del_floating_pvt_ip_atts (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_id); END;"

    m_timer.start ();

    m_stmt_del_float_pvt_ip_atts.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_pvt_ip_atts.setObject (2, ls_id);
    m_stmt_del_float_pvt_ip_atts.setObject (3, ls_fencing_token);
    m_stmt_del_float_pvt_ip_atts.setObject (4, fpipatt_id);

    m_stmt_del_float_pvt_ip_atts.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_pvt_ip_atts.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_pvt_ip_atts.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_pvt_ip_atts elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_pvt_ip_atts", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_pvt_ip_atts lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_float_pvt_ip_atts ()
  private void put_float_pvt_ip_atts () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_floating_pvt_ip_atts (" + 
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_id, " +
    //             "        :fpipatt_fpip_id, " +
    //             "        :fpipatt_id, " +
    //             "        :fpipatt_nat_ip_addr, " +
    //             "        :fpiatt_lifecycle_state, " +
    //             "        :fpipatt_do_json); END;"

    Object fpipatt_id = "0";
    Object fpipatt_fpip_id = "0";
    Object fpipatt_id = "0";
    Object fpipatt_nat_ip_addr = "0";
    Object fpiatt_lifecycle_state = "0";
    Object fpipatt_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_floating_pvt_ip_atts input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_PVT_IP_ATTS_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_PVT_IP_ATTS_DATA");

    fpipatt_id = row.get ("ID"); 
    fpipatt_fpip_id = row.get ("FLOATING_PRIVATE_IP_ID");
    fpipatt_id = row.get ("nic_ID");
    fpipatt_nat_ip_addr = row.get ("NAT_IP_ADDRESS");
    fpiatt_lifecycle_state = row.get ("LIFECYCLE_STATE"); 
    fpipatt_do_json = row.get ("DO_JSON");   
    





    m_manager.log (m_threadName, "Calling put_floating_pvt_ip_atts ...");

    // "BEGIN put_floating_pvt_ip_atts (" + 
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_id, " +
    //             "        :fpipatt_fpip_id, " +
    //             "        :fpipatt_id, " +
    //             "        :fpipatt_nat_ip_addr, " +
    //             "        :fpiatt_lifecycle_state, " +
    //             "        :fpipatt_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_pvt_ip_atts.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_pvt_ip_atts.setObject (2, ls_id);
    m_stmt_put_float_pvt_ip_atts.setObject (3, ls_fencing_token);

    m_stmt_put_float_pvt_ip_atts.setObject (4, fpipatt_id);
    m_stmt_put_float_pvt_ip_atts.setObject (5, fpipatt_fpip_id);

    m_stmt_put_float_pvt_ip_atts.setObject (6, fpipatt_id);
    m_stmt_put_float_pvt_ip_atts.setObject (7, fpipatt_nat_ip_addr);

    m_stmt_put_float_pvt_ip_atts.setObject (8, fpiatt_lifecycle_state);
    m_stmt_put_float_pvt_ip_atts.setObject (9, fpipatt_do_json);


    m_stmt_put_float_pvt_ip_atts.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_pvt_ip_atts.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_pvt_ip_atts.getInt (1);

    // done
    m_conn.commit ();



    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_pvt_ip_atts elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_pvt_ip_atts", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_pvt_ip_atts updated_rows: " + 
      updated_rows); 



    return;
  }

 
  // ----------------------------------------
  // del_float_ips ()
  private void del_float_ips () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    // "BEGIN del_floating_ips " +
    // "        :lease_valid, " +
    // "        :ls_id,  " +
    // "        :ls_fencing_token,  " +
    // "        :fip_id); END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object fip_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_floating_ips input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from FLOATING_IPS_DATA
    fip_id = m_manager.getCachedValue (m_threadName, "FLOATING_IPS_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_floating_ips ...");

    // "BEGIN del_floating_ips " +
    // "        :lease_valid, " +
    // "        :ls_id,  " +
    // "        :ls_fencing_token,  " +
    // "        :fip_id); END;"

    m_timer.start ();

    m_stmt_del_float_ips.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_ips.setObject (2, ls_id);
    m_stmt_del_float_ips.setObject (3, ls_fencing_token);
    m_stmt_del_float_ips.setObject (4, fip_id);

    m_stmt_del_float_ips.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_ips.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_ips.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_ips elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_ips", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_ips lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_float_ips ()
  private void put_float_ips () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;


    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_floating_ips ( " +
    //       " :updated_rows " +
    //       " :ls_id " +
    //       " :ls_fencing_token " +
    //       " :fip_id " +
    //       " :fip_ip_addr " +
    //       " :fip_com_id " +
    //       " :fip_scope " +
    //       " :fip_availability_domain " +
    //       " :fip_lifetime " +
    //       " :fip_public_ip_pool_id " +
    //       " :fip_do_json); END;"

    Object fip_id = "0";
    Object fip_ip_addr = "0";
    Object fip_com_id = "0";
    Object fip_scope = "0";
    Object fip_availability_domain = "0";
    Object fip_lifetime = "0";
    Object fip_public_ip_pool_id = "0";
    Object fip_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_floating_ips input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_IPS_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_IPS_DATA");

    fip_id = row.get ("ID"); 
    fip_ip_addr = row.get ("IP_ADDRESS");
    fip_com_id = row.get ("com_ID");
    fip_scope = row.get ("SCOPE");
    fip_availability_domain = row.get ("AVAILABILITY_DOMAIN"); 
    fip_lifetime = row.get ("LIFETIME");
    fip_public_ip_pool_id = row.get ("PUBLIC_IP_POOL_ID");
    fip_do_json = row.get ("DO_JSON");



    m_manager.log (m_threadName, "Calling put_floating_ips ...");

    // "BEGIN put_floating_ips ( " +
    //       " :updated_rows " +
    //       " :ls_id " +
    //       " :ls_fencing_token " +
    //       " :fip_id " +
    //       " :fip_ip_addr " +
    //       " :fip_com_id " +
    //       " :fip_scope " +
    //       " :fip_availability_domain " +
    //       " :fip_lifetime " +
    //       " :fip_public_ip_pool_id " +
    //       " :fip_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_ips.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_ips.setObject (2, ls_id);
    m_stmt_put_float_ips.setObject (3, ls_fencing_token);

    m_stmt_put_float_ips.setObject (4, fip_id);
    m_stmt_put_float_ips.setObject (5, fip_ip_addr);

    m_stmt_put_float_ips.setObject (6, fip_com_id);
    m_stmt_put_float_ips.setObject (7, fip_scope);

    m_stmt_put_float_ips.setObject (8, fip_availability_domain);
    m_stmt_put_float_ips.setObject (9, fip_lifetime);

    m_stmt_put_float_ips.setObject (10, fip_public_ip_pool_id);
    m_stmt_put_float_ips.setObject (11, fip_do_json);


    m_stmt_put_float_ips.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_ips.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_ips.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_ips elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_ips", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_ips updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // put_cache_status ()
  private void put_cache_status () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN ; put_cache_status (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :cs_id, " +
    //             "        :cs_cursor, " +
    //             "        :cs_commit_txn_id, " +
    //             "        :cs_status, " +
    //             "        :cs_buckets) END;"

    Object cs_id = "0";
    Object cs_cursor = "0";
    Object cs_commit_txn_id = 0;
    Object cs_status = "0";
    Object cs_buckets = "0";


    m_manager.log (m_threadName, "Preparing put_cache_status input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from CACHE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "CACHE_STATUS_DATA");

    cs_id = row.get ("ID"); 
    cs_cursor = row.get ("CURSOR"); 
    cs_commit_txn_id = row.get ("COMMIT_TXN_ID"); 
    cs_status = row.get ("STATUS"); 
    cs_buckets = row.get ("BUCKETS"); 







    m_manager.log (m_threadName, "Calling put_cache_status ...");

    // "BEGIN ; put_cache_status (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :cs_id, " +
    //             "        :cs_cursor, " +
    //             "        :cs_commit_txn_id, " +
    //             "        :cs_status, " +
    //             "        :cs_buckets) END;"



    m_timer.start ();

    m_stmt_put_cache_status.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_cache_status.setObject (2, ls_id);
    m_stmt_put_cache_status.setObject (3, ls_fencing_token);

    m_stmt_put_cache_status.setObject (4, cs_id);
    m_stmt_put_cache_status.setObject (5, cs_cursor);

    m_stmt_put_cache_status.setObject (6, cs_commit_txn_id);
    m_stmt_put_cache_status.setObject (7, cs_status);

    m_stmt_put_cache_status.setObject (8, cs_buckets);

    m_stmt_put_cache_status.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_cache_status.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_cache_status.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_cache_status elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_cache_status", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_cache_status updated_rows: " + 
      updated_rows); 



    return;
  }


 
  // ----------------------------------------
  // del_float_ip_atts ()
  private void del_float_ip_atts () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    // "BEGIN del_floating_ip_atts (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fipatt_id); END;"

    Object lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object fipatt_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_floating_ip_atts input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random id from FLOATING_IP_ATTS_DATA
    fipatt_id = m_manager.getCachedValue(m_threadName, "FLOATING_IP_ATTS_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_floating_ip_atts ...");

    // "BEGIN del_floating_ip_atts (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fipatt_id); END;"

    m_timer.start ();

    m_stmt_del_float_ip_atts.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_ip_atts.setObject (2, ls_id);
    m_stmt_del_float_ip_atts.setObject (3, ls_fencing_token);
    m_stmt_del_float_ip_atts.setObject (4, fipatt_id);

    m_stmt_del_float_ip_atts.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_ip_atts.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_float_ip_atts.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_ip_atts elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_ip_atts", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_ip_atts lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_float_ip_atts ()
  private void put_float_ip_atts () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_floating_ip_atts (" +
    //       "   updated_rows, " +
    //       "   ls_id, " +
    //       "   ls_fencing_token, " +
    //       "   fipatt_id, " +
    //       "   fipatt_id, " +
    //       "   fipatt_floating_ip_id, " +
    //       "   fipatt_floating_private_ip_id, " +
    //       "   fipatt_assigned_entity_id, " +
    //       "   fipatt_lifecycle_state, " +
    //       "   fipatt_do_json); END;"

    Object fipatt_id = "0";
    Object fipatt_id = "0";
    Object fipatt_floating_ip_id = "0";
    Object fipatt_floating_private_ip_id = "0";
    Object fipatt_assigned_entity_id = "0";
    Object fipatt_lifecycle_state = "0";
    Object fipatt_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_floating_ip_atts input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_IP_ATTS_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_IP_ATTS_DATA");

    fipatt_id = row.get ("ID"); 
    fipatt_id = row.get ("nic_ID"); 
    fipatt_floating_ip_id = row.get ("FLOATING_IP_ID"); 
    fipatt_lifecycle_state = row.get ("LIFECYCLE_STATE"); 
    fipatt_floating_private_ip_id = row.get ("FLOATING_PRIVATE_IP_ID"); 
    fipatt_assigned_entity_id = row.get ("ASSIGNED_ENTITY_ID"); 
    fipatt_do_json = row.get ("DO_JSON"); 




    m_manager.log (m_threadName, "Calling put_floating_ip_atts ...");

    // "BEGIN put_floating_ip_atts (" +
    //       "   updated_rows, " +
    //       "   ls_id, " +
    //       "   ls_fencing_token, " +
    //       "   fipatt_id, " +
    //       "   fipatt_id, " +
    //       "   fipatt_floating_ip_id, " +
    //       "   fipatt_floating_private_ip_id, " +
    //       "   fipatt_assigned_entity_id, " +
    //       "   fipatt_lifecycle_state, " +
    //       "   fipatt_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_ip_atts.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_ip_atts.setObject (2, ls_id);
    m_stmt_put_float_ip_atts.setObject (3, ls_fencing_token);

    m_stmt_put_float_ip_atts.setObject (4, fipatt_id);
    m_stmt_put_float_ip_atts.setObject (5, fipatt_id);

    m_stmt_put_float_ip_atts.setObject (6, fipatt_floating_ip_id);
    m_stmt_put_float_ip_atts.setObject (7, fipatt_floating_private_ip_id);

    m_stmt_put_float_ip_atts.setObject (8, fipatt_assigned_entity_id);

    m_stmt_put_float_ip_atts.setObject (9, fipatt_lifecycle_state);
    m_stmt_put_float_ip_atts.setObject (10, fipatt_do_json);

    
    m_stmt_put_float_ip_atts.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_ip_atts.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_float_ip_atts.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_ip_atts elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_ip_atts", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_ip_atts updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_nets ()
  private void del_nets () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;


    // "BEGIN del_nets ( " +
    //       " :lease_valid, " +
    //       " :ls_id, " +
    //       " :ls_fencing_token, " +
    //       " :sn_id)    ; END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object sn_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_nets input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from SUBNETS_DATA
    sn_id = m_manager.getCachedValue(m_threadName, "SUBNETS_DATA", "ID");


   



    m_manager.log (m_threadName, "Calling del_nets ...");

    // "BEGIN del_nets ( " +
    //       " :lease_valid, " +
    //       " :ls_id, " +
    //       " :ls_fencing_token, " +
    //       " :sn_id)    ; END;"

    m_timer.start ();

    m_stmt_del_nets.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_nets.setObject (2, ls_id);
    m_stmt_del_nets.setObject (3, ls_fencing_token);
    m_stmt_del_nets.setObject (4, sn_id);

    m_stmt_del_nets.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_nets.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_nets.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_nets elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_nets", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_nets lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_nets ()
  private void put_nets () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_nets (" +
    //           ":updated_rows, " +
    //           ":ls_id, " +
    //           ":ls_fencing_token, " +
    //           ":sn_id, " +
    //           ":sn_com_id, " +
    //           ":sn_cn_id, " +
    //           ":sn_route_table_id, " +
    //           ":sn_dhcp_options_id, " +
    //           ":sn_dns_label, " +
    //           ":sn_display_name, " +
    //           ":sn_time_created, " +
    //           ":sn_do_json)   ; END;"

    Object sn_id = "0";
    Object sn_com_id = "0";
    Object sn_cn_id = "0";
    Object sn_route_table_id = "0";
    Object sn_dhcp_options_id = "0";
    Object sn_dns_label = "0";
    Object sn_display_name = "0";
    Object sn_time_created = null;
    Object sn_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_nets input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from SUBNETS_DATA
    row = m_manager.getCachedRow (m_threadName, "SUBNETS_DATA");

    sn_id = row.get ("ID"); 
    sn_com_id = row.get ("com_ID"); 
    sn_cn_id = row.get ("cn_ID"); 
    sn_route_table_id = row.get ("ROUTE_TABLE_ID"); 
    sn_dhcp_options_id = row.get ("DHCP_OPTIONS_ID"); 
    sn_dns_label = row.get ("DNS_LABEL"); 
    sn_display_name = row.get ("DISPLAY_NAME"); 
    sn_time_created = row.get ("TIME_CREATED"); 
    sn_do_json = row.get ("DO_JSON");  




    m_manager.log (m_threadName, "Calling put_nets ...");

    // "BEGIN put_nets (" +
    //           ":updated_rows, " +
    //           ":ls_id, " +
    //           ":ls_fencing_token, " +
    //           ":sn_id, " +
    //           ":sn_com_id, " +
    //           ":sn_cn_id, " +
    //           ":sn_route_table_id, " +
    //           ":sn_dhcp_options_id, " +
    //           ":sn_dns_label, " +
    //           ":sn_display_name, " +
    //           ":sn_time_created, " +
    //           ":sn_do_json)   ; END;"



    m_timer.start ();

    m_stmt_put_nets.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_nets.setObject (2, ls_id);
    m_stmt_put_nets.setObject (3, ls_fencing_token);

    m_stmt_put_nets.setObject (4, sn_id);
    m_stmt_put_nets.setObject (5, sn_com_id);

    m_stmt_put_nets.setObject (6, sn_cn_id);
    m_stmt_put_nets.setObject (7, sn_route_table_id);

    m_stmt_put_nets.setObject (8, sn_dhcp_options_id);

    m_stmt_put_nets.setObject (9, sn_dns_label);
    m_stmt_put_nets.setObject (10, sn_display_name);

    m_stmt_put_nets.setObject (11, sn_time_created);
    m_stmt_put_nets.setObject (12, sn_do_json);


    
    m_stmt_put_nets.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_nets.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_nets.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_nets elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_nets", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_nets updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_net_sec_group ()
  private void del_net_sec_group () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    // "BEGIN del_net_sec_group (" +
    //                 "        :lease_valid, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id); END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nsg_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_net_sec_group input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from net_sec_GROUP_DAT
    nsg_id = m_manager.getCachedValue(m_threadName, "net_sec_GROUP_DAT", "ID");

   



    m_manager.log (m_threadName, "Calling del_net_sec_group ...");

    // "BEGIN del_net_sec_group (" +
    //                 "        :lease_valid, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id); END;"

    m_timer.start ();

    m_stmt_del_net_sec_group.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_net_sec_group.setObject (2, ls_id);
    m_stmt_del_net_sec_group.setObject (3, ls_fencing_token);
    m_stmt_del_net_sec_group.setObject (4, nsg_id);

    m_stmt_del_net_sec_group.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_net_sec_group.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the lease status
    if (!warningFlag) 
      lease_valid = m_stmt_del_net_sec_group.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_net_sec_group elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_net_sec_group", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_net_sec_group lease valid: " + lease_valid); 

    return;
  }
  
// ----------------------------------------
  // put_net_sec_group ()
  private void put_net_sec_group () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN ; put_net_sec_group (" +
    //                 "        :updated_rows, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id, " +
    //                 "        :nsg_dp_id, " +
    //                 "        :nsg_com_id, " +
    //                 "        :nsg_cn_id, " +
    //                 "        :nsg_display_name, " +
    //                 "        :nsg_time_created, " +
    //                 "        :nsg_do_json) END;"

    Object nsg_id = "0";
    Object nsg_dp_id = "0";
    Object nsg_com_id = "0";
    Object nsg_cn_id = "0";
    Object nsg_display_name = "0";
    Object nsg_time_created = null;
    Object nsg_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_net_sec_group input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from net_sec_GROUP_DAT
    row = m_manager.getCachedRow (m_threadName, "net_sec_GROUP_DAT");
  
    nsg_id = row.get ("ID"); 
    nsg_dp_id = row.get ("DP_ID"); 
    nsg_com_id = row.get ("com_ID"); 
    nsg_cn_id = row.get ("cn_ID"); 
    nsg_display_name = row.get ("DISPLAY_NAME"); 
    nsg_time_created = row.get ("TIME_CREATED"); 
    nsg_do_json = row.get ("DO_JSON"); 






    m_manager.log (m_threadName, "Calling put_net_sec_group ...");

    // "BEGIN ; put_net_sec_group (" +
    //                 "        :updated_rows, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_id, " +
    //                 "        :nsg_dp_id, " +
    //                 "        :nsg_com_id, " +
    //                 "        :nsg_cn_id, " +
    //                 "        :nsg_display_name, " +
    //                 "        :nsg_time_created, " +
    //                 "        :nsg_do_json) END;"



    m_timer.start ();

    m_stmt_put_net_sec_group.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_net_sec_group.setObject (2, ls_id);
    m_stmt_put_net_sec_group.setObject (3, ls_fencing_token);

    m_stmt_put_net_sec_group.setObject (4, nsg_id);
    m_stmt_put_net_sec_group.setObject (5, nsg_dp_id);

    m_stmt_put_net_sec_group.setObject (6, nsg_com_id);
    m_stmt_put_net_sec_group.setObject (7, nsg_cn_id);

    m_stmt_put_net_sec_group.setObject (8, nsg_display_name);

    m_stmt_put_net_sec_group.setObject (9, nsg_time_created);
    m_stmt_put_net_sec_group.setObject (10, nsg_do_json);


    
    m_stmt_put_net_sec_group.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_net_sec_group.getWarnings()) != null) 
    {
      do 
      {
        warningFlag = true;
        m_manager.log (m_threadName, "Warning: " + wn);

        wn = wn.getNextWarning();
      } while (wn != null);
    }

    // Get the update status
    if (!warningFlag) 
      updated_rows = m_stmt_put_net_sec_group.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_net_sec_group elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_net_sec_group", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_net_sec_group updated_rows: " + 
      updated_rows); 



    return;
  }

  // ----------------------------------------
  // query_get ()
  private void query_get () throws SQLException
  {
    // locals
    ResultSet rs;

    // "SELECT do_json FROM VNICS WHERE id = ?"

    Object nic_id = "0";
    String nic_do_json = "0";


    m_manager.log (m_threadName, "Preparing query_get input ...");
    
    // select a random id from VNICS_DATA
    nic_id = m_manager.getCachedValue (m_threadName, "VNICS_DATA", "ID");


    m_manager.log (m_threadName, "Calling query_get ...");

    m_timer.start ();

    m_stmt_query_get.setObject (1, nic_id);
    rs = m_stmt_query_get.executeQuery();

    while (rs.next ())
    {
      nic_do_json = rs.getString (1);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get do_json: " + nic_do_json); 

    return;
  }



  // ----------------------------------------
  // query_get_net_sec_groups ()
  private void query_get_net_sec_groups () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT NSG.do_json FROM net_sec_GROUP NSG " + 
    // "INNER JOIN NSG_ASSOCIATIONS NSG_ASC " + 
    // "ON NSG.id = NSG_ASC.nsg_id " + 
    // "WHERE NSG_ASC.nic_id = ?"

    Object nic_id = "0";
    String nsg_do_json = "0";


    m_manager.log (m_threadName, "Preparing query_get_net_sec_groups input ...");
  
    // select a random id from the table
    nic_id = m_manager.getCachedValue (m_threadName, "NSG_ASSOCIATIONS_DATA", "nic_ID");

    

    m_manager.log (m_threadName, "Calling query_get_net_sec_groups ...");

    m_timer.start ();

    m_stmt_query_get_net_sec_groups.setObject (1, nic_id);
    rs = m_stmt_query_get_net_sec_groups.executeQuery();

    while (rs.next ())
    {
      nsg_do_json = rs.getString (1);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_net_sec_groups elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_net_sec_groups", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_net_sec_groups do_json: " + nsg_do_json); 

    return;
  }


  // ----------------------------------------
  // query_get_prim_pub_ip ()
  private void query_get_prim_pub_ip () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT FIPs.do_json, FIPAs.do_json FROM FLOATING_IP_ATTS FIPAs " +
    // "INNER JOIN FLOATING_IPS FIPs " +
    // "ON FIPAs.floating_ip_id = FIPs.id " +
    // "WHERE FIPAs.floating_private_ip_id = ? " +
    // "AND FIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"

    Object floating_private_ip_id = "0";
    String fips_do_json = "0";
    String fipas_do_json = "0";

    m_manager.log (m_threadName, "Preparing query_get_prim_pub_ip input ...");
  
    // select random IDs from the table
    floating_private_ip_id = m_manager.getCachedValue (m_threadName, "FLOATING_IP_ATTS_DATA", "FLOATING_PRIVATE_IP_ID");


    m_manager.log (m_threadName, "Calling query_get_prim_pub_ip ...");

    m_timer.start ();

    m_stmt_query_get_prim_pub_ip.setObject (1, floating_private_ip_id);
    rs = m_stmt_query_get_prim_pub_ip.executeQuery();

    while (rs.next ())
    {
      fips_do_json = rs.getString (1);
      fipas_do_json = rs.getString (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_prim_pub_ip elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_prim_pub_ip", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_prim_pub_ip fips_do_json: " + fips_do_json); 
    m_manager.log (m_threadName, "query_get_prim_pub_ip fipas_do_json: " + fipas_do_json); 

    return;
  }


  // ----------------------------------------
  // query_get_prim_priv_ip ()
  private void query_get_prim_priv_ip () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT FPIPs.do_json, FPIPAs.do_json FROM FLOATING_PVT_IP_ATTS FPIPAs " +
    // "INNER JOIN FLOATING_PVT_IPS FPIPs " +
    // "ON FPIPAs.floating_private_ip_id = FPIPs.id " +
    // "WHERE FPIPAs.nic_id = ? " +
    // "AND FPIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"

    Object nic_id = "0";
    String fips_do_json = "0";
    String fipas_do_json = "0";

    m_manager.log (m_threadName, "Preparing query_get_prim_priv_ip input ...");
  
    // select random IDs from the table
    nic_id = m_manager.getCachedValue (m_threadName, "FLOATING_PVT_IP_ATTS_DATA", "nic_ID");




    m_manager.log (m_threadName, "Calling query_get_prim_priv_ip ...");

    m_timer.start ();

    m_stmt_query_get_prim_priv_ip.setObject (1, nic_id); 
    rs = m_stmt_query_get_prim_priv_ip.executeQuery();

    while (rs.next ())
    {
      fips_do_json = rs.getString (1);
      fipas_do_json = rs.getString (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_prim_priv_ip elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_prim_priv_ip", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_prim_priv_ip fips_do_json: " + fips_do_json); 
    m_manager.log (m_threadName, "query_get_prim_priv_ip fipas_do_json: " + fipas_do_json); 

    return;
  }


  // ----------------------------------------
  // query_get_ipv6_addr ()
  private void query_get_ipv6_addr () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT do_json, time_created FROM IPV6S " +
    // "WHERE nic_id = ? " +
    // "AND rownum <= 32 " +
    // "ORDER BY time_created ASC"

    Object nic_id = "0";
    String do_json = "0";
    java.sql.Timestamp time_created = null;

    m_manager.log (m_threadName, "Preparing query_get_ipv6_addr input ...");
  
    // select random IDs from the table
    nic_id = m_manager.getCachedValue (m_threadName, "IPV6S_DATA", "nic_ID");



    m_manager.log (m_threadName, "Calling query_get_ipv6_addr ...");

    m_timer.start ();

    m_stmt_query_get_ipv6_addr.setObject (1, nic_id); 
    rs = m_stmt_query_get_ipv6_addr.executeQuery();

    while (rs.next ())
    {
      do_json = rs.getString (1);
      time_created = rs.getTimestamp (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_ipv6_addr elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_ipv6_addr", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_ipv6_addr do_json: " + do_json); 
    m_manager.log (m_threadName, "query_get_ipv6_addr time_created: " + time_created); 

    return;
  }






  // ----------------------------------------
  // getLeaseStatusData ()
  // Returns a random ID and FENCING_TOKEN from the LEASE_STATUS_DATA
  // table. The first element in the returned array list is ID (String) followed 
  // by FENCING_TOKEN (int).
  private ArrayList<Object> getLeaseStatusData () throws SQLException
  {

    PreparedStatement pstmt = null;
    ResultSet rsRow = null;
    String sql, sqlRows;

    ArrayList<Object> leaseData = new ArrayList<>();


    // select a random row from LEASE_STATUS_DATA
    sqlRows = getRowSelectionClause ("LEASE_STATUS_DATA");
    sql = "SELECT " + sqlRows + " ID, FENCING_TOKEN FROM LEASE_STATUS_DATA";
    m_manager.log (m_threadName, sql);

    pstmt = m_conn.prepareStatement (sql);
    rsRow = pstmt.executeQuery();

    if (rsRow.next ())
    {
      leaseData.add (rsRow.getString (1));
      leaseData.add (rsRow.getInt (2));
    }

    pstmt.close ();

    return leaseData;
  }


  // ----------------------------------------
  // getTableIdData ()
  // 
  private String getTableIdData (String tableName) throws SQLException
  {
    PreparedStatement pstmt;
    ResultSet rsRow;

    String id = "0";
    String sql, sqlRows;

    // select a random ID from the table
    sqlRows = getRowSelectionClause (tableName);
    sql = "SELECT " + sqlRows + " ID FROM " + tableName;
    m_manager.log (m_threadName, sql);

    pstmt = m_conn.prepareStatement (sql);
    rsRow = pstmt.executeQuery();

    if (rsRow.next ())
    {
      id = rsRow.getString (1);
    }

    pstmt.close ();
   
    return id;
  }

  // ----------------------------------------
  // getRowSelectionClause ()
  // Returns a random "ROWS N TO M" clause based on the table row count
  private String getRowSelectionClause (String tableName)
  {
    String rowSelectionClause = null;
    int rowCount = 0;
    int row = 0;

    rowCount = m_manager.getTableRowCount (tableName);

    if (rowCount != 0)
      row = m_rand.nextInt (rowCount) + 1;

    rowSelectionClause = "ROWS " + row + " TO " + row;


    return rowSelectionClause;
  }
 

  
  // ----------------------------------------
  // getStatus ()
  // Returns the test status of this thread
  public boolean getStatus ()
  {
    return m_status;
  }


  // ----------------------------------------
  // getReady ()
  // Returns true if the thread is ready to run
  public boolean getReady ()
  {
    return m_ready;
  }  

  // ----------------------------------------
  // setStart ()
  // Indicate that the thread ready to start running
  public void setStart ()
  {
    m_start = true;
    return;
  }

  private int getRandomIndex(double[] probabilities, Random random) 
  {
    double r = random.nextDouble();
    double cumulativeProbability = 0;

    for (int i = 0; i < probabilities.length; i++) {

        cumulativeProbability += probabilities[i];

        if (r <= cumulativeProbability) {
            return i;
        }
    }

    return probabilities.length - 1;
  }



}


// Measure in microseconds
class TTTimer {

  public static String curTime() {
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
      LocalTime localTime = LocalTime.now();
      return dtf.format(localTime);    // 16:37:15
  }
  
  private long startTime = -1;
  private long endTime = -1;
  
  public long start() {

      startTime = System.nanoTime() / 1000;

      return startTime;
  }

  public long stop() {
      
      endTime = System.nanoTime() / 1000;

      return endTime;
  }

  public long getCurrent() {
      return System.nanoTime() / 1000;
  }

  public long getElapsed() {
      if ( startTime <= 0  )
          return 0;
      else
          return ((System.nanoTime() / 1000) - startTime);

  }

  public long getTimeInUs() {

    if((startTime == -1) || (endTime == -1)) {
        return -1;
    }
    else if((endTime == startTime)) {
        return 1;
    }
    else
        return (endTime - startTime);

  }
}


