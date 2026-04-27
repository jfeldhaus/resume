// VCNThread.java



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



public class VCNThread extends Thread
{
  
  // ATTRIBUTES
  private Connection m_conn = null;
  private VCNManager m_manager;

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

  private CallableStatement m_stmt_del_vnics_rgn;
  private CallableStatement m_stmt_put_vnics_rgn;

  private CallableStatement m_stmt_del_ipv6s_rgn;
  private CallableStatement m_stmt_put_ipv6s_rgn;

  private CallableStatement m_stmt_del_vlans_rgn; 
  private CallableStatement m_stmt_put_vlans_rgn;

  private CallableStatement m_stmt_del_sec_lists_rgn; 
  private CallableStatement m_stmt_put_sec_lists_rgn;

  private CallableStatement m_stmt_del_vcns_rgn;
  private CallableStatement m_stmt_put_vcns_rgn;

  private CallableStatement m_stmt_del_vnic_garp_rgn;
  private CallableStatement m_stmt_put_vnic_garp_rgn;

  private CallableStatement m_stmt_del_nsg_assoc_rgn;
  private CallableStatement m_stmt_put_nsg_assoc_rgn;

  private CallableStatement m_stmt_del_float_pvt_ips_rgn;
  private CallableStatement m_stmt_put_float_pvt_ips_rgn;

  private CallableStatement m_stmt_del_float_pvt_ip_atts_rgn;
  private CallableStatement m_stmt_put_float_pvt_ip_atts_rgn;

  private CallableStatement m_stmt_del_float_ips_rgn;
  private CallableStatement m_stmt_put_float_ips_rgn;

  private CallableStatement m_stmt_put_cache_status;

  private CallableStatement m_stmt_del_float_ip_atts_rgn;
  private CallableStatement m_stmt_put_float_ip_atts_rgn;

  private CallableStatement m_stmt_del_subnets_rgn;
  private CallableStatement m_stmt_put_subnets_rgn;

  private CallableStatement m_stmt_del_net_sec_group_rgn;
  private CallableStatement m_stmt_put_net_sec_group_rgn;

  private CallableStatement m_stmt_query_get_vnic;
  private CallableStatement m_stmt_query_get_net_sec_groups;
  private CallableStatement m_stmt_query_get_prim_pub_ip;
  private CallableStatement m_stmt_query_get_prim_priv_ip;

  private CallableStatement m_stmt_query_get_ipv6_addr_vnic;

  // OPERATIONS
  
  // ----------------------------------------
  // VCNThread ()
  public VCNThread (VCNManager manager, int threadId, int txns,
    LinkedHashMap<String, Integer> txnMap,
    LinkedHashMap<String, Integer> txnProbsMap)
  {
    
    // initialize the thread name with the xid data
    super ("VCNThread");
    
    // initialize attributes
    m_manager = manager;
    m_threadId = threadId;
    m_txns = txns;

    m_threadName = "VCNThread " + m_threadId;
    m_rand = new Random ();

    m_timer = new TTTimer ();   


    // the transaction list and probabilties
    m_txnMap = txnMap;
    m_txnProbsMap = txnProbsMap;


    // the status of the branch is 'success' unless something
    // unexpected occurs
    m_status = true;
  
    // report the creation of this TxnBranch
    m_manager.log (m_threadName, "VCNThread created.");
    
    return;
  }
  

  
  // ----------------------------------------
  // checkoutConn ()
  // Checks a connection out from the VCNManager
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
  // Checks a connection back into the VCNManager
  private void checkinConn () throws SQLException
  {
    m_conn.setAutoCommit (true);
    m_manager.checkinConn (m_threadName, m_conn);    
    m_conn = null;
 
    return;
  }
  
  
  // ----------------------------------------
  // run ()
  // Execution of the VCNThread begins here
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
            // VNICS_RGN_DATA
            case 16:
              del_vnics_rgn ();
              break;
            
            case 26:
              put_vnics_rgn ();
              break;

            // IPV6S_RGN
            case 7:
              del_ipv6s_rgn ();
              break;

            case 14:
              put_ipv6s_rgn ();
              break;

            // VLANS_RGN
            case 9:
              del_vlans_rgn ();
              break;
            
            case 22:
              put_vlans_rgn ();
              break;

            // SECURITY_LISTS_REGION
            case 11:
              del_sec_lists_rgn ();
              break;
              
            case 21:
              put_sec_lists_rgn ();
              break;

            // VCNS_RGN
            case 4:
              del_vcns_rgn ();
              break;

            case 18:
              put_vcns_rgn ();
              break;

            // VNIC_GARP_INFO_RGN
            case 3:
              del_vnic_garp_rgn ();
              break;
            
            case 15:
              put_vnic_garp_rgn ();
              break;

            // NSG_ASSOCIATIONS_RGN
            case 12:
              del_nsg_assoc_rgn ();
              break;

            case 13:  
              put_nsg_assoc_rgn ();
              break;

            // FLOATING_PVT_IPS_RGN
            case 6:
              del_float_pvt_ips_rgn ();
              break;

            case 25:  
              put_float_pvt_ips_rgn ();
              break;

            // FLOATING_PVT_IP_ATTS_RGN
            case 5:
              del_float_pvt_ip_atts_rgn ();
              break;

            case 17:
              put_float_pvt_ip_atts_rgn ();
              break;

            // FLOATING_IPS_RGN
            case 2:
              del_float_ips_rgn ();
              break;

            case 20:
              put_float_ips_rgn ();
              break;

            // CACHE_STATUS
            case 27:
              put_cache_status ();
              break;

            // FLOATING_IP_ATTS_RGN
            case 8:
              del_float_ip_atts_rgn ();
              break;
            
            case 19:  
              put_float_ip_atts_rgn ();
              break;

            // SUBNETS_RGN
            case 1:
              del_subnets_rgn ();
              break;

            case 24:  
              put_subnets_rgn ();
              break;

            // NETWORK_SECURITY_GROUP_RGN
            case 10:
              del_net_sec_group_rgn ();
              break;
            
            case 23:
              put_net_sec_group_rgn ();
              break;
            

            // queries
            case 31:
              query_get_vnic ();
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
              query_get_ipv6_addr_vnic ();
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
    closeStatement (m_stmt_del_vnics_rgn);
    closeStatement (m_stmt_put_vnics_rgn);
    closeStatement (m_stmt_del_ipv6s_rgn);
    closeStatement (m_stmt_put_ipv6s_rgn);
    closeStatement (m_stmt_del_vlans_rgn);
    closeStatement (m_stmt_put_vlans_rgn);
    closeStatement (m_stmt_del_sec_lists_rgn);
    closeStatement (m_stmt_put_sec_lists_rgn);
    closeStatement (m_stmt_del_vcns_rgn);
    closeStatement (m_stmt_put_vcns_rgn);
    closeStatement (m_stmt_del_vnic_garp_rgn);
    closeStatement (m_stmt_put_vnic_garp_rgn);
    closeStatement (m_stmt_del_nsg_assoc_rgn);
    closeStatement (m_stmt_put_nsg_assoc_rgn);
    closeStatement (m_stmt_del_float_pvt_ips_rgn);
    closeStatement (m_stmt_put_float_pvt_ips_rgn);
    closeStatement (m_stmt_del_float_pvt_ip_atts_rgn);
    closeStatement (m_stmt_put_float_pvt_ip_atts_rgn);
    closeStatement (m_stmt_del_float_ips_rgn);
    closeStatement (m_stmt_put_float_ips_rgn);
    closeStatement (m_stmt_put_cache_status);
    closeStatement (m_stmt_del_float_ip_atts_rgn);
    closeStatement (m_stmt_put_float_ip_atts_rgn);
    closeStatement (m_stmt_del_subnets_rgn);
    closeStatement (m_stmt_put_subnets_rgn);
    closeStatement (m_stmt_del_net_sec_group_rgn);
    closeStatement (m_stmt_put_net_sec_group_rgn);
    closeStatement (m_stmt_query_get_vnic);
    closeStatement (m_stmt_query_get_net_sec_groups);
    closeStatement (m_stmt_query_get_prim_pub_ip);
    closeStatement (m_stmt_query_get_prim_priv_ip);
    closeStatement (m_stmt_query_get_ipv6_addr_vnic);

    m_stmt_del_vnics_rgn = null;
    m_stmt_put_vnics_rgn = null;
    m_stmt_del_ipv6s_rgn = null;
    m_stmt_put_ipv6s_rgn = null;
    m_stmt_del_vlans_rgn = null;
    m_stmt_put_vlans_rgn = null;
    m_stmt_del_sec_lists_rgn = null;
    m_stmt_put_sec_lists_rgn = null;
    m_stmt_del_vcns_rgn = null;
    m_stmt_put_vcns_rgn = null;
    m_stmt_del_vnic_garp_rgn = null;
    m_stmt_put_vnic_garp_rgn = null;
    m_stmt_del_nsg_assoc_rgn = null;
    m_stmt_put_nsg_assoc_rgn = null;
    m_stmt_del_float_pvt_ips_rgn = null;
    m_stmt_put_float_pvt_ips_rgn = null;
    m_stmt_del_float_pvt_ip_atts_rgn = null;
    m_stmt_put_float_pvt_ip_atts_rgn = null;
    m_stmt_del_float_ips_rgn = null;
    m_stmt_put_float_ips_rgn = null;
    m_stmt_put_cache_status = null;
    m_stmt_del_float_ip_atts_rgn = null;
    m_stmt_put_float_ip_atts_rgn = null;
    m_stmt_del_subnets_rgn = null;
    m_stmt_put_subnets_rgn = null;
    m_stmt_del_net_sec_group_rgn = null;
    m_stmt_put_net_sec_group_rgn = null;
    m_stmt_query_get_vnic = null;
    m_stmt_query_get_net_sec_groups = null;
    m_stmt_query_get_prim_pub_ip = null;
    m_stmt_query_get_prim_priv_ip = null;
    m_stmt_query_get_ipv6_addr_vnic = null;
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
    
    m_stmt_del_vnics_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_vnics_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vnic_ocid); END;");
    
    m_stmt_put_vnics_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_vnics_rgn (:updated_rows, :ls_id, :ls_fencing_token, " + 
      ":vnic_ocid, :vnic_subnet_id, :vnic_compartment_id, :vnic_vlan_id, " + 
      ":vnic_public_ip, :vnic_overlay_mac, :vnic_resource_id, " + 
      ":vnic_is_vnic_service_vnic, :vnic_do_json); END;"); 
  

    m_stmt_del_ipv6s_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_ipv6s_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vnic_ocid); END;");

    m_stmt_put_ipv6s_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_ipv6s_rgn ( " + 
                "        :updated_rows, " + 
                "        :ls_id, " + 
                "        :ls_fencing_token, " + 
                "        :ipv6_ocid, " + 
                "        :ipv6_vnic_id, " + 
                "        :ipv6_subnet_id, " + 
                "        :ipv6_ip_address, " + 
                "        :ipv6_time_created, " + 
                "        :ipv6_do_json); END;");

    
    m_stmt_del_vlans_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_vlans_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vlan_ocid); END;");

    m_stmt_put_vlans_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_vlans_rgn (" + 
                      " :updated_rows, " +
                      " :ls_id, " +
                      " :ls_fencing_token, " +
                      " :vlan_ocid, " +
                      " :vlan_compartment_id, " +
                      " :vlan_vcn_id, " +
                      " :vlan_route_table_id, " +
                      " :vlan_display_name, " +
                      " :vlan_virtual_router_mac, " +
                      " :vlan_time_created, " +
                      " :vlan_do_json); END;");



    m_stmt_del_sec_lists_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_security_lists_rgn (:lease_valid, :ls_id, :ls_fencing_token, :sl_ocid); END;");


    m_stmt_put_sec_lists_rgn = m_conn.prepareCall
      ("BEGIN  vcn.put_security_lists_rgn (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token,  " +
                "        :sl_ocid, " +
                "        :sl_compartment_id, " +
                "        :sl_vcn_id,  " +
                "        :sl_display_name,  " +
                "        :sl_time_created,  " +
                "        :sl_do_json); END;");


    m_stmt_del_vcns_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_vcns_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vcn_ocid); END;");
          
    m_stmt_put_vcns_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_vcns_rgn (" + 
                "        :updated_rows, " + 
                "        :ls_id, " + 
                "        :ls_fencing_token, " + 
                "        :vcn_ocid, " + 
                "        :default_route_table_ocid, " + 
                "        :default_security_list_ocid, " + 
                "        :default_dhcp_options_ocid, " + 
                "        :compartment_ocid, " + 
                "        :display_name, " + 
                "        :time_created, " + 
                "        :vcn_do_json) ; END;");


    m_stmt_del_vnic_garp_rgn = m_conn.prepareCall 
      ("BEGIN vcn.del_vnic_garp_info_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vnic_id); END;");

    m_stmt_put_vnic_garp_rgn = m_conn.prepareCall 
      ("BEGIN vcn.put_vnic_garp_info_rgn (" + 
                "        :updated_rows, " + 
                "        :ls_id,  " + 
                "        :ls_fencing_token,  " + 
                "        :vnic_id,  " + 
                "        :vnic_garp_info_do_json) ; END;");



    m_stmt_del_nsg_assoc_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_nsg_associations_rgn (:lease_valid, :ls_id, :ls_fencing_token, :nsg_ocid, :vnic_ocid); END;");

    m_stmt_put_nsg_assoc_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_nsg_associations_rgn (" + 
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_ocid, " +
                "        :vnic_ocid, " +
                "        :association_time, " +
                "        :nsg_assc_do_json)  ; END;");


    m_stmt_del_float_pvt_ips_rgn = m_conn.prepareCall
        ("BEGIN vcn.del_floating_pvt_ips_rgn (" +
         "  :lease_valid, " +
         "  :ls_id, " +
         "  :ls_fencing_token, " +
         "  :fpip_ocid )  ; END;");

    m_stmt_put_float_pvt_ips_rgn = m_conn.prepareCall
        ("BEGIN vcn.put_floating_pvt_ips_rgn ( " +
          ":updated_rows, " +
          ":ls_id, " +
          ":ls_fencing_token, " +
          ":fpip_ocid, " +
          ":fpip_subnet_id, " +
          ":fpip_ip_address_int, " +
          ":fpip_time_created, " +
          ":fpip_display_name, " +
          ":fpip_vlan_id, " +
          ":fpip_do_json); END;");



    m_stmt_del_float_pvt_ip_atts_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_floating_pvt_ip_atts_rgn (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fpipatt_ocid); END;");

    m_stmt_put_float_pvt_ip_atts_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_floating_pvt_ip_atts_rgn (" + 
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fpipatt_ocid, " +
                "        :fpipatt_fpip_ocid, " +
                "        :fpipatt_vnic_ocid, " +
                "        :fpipatt_nat_ip_addr, " +
                "        :fpiatt_lifecycle_state, " +
                "        :fpipatt_do_json); END;");


    m_stmt_del_float_ips_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_floating_ips_rgn (" +
                "        :lease_valid, " +
                "        :ls_id,  " +
                "        :ls_fencing_token,  " +
                "        :fip_ocid); END;");

    m_stmt_put_float_ips_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_floating_ips_rgn ( " +
          " :updated_rows, " +
          " :ls_id, " +
          " :ls_fencing_token, " +
          " :fip_ocid, " +
          " :fip_ip_addr, " +
          " :fip_compartment_id, " +
          " :fip_scope, " +
          " :fip_availability_domain, " +
          " :fip_lifetime, " +
          " :fip_public_ip_pool_id, " +
          " :fip_do_json); END;");


    m_stmt_put_cache_status = m_conn.prepareCall
      ("BEGIN vcn.put_cache_status (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :cs_id, " +
                "        :cs_cursor, " +
                "        :cs_commit_txn_id, " +
                "        :cs_status, " +
                "        :cs_buckets); END;");          


    m_stmt_del_float_ip_atts_rgn = m_conn.prepareCall 
      ("BEGIN vcn.del_floating_ip_atts_rgn (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :fipatt_ocid); END;");


    m_stmt_put_float_ip_atts_rgn = m_conn.prepareCall 
      ("BEGIN vcn.put_floating_ip_atts_rgn (" +
      "   :updated_rows, " +
      "   :ls_id, " +
      "   :ls_fencing_token, " +
      "   :fipatt_ocid, " +
      "   :fipatt_vnic_id, " +
      "   :fipatt_floating_ip_id, " +
      "   :fipatt_floating_private_ip_id, " +
      "   :fipatt_assigned_entity_id, " +
      "   :fipatt_lifecycle_state, " +
      "   :fipatt_do_json); END;");

    m_stmt_del_subnets_rgn = m_conn.prepareCall 
      ("BEGIN vcn.del_subnets_rgn ( " +
      " :lease_valid, " +
      " :ls_id, " +
      " :ls_fencing_token, " +
      " :sn_ocid)    ; END;"); 

    m_stmt_put_subnets_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_subnets_rgn (" +
          ":updated_rows, " +
          ":ls_id, " +
          ":ls_fencing_token, " +
          ":sn_ocid, " +
          ":sn_compartment_id, " +
          ":sn_vcn_id, " +
          ":sn_route_table_id, " +
          ":sn_dhcp_options_id, " +
          ":sn_dns_label, " +
          ":sn_display_name, " +
          ":sn_time_created, " +
          ":sn_do_json)   ; END;");


    m_stmt_del_net_sec_group_rgn = m_conn.prepareCall
      ("BEGIN vcn.del_network_security_group_rgn (" +
                "        :lease_valid, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_ocid); END;");

    m_stmt_put_net_sec_group_rgn = m_conn.prepareCall
      ("BEGIN vcn.put_network_security_group_rgn (" +
                "        :updated_rows, " +
                "        :ls_id, " +
                "        :ls_fencing_token, " +
                "        :nsg_ocid, " +
                "        :nsg_dp_id, " +
                "        :nsg_compartment_id, " +
                "        :nsg_vcn_id, " +
                "        :nsg_display_name, " +
                "        :nsg_time_created, " +
                "        :nsg_do_json); END;"); 
    

    m_stmt_query_get_vnic = m_conn.prepareCall
      ("SELECT do_json FROM VNICS_RGN WHERE id = ?");

    m_stmt_query_get_net_sec_groups = m_conn.prepareCall
      ("SELECT NSG.do_json FROM NETWORK_SECURITY_GROUP_RGN NSG " + 
                "INNER JOIN NSG_ASSOCIATIONS_RGN NSG_ASC " + 
                "ON NSG.id = NSG_ASC.nsg_id " + 
                "WHERE NSG_ASC.vnic_id = ?");


    m_stmt_query_get_prim_pub_ip = m_conn.prepareCall
      ("SELECT FIPs.do_json, FIPAs.do_json FROM FLOATING_IP_ATTS_RGN FIPAs " +
        "INNER JOIN FLOATING_IPS_RGN FIPs " +
        "ON FIPAs.floating_ip_id = FIPs.id " +
        "WHERE FIPAs.floating_private_ip_id = ? " +
        "AND FIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"); 

    m_stmt_query_get_prim_priv_ip = m_conn.prepareCall
        ("SELECT FPIPs.do_json, FPIPAs.do_json FROM FLOATING_PVT_IP_ATTS_RGN FPIPAs " +
          "INNER JOIN FLOATING_PVT_IPS_RGN FPIPs " +
          "ON FPIPAs.floating_private_ip_id = FPIPs.id " +
          "WHERE FPIPAs.vnic_id = ? " +
          "AND FPIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')");


    m_stmt_query_get_ipv6_addr_vnic =  m_conn.prepareCall
      ("SELECT do_json, time_created FROM VCN.IPV6S_RGN " +
        "WHERE vnic_id = ? " +
        "AND rownum <= 32 " +
        "ORDER BY time_created ASC");
  

    return;
  }


  
  // ----------------------------------------
  // del_vnics_rgn ()
  private void del_vnics_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object vnic_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_vnics_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random id from VNICS_RGN_DATA
    vnic_ocid = m_manager.getCachedValue (m_threadName, "VNICS_RGN_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_vnics_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_vnics_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vnic_ocid); END;"
      
    m_timer.start ();

    m_stmt_del_vnics_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_vnics_rgn.setObject (2, ls_id);
    m_stmt_del_vnics_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_vnics_rgn.setObject (4, vnic_ocid);

    m_stmt_del_vnics_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_vnics_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_vnics_rgn.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_vnics_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_vnics_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_vnics_rgn lease valid: " + lease_valid); 

    return;
  }
  
  
  // ----------------------------------------
  // put_vnics_rgn ()
  private void put_vnics_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    HashMap<String, Object> row;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object vnic_ocid = "0"; 
    Object vnic_subnet_id = "0"; 
    Object vnic_compartment_id = "0";
    Object vnic_vlan_id = "0"; 
    Object vnic_public_ip = "0"; 
    Object vnic_overlay_mac = 0; 
    Object vnic_resource_id = "0"; 
    Object vnic_is_vnic_service_vnic = 0;
    Object vnic_do_json = "0"; 



    m_manager.log (m_threadName, "Preparing put_vnics_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VNICS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "VNICS_RGN_DATA");

    vnic_ocid = row.get ("ID"); 
    vnic_compartment_id = row.get ("COMPARTMENT_ID");
    vnic_subnet_id = row.get ("SUBNET_ID"); 
    vnic_vlan_id = row.get ("VLAN_ID"); 
    vnic_public_ip = row.get ("PUBLIC_IP"); 
    vnic_overlay_mac = row.get ("OVERLAY_MAC"); 
    vnic_resource_id = row.get ("RESOURCE_ID"); 
    vnic_is_vnic_service_vnic = row.get ("IS_VNIC_SERVICE_VNIC");
    vnic_do_json = row.get ("DO_JSON");






    m_manager.log (m_threadName, "Calling put_vnics_rgn ...");

    // Prepare to call PLSQL: 
    //   "BEGIN vcn.put_vnics_rgn (:updated_rows, :ls_id, :ls_fencing_token, " + 
    //   ":vnic_ocid, :vnic_subnet_id, :vnic_compartment_id, :vnic_vlan_id, " + 
    //   ":vnic_public_ip, :vnic_overlay_mac, :vnic_resource_id, " + 
    //   ":vnic_is_vnic_service_vnic, :vnic_do_json); END;"

    m_timer.start ();

    m_stmt_put_vnics_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_vnics_rgn.setObject (2, ls_id);
    m_stmt_put_vnics_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_vnics_rgn.setObject (4, vnic_ocid);
    m_stmt_put_vnics_rgn.setObject (5, vnic_subnet_id);
    m_stmt_put_vnics_rgn.setObject (6, vnic_compartment_id);
    m_stmt_put_vnics_rgn.setObject (7, vnic_vlan_id);

    m_stmt_put_vnics_rgn.setObject (8, vnic_public_ip);
    m_stmt_put_vnics_rgn.setObject (9, vnic_overlay_mac);
    m_stmt_put_vnics_rgn.setObject (10, vnic_resource_id);

    m_stmt_put_vnics_rgn.setObject (11, vnic_is_vnic_service_vnic);
    m_stmt_put_vnics_rgn.setObject (12, vnic_do_json);


    m_stmt_put_vnics_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_vnics_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_vnics_rgn.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_vnics_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_vnics_rgn", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_vnics_rgn updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_ipv6s_rgn ()
  private void del_ipv6s_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object vnic_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_ipv6s_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

    // select a random ID from IPV6S_RGN_DATA
    vnic_ocid = m_manager.getCachedValue (m_threadName, "IPV6S_RGN_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_ipv6s_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_ipv6s_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vnic_ocid); END;"
      
    m_timer.start ();

    m_stmt_del_ipv6s_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_ipv6s_rgn.setObject (2, ls_id);
    m_stmt_del_ipv6s_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_ipv6s_rgn.setObject (4, vnic_ocid);

    m_stmt_del_ipv6s_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_ipv6s_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_ipv6s_rgn.getInt (1);

    // done
    m_conn.commit ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_ipv6s_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_ipv6s_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_ipv6s_rgn lease valid: " + lease_valid); 

    return;
  }
  


  // ----------------------------------------
  // put_ipv6s_rgn ()
  private void put_ipv6s_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    HashMap<String, Object> row;

    Object ls_id = "0";
    Object ls_fencing_token = 0;
   
    Object ipv6_ocid = "0";  
    Object ipv6_vnic_id = "0";
    Object ipv6_subnet_id = "0";
    Object ipv6_ip_address = "0"; 
    Object ipv6_time_created = null; 
    Object ipv6_do_json = "0"; 



    m_manager.log (m_threadName, "Preparing put_ipv6s_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from IPV6S_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "IPV6S_RGN_DATA");

    ipv6_ocid = row.get ("ID"); 
    ipv6_vnic_id = row.get ("VNIC_ID");
    ipv6_subnet_id = row.get ("SUBNET_ID");
    ipv6_ip_address = row.get ("IP_ADDRESS");
    ipv6_time_created = row.get ("TIME_CREATED"); 
    ipv6_do_json = row.get ("DO_JSON");


    m_manager.log (m_threadName, "Calling put_ipv6s_rgn ...");

    // Prepare to call PLSQL: 
    //   "BEGIN vcn.put_ipv6s_rgn ( " + 
    //"        :updated_rows, " + 
    //"        :ls_id " + 
    //"        :ls_fencing_token " + 
    //"        :ipv6_ocid " + 
    //"        :ipv6_vnic_id " + 
    //"        :ipv6_subnet_id " + 
    //"        :ipv6_ip_address " + 
    //"        :ipv6_time_created " + 
    //"        :ipv6_do_json); END;"

    m_timer.start ();

    m_stmt_put_ipv6s_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_ipv6s_rgn.setObject (2, ls_id);
    m_stmt_put_ipv6s_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_ipv6s_rgn.setObject (4, ipv6_ocid);
    m_stmt_put_ipv6s_rgn.setObject (5, ipv6_vnic_id);
    m_stmt_put_ipv6s_rgn.setObject (6, ipv6_subnet_id);
    m_stmt_put_ipv6s_rgn.setObject (7, ipv6_ip_address);
    m_stmt_put_ipv6s_rgn.setObject (8, ipv6_time_created);
    m_stmt_put_ipv6s_rgn.setObject (9, ipv6_do_json);


    m_stmt_put_ipv6s_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_ipv6s_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_ipv6s_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_ipv6s_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_ipv6s_rgn", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_ipv6s_rgn updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_vlans_rgn ()
  private void del_vlans_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object vlan_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_vlans_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

    // select a random ID from VLANS_RGN_DATA
    vlan_ocid = m_manager.getCachedValue (m_threadName, "VLANS_RGN_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_vlans_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_vlans_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vlan_ocid); END;"
      
    m_timer.start ();

    m_stmt_del_vlans_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_vlans_rgn.setObject (2, ls_id);
    m_stmt_del_vlans_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_vlans_rgn.setObject (4, vlan_ocid);

    m_stmt_del_vlans_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_vlans_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_vlans_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_vlans_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_vlans_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_vlans_rgn lease valid: " + lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_vlans_rgn ()
  private void put_vlans_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object vlan_ocid = "0";
    Object vlan_compartment_id = "0";
    Object vlan_vcn_id = "0";
    Object vlan_route_table_id = "0";
    Object vlan_display_name = "0";
    Object vlan_virtual_router_mac = 0;
    Object vlan_time_created = null;
    Object vlan_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_vlans_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VLANS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "VLANS_RGN_DATA");

    vlan_ocid = row.get ("ID"); 
    vlan_compartment_id = row.get ("COMPARTMENT_ID");
    vlan_vcn_id = row.get ("VCN_ID");
    vlan_route_table_id = row.get ("ROUTE_TABLE_ID");
    vlan_display_name = row.get ("DISPLAY_NAME");
    vlan_virtual_router_mac = row.get ("VIRTUAL_ROUTER_MAC");
    vlan_time_created = row.get ("TIME_CREATED"); 
    vlan_do_json = row.get ("DO_JSON");





    m_manager.log (m_threadName, "Calling put_vlans_rgn ...");

    // Prepare to call PLSQL: 
    // "BEGIN vcn.put_vlans_rgn (" + 
    //                   " :updated_rows, " +
    //                   " :ls_id, " +
    //                   " :ls_fencing_token, " +
    //                   " :vlan_ocid, " +
    //                   " :vlan_compartment_id, " +
    //                   " :vlan_vcn_id, " +
    //                   " :vlan_route_table_id, " +
    //                   " :vlan_display_name, " +
    //                   " :vlan_virtual_router_mac, " +
    //                   " :vlan_time_created, " +
    //                   " :vlan_do_json); END;"

    m_timer.start ();

    m_stmt_put_vlans_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_vlans_rgn.setObject (2, ls_id);
    m_stmt_put_vlans_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_vlans_rgn.setObject (4, vlan_ocid);
    m_stmt_put_vlans_rgn.setObject (5, vlan_compartment_id);
    m_stmt_put_vlans_rgn.setObject (6, vlan_vcn_id);

    m_stmt_put_vlans_rgn.setObject (7, vlan_route_table_id);
    m_stmt_put_vlans_rgn.setObject (8, vlan_display_name);
    m_stmt_put_vlans_rgn.setObject (9, vlan_virtual_router_mac);

    m_stmt_put_vlans_rgn.setObject (10, vlan_time_created);
    m_stmt_put_vlans_rgn.setObject (11, vlan_do_json);

    m_stmt_put_vlans_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_vlans_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_vlans_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_vlans_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_vlans_rgn", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_vlans_rgn updated_rows: " + updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_sec_lists_rgn ()
  private void del_sec_lists_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object sl_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_security_lists_rgn input ...");
    


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");
 
    // select a random ID from SECURITY_LISTS_RGN_DATA
    sl_ocid = m_manager.getCachedValue (m_threadName, "SECURITY_LISTS_RGN_DATA", "ID");



    m_manager.log (m_threadName, "Calling del_security_lists_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_security_lists_rgn (:lease_valid, :ls_id, :ls_fencing_token, :sl_ocid); END;"
      
    m_timer.start ();

    m_stmt_del_sec_lists_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_sec_lists_rgn.setObject (2, ls_id);
    m_stmt_del_sec_lists_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_sec_lists_rgn.setObject (4, sl_ocid);

    m_stmt_del_sec_lists_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_sec_lists_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_sec_lists_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_security_lists_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_security_lists_rgn", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_security_lists_rgn lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_sec_lists_rgn ()
  private void put_sec_lists_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_security_lists_rgn (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token  " +
    //             "        :sl_ocid, " +
    //             "        :sl_compartment_id, " +
    //             "        :sl_vcn_id,  " +
    //             "        :sl_display_name,  " +
    //             "        :sl_time_created,  " +
    //             "        :sl_do_json); END;"

    Object sl_ocid = "0";
    Object sl_compartment_id = "0";
    Object sl_vcn_id = "0";
    Object sl_display_name = "0";
    Object sl_time_created = null;
    Object sl_do_json = "0";




    m_manager.log (m_threadName, "Preparing put_security_lists_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");



    // select a random row from SECURITY_LISTS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "SECURITY_LISTS_RGN_DATA");

    sl_ocid = row.get ("ID"); 
    sl_compartment_id = row.get ("COMPARTMENT_ID");
    sl_vcn_id = row.get ("VCN_ID");
    sl_display_name = row.get ("DISPLAY_NAME");
    sl_time_created = row.get ("TIME_CREATED"); 
    sl_do_json = row.get ("DO_JSON");



    m_manager.log (m_threadName, "Calling put_security_lists_rgn ...");

    // Prepare to call PLSQL: 
    // "BEGIN put_security_lists_rgn (" +
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token  " +
    //             "        :sl_ocid, " +
    //             "        :sl_compartment_id, " +
    //             "        :sl_vcn_id,  " +
    //             "        :sl_display_name,  " +
    //             "        :sl_time_created,  " +
    //             "        :sl_do_json); END;"


    m_timer.start ();

    m_stmt_put_sec_lists_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_sec_lists_rgn.setObject (2, ls_id);
    m_stmt_put_sec_lists_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_sec_lists_rgn.setObject (4, sl_ocid);
    m_stmt_put_sec_lists_rgn.setObject (5, sl_compartment_id);
    m_stmt_put_sec_lists_rgn.setObject (6, sl_vcn_id);

    m_stmt_put_sec_lists_rgn.setObject (7, sl_display_name);

    m_stmt_put_sec_lists_rgn.setObject (8, sl_time_created);

    // DEBUG:
    m_stmt_put_sec_lists_rgn.setObject (9, sl_do_json);

    // m_stmt_put_sec_lists_rgn.setString (9, sl_do_json.toString ());

    // Clob clob = m_conn.createClob();
    // clob.setString (1, sl_do_json.toString());
    // m_stmt_put_sec_lists_rgn.setClob (9, clob);


    m_stmt_put_sec_lists_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_sec_lists_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_sec_lists_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_security_lists_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_security_lists_rgn", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_security_lists_rgn updated_rows: " + updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_vcns_rgn ()
  private void del_vcns_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object vcn_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_vcns_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from VCNS_RGN_DATA
    vcn_ocid = m_manager.getCachedValue (m_threadName, "VCNS_RGN_DATA", "ID");

   
    m_manager.log (m_threadName, "Calling del_vcns_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_vcns_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vcn_ocid); END;"
      
    m_timer.start ();

    m_stmt_del_vcns_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_vcns_rgn.setObject (2, ls_id);
    m_stmt_del_vcns_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_vcns_rgn.setObject (4, vcn_ocid);

    m_stmt_del_vcns_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_vcns_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_vcns_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_vcns_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_vcns_rgn", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_vcns_rgn lease valid: " + 
      lease_valid); 

    return;
  }

  // ----------------------------------------
  // put_vcns_rgn ()
  private void put_vcns_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_vcns_rgn (" + 
    //             "        updated_rows, " + 
    //             "        ls_id, " + 
    //             "        ls_fencing_token, " + 
    //             "        vcn_ocid, " + 
    //             "        default_route_table_ocid, " + 
    //             "        default_security_list_ocid, " + 
    //             "        default_dhcp_options_ocid, " + 
    //             "        compartment_ocid, " + 
    //             "        display_name, " + 
    //             "        time_created, " + 
    //             "        vcn_do_json) ; END;"

    Object vcn_ocid = "0";
    Object default_route_table_ocid = "0";
    Object default_security_list_ocid = "0";
    Object default_dhcp_options_ocid = "0";
    Object compartment_ocid = "0";
    Object display_name = "0";
    Object time_created = null;
    Object vcn_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_vcns_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VCNS_RGN_DATA
     row = m_manager.getCachedRow (m_threadName, "VCNS_RGN_DATA");

     vcn_ocid = row.get ("ID"); 
     default_route_table_ocid = row.get ("DEFAULT_ROUTE_TABLE_ID"); 
     default_security_list_ocid = row.get ("DEFAULT_SECURITY_LIST_ID"); 
     default_dhcp_options_ocid = row.get ("DEFAULT_DHCP_OPTIONS_ID"); 
     compartment_ocid = row.get ("COMPARTMENT_ID"); 
     display_name = row.get ("DISPLAY_NAME"); 
     time_created = row.get ("TIME_CREATED");  
     vcn_do_json = row.get ("DO_JSON");     



    m_manager.log (m_threadName, "Calling put_vcns_rgn ...");

    // "BEGIN put_vcns_rgn (" + 
    //             "        updated_rows, " + 
    //             "        ls_id, " + 
    //             "        ls_fencing_token, " + 
    //             "        vcn_ocid, " + 
    //             "        default_route_table_ocid, " + 
    //             "        default_security_list_ocid, " + 
    //             "        default_dhcp_options_ocid, " + 
    //             "        compartment_ocid, " + 
    //             "        display_name, " + 
    //             "        time_created, " + 
    //             "        vcn_do_json) ; END;"


    m_timer.start ();

    m_stmt_put_vcns_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_vcns_rgn.setObject (2, ls_id);
    m_stmt_put_vcns_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_vcns_rgn.setObject (4, vcn_ocid);
    m_stmt_put_vcns_rgn.setObject (5, default_route_table_ocid);
    m_stmt_put_vcns_rgn.setObject (6, default_security_list_ocid);
    m_stmt_put_vcns_rgn.setObject (7, default_dhcp_options_ocid);

    m_stmt_put_vcns_rgn.setObject (8, compartment_ocid);
    m_stmt_put_vcns_rgn.setObject (9, display_name);

    m_stmt_put_vcns_rgn.setObject (10, time_created);
    m_stmt_put_vcns_rgn.setObject (11, vcn_do_json);

    m_stmt_put_vcns_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_vcns_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_vcns_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_vcns_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_vcns_rgn", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_vcns_rgn updated_rows: " + updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_vnic_garp_rgn ()
  private void del_vnic_garp_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object vnic_id = "0";

    
    m_manager.log (m_threadName, "Preparing del_vnic_garp_info_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from VNIC_GARP_INFO_RGN_DATA
    vnic_id = m_manager.getCachedValue(m_threadName, "VNIC_GARP_INFO_RGN_DATA", "ID");
   



    m_manager.log (m_threadName, "Calling del_vnic_garp_info_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_vnic_garp_info_rgn (:lease_valid, :ls_id, :ls_fencing_token, :vnic_id); END;"
      
    m_timer.start ();

    m_stmt_del_vnic_garp_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_vnic_garp_rgn.setObject (2, ls_id);
    m_stmt_del_vnic_garp_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_vnic_garp_rgn.setObject (4, vnic_id);

    m_stmt_del_vnic_garp_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_vnic_garp_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_vnic_garp_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_vnic_garp_info_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_vnic_garp_info_rgn", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_vnic_garp_info_rgn lease valid: " + 
      lease_valid); 

    return;
  }

  // ----------------------------------------
  // put_vnic_garp_rgn ()
  private void put_vnic_garp_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_vnic_garp_info_rgn " + 
    //             "        :updated_rows, " + 
    //             "        :ls_id,  " + 
    //             "        :ls_fencing_token,  " + 
    //             "        :vnic_id,  " + 
    //             "        :vnic_garp_info_do_json ; END;"

    Object vnic_id = "0";
    Object vnic_garp_info_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_vnic_garp_info_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from VNIC_GARP_INFO_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "VNIC_GARP_INFO_RGN_DATA");
    vnic_id = row.get ("ID"); 
    vnic_garp_info_do_json = row.get ("DO_JSON");    




    m_manager.log (m_threadName, "Calling put_vnic_garp_info_rgn ...");

    // "BEGIN put_vnic_garp_info_rgn " + 
    //             "        :updated_rows, " + 
    //             "        :ls_id,  " + 
    //             "        :ls_fencing_token,  " + 
    //             "        :vnic_id,  " + 
    //             "        :vnic_garp_info_do_json ; END;"



    m_timer.start ();

    m_stmt_put_vnic_garp_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_vnic_garp_rgn.setObject (2, ls_id);
    m_stmt_put_vnic_garp_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_vnic_garp_rgn.setObject (4, vnic_id);
    m_stmt_put_vnic_garp_rgn.setObject (5, vnic_garp_info_do_json);

    m_stmt_put_vnic_garp_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_vnic_garp_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_vnic_garp_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_vnic_garp_info_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_vnic_garp_info_rgn", m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_vnic_garp_info_rgn updated_rows: " + updated_rows); 



    return;
  }




  // ----------------------------------------
  // del_nsg_assoc_rgn ()
  private void del_nsg_assoc_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object nsg_ocid = "0";
    Object vnic_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_nsg_associations_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select random row from NSG_ASSOCIATIONS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "NSG_ASSOCIATIONS_RGN_DATA");
    nsg_ocid = row.get ("NSG_ID");
    vnic_ocid = row.get ("VNIC_ID"); 



    m_manager.log (m_threadName, "Calling del_nsg_associations_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_nsg_associations_rgn (:lease_valid, :ls_id, :ls_fencing_token, :nsg_ocid, :vnic_ocid); END;"
      
    m_timer.start ();

    m_stmt_del_nsg_assoc_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_nsg_assoc_rgn.setObject (2, ls_id);
    m_stmt_del_nsg_assoc_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_nsg_assoc_rgn.setObject (4, nsg_ocid);
    m_stmt_del_nsg_assoc_rgn.setObject (5, vnic_ocid);

    m_stmt_del_nsg_assoc_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_nsg_assoc_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_nsg_assoc_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_nsg_associations_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_nsg_associations_rgn", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_nsg_associations_rgn lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_nsg_assoc_rgn ()
  private void put_nsg_assoc_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_nsg_associations_rgn (" + //
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :nsg_ocid, " +
    //             "        :vnic_ocid, " +
    //             "        :association_time, " +
    //             "        :nsg_assc_do_json)  ; END;"

    Object nsg_ocid = "0";
    Object vnic_ocid = "0";
    Object association_time = null;
    Object nsg_assc_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_nsg_associations_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");



    // select a random row from NSG_ASSOCIATIONS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "NSG_ASSOCIATIONS_RGN_DATA");
    nsg_ocid = row.get ("NSG_ID"); 
    vnic_ocid = row.get ("VNIC_ID"); 
    association_time = row.get ("ASSOCIATION_TIME"); 
    nsg_assc_do_json = row.get ("DO_JSON");  



    m_manager.log (m_threadName, "Calling put_nsg_associations_rgn ...");

    // "BEGIN put_nsg_associations_rgn (" + //
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :nsg_ocid, " +
    //             "        :vnic_ocid, " +
    //             "        :association_time, " +
    //             "        :nsg_assc_do_json)  ; END;"



    m_timer.start ();

    m_stmt_put_nsg_assoc_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_nsg_assoc_rgn.setObject (2, ls_id);
    m_stmt_put_nsg_assoc_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_nsg_assoc_rgn.setObject (4, nsg_ocid);
    m_stmt_put_nsg_assoc_rgn.setObject (5, vnic_ocid);

    m_stmt_put_nsg_assoc_rgn.setObject (6, association_time);    
    m_stmt_put_nsg_assoc_rgn.setObject (7, nsg_assc_do_json);


    m_stmt_put_nsg_assoc_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_nsg_assoc_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_nsg_assoc_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_nsg_associations_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_nsg_associations_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_nsg_associations_rgn updated_rows: " + 
      updated_rows); 



    return;
  }



  // ----------------------------------------
  // del_float_pvt_ips_rgn ()
  private void del_float_pvt_ips_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object fpip_ocid = "0";


    
    m_manager.log (m_threadName, "Preparing del_floating_pvt_ips_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from FLOATING_PVT_IPS_RGN_DATA
    fpip_ocid = m_manager.getCachedValue (m_threadName, "FLOATING_PVT_IPS_RGN_DATA", "ID");
   




    m_manager.log (m_threadName, "Calling del_floating_pvt_ips_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN vcn.del_floating_pvt_ips_rgn (" +
    //      "  :lease_valid, " +
    //      "  :ls_id, " +
    //      "  :ls_fencing_token, " +
    //      "  :fpip_ocid )  ; END;"

    m_timer.start ();

    m_stmt_del_float_pvt_ips_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_pvt_ips_rgn.setObject (2, ls_id);
    m_stmt_del_float_pvt_ips_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_float_pvt_ips_rgn.setObject (4, fpip_ocid);

    m_stmt_del_float_pvt_ips_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_pvt_ips_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_float_pvt_ips_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_pvt_ips_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_pvt_ips_rgn", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_pvt_ips_rgn lease valid: " + 
      lease_valid); 

    return;
  }



  // ----------------------------------------
  // put_float_pvt_ips_rgn ()
  private void put_float_pvt_ips_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN put_floating_pvt_ips_rgn ( " +
    //       ":updated_rows, " +
    //       ":ls_id, " +
    //       ":ls_fencing_token, " +
    //       ":fpip_ocid, " +
    //       ":fpip_subnet_id, " +
    //       ":fpip_ip_address_int, " +
    //       ":fpip_time_created, " +
    //       ":fpip_display_name, " +
    //       ":fpip_vlan_id, " +
    //       ":fpip_do_json); END;"

    Object fpip_ocid = "0";
    Object fpip_subnet_id = "0";
    Object fpip_ip_address_int = 0;
    Object fpip_time_created = null;
    Object fpip_display_name = "0";
    Object fpip_vlan_id = "0";
    Object fpip_do_json = "0";



    m_manager.log (m_threadName, "Preparing put_floating_pvt_ips_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_PVT_IPS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_PVT_IPS_RGN_DATA");

    fpip_ocid = row.get ("ID"); 
    fpip_subnet_id = row.get ("SUBNET_ID"); 
    fpip_ip_address_int = row.get ("IP_ADDRESS_INT"); 
    fpip_time_created = row.get ("TIME_CREATED"); 
    fpip_display_name = row.get ("DISPLAY_NAME"); 
    fpip_vlan_id = row.get ("VLAN_ID"); 
    fpip_do_json = row.get ("DO_JSON");      



    m_manager.log (m_threadName, "Calling put_floating_pvt_ips_rgn ...");

    // "BEGIN put_floating_pvt_ips_rgn ( " +
    //       ":updated_rows, " +
    //       ":ls_id, " +
    //       ":ls_fencing_token, " +
    //       ":fpip_ocid, " +
    //       ":fpip_subnet_id, " +
    //       ":fpip_ip_address_int, " +
    //       ":fpip_time_created, " +
    //       ":fpip_display_name, " +
    //       ":fpip_vlan_id, " +
    //       ":fpip_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_pvt_ips_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_pvt_ips_rgn.setObject (2, ls_id);
    m_stmt_put_float_pvt_ips_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_float_pvt_ips_rgn.setObject (4, fpip_ocid);
    m_stmt_put_float_pvt_ips_rgn.setObject (5, fpip_subnet_id);

    m_stmt_put_float_pvt_ips_rgn.setObject (6, fpip_ip_address_int);
    m_stmt_put_float_pvt_ips_rgn.setObject (7, fpip_time_created); 
    
    m_stmt_put_float_pvt_ips_rgn.setObject (8, fpip_display_name);
    m_stmt_put_float_pvt_ips_rgn.setObject (9, fpip_vlan_id);

    m_stmt_put_float_pvt_ips_rgn.setObject (10, fpip_do_json);


    m_stmt_put_float_pvt_ips_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_pvt_ips_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_float_pvt_ips_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_pvt_ips_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_pvt_ips_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_pvt_ips_rgn updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_float_pvt_ip_atts_rgn ()
  private void del_float_pvt_ip_atts_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;

    Object fpipatt_ocid = "0";


    
    m_manager.log (m_threadName, "Preparing del_floating_pvt_ip_atts_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random ID from FLOATING_PVT_IP_ATTS_RGN_DATA
    fpipatt_ocid = m_manager.getCachedValue (m_threadName, "FLOATING_PVT_IP_ATTS_RGN_DATA", "ID");



    m_manager.log (m_threadName, "Calling del_floating_pvt_ip_atts_rgn ...");

    // Prepare to call PLSQL:  
    // "BEGIN ; del_floating_pvt_ip_atts_rgn (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_ocid); END;"

    m_timer.start ();

    m_stmt_del_float_pvt_ip_atts_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_pvt_ip_atts_rgn.setObject (2, ls_id);
    m_stmt_del_float_pvt_ip_atts_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_float_pvt_ip_atts_rgn.setObject (4, fpipatt_ocid);

    m_stmt_del_float_pvt_ip_atts_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_pvt_ip_atts_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_float_pvt_ip_atts_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_pvt_ip_atts_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_pvt_ip_atts_rgn", 
      m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_pvt_ip_atts_rgn lease valid: " + 
      lease_valid); 

    return;
  }


  // ----------------------------------------
  // put_float_pvt_ip_atts_rgn ()
  private void put_float_pvt_ip_atts_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_floating_pvt_ip_atts_rgn (" + 
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_ocid, " +
    //             "        :fpipatt_fpip_ocid, " +
    //             "        :fpipatt_vnic_ocid, " +
    //             "        :fpipatt_nat_ip_addr, " +
    //             "        :fpiatt_lifecycle_state, " +
    //             "        :fpipatt_do_json); END;"

    Object fpipatt_ocid = "0";
    Object fpipatt_fpip_ocid = "0";
    Object fpipatt_vnic_ocid = "0";
    Object fpipatt_nat_ip_addr = "0";
    Object fpiatt_lifecycle_state = "0";
    Object fpipatt_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_floating_pvt_ip_atts_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_PVT_IP_ATTS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_PVT_IP_ATTS_RGN_DATA");

    fpipatt_ocid = row.get ("ID"); 
    fpipatt_fpip_ocid = row.get ("FLOATING_PRIVATE_IP_ID");
    fpipatt_vnic_ocid = row.get ("VNIC_ID");
    fpipatt_nat_ip_addr = row.get ("NAT_IP_ADDRESS");
    fpiatt_lifecycle_state = row.get ("LIFECYCLE_STATE"); 
    fpipatt_do_json = row.get ("DO_JSON");   
    





    m_manager.log (m_threadName, "Calling put_floating_pvt_ip_atts_rgn ...");

    // "BEGIN vcn.put_floating_pvt_ip_atts_rgn (" + 
    //             "        :updated_rows, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fpipatt_ocid, " +
    //             "        :fpipatt_fpip_ocid, " +
    //             "        :fpipatt_vnic_ocid, " +
    //             "        :fpipatt_nat_ip_addr, " +
    //             "        :fpiatt_lifecycle_state, " +
    //             "        :fpipatt_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_pvt_ip_atts_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_pvt_ip_atts_rgn.setObject (2, ls_id);
    m_stmt_put_float_pvt_ip_atts_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_float_pvt_ip_atts_rgn.setObject (4, fpipatt_ocid);
    m_stmt_put_float_pvt_ip_atts_rgn.setObject (5, fpipatt_fpip_ocid);

    m_stmt_put_float_pvt_ip_atts_rgn.setObject (6, fpipatt_vnic_ocid);
    m_stmt_put_float_pvt_ip_atts_rgn.setObject (7, fpipatt_nat_ip_addr);

    m_stmt_put_float_pvt_ip_atts_rgn.setObject (8, fpiatt_lifecycle_state);
    m_stmt_put_float_pvt_ip_atts_rgn.setObject (9, fpipatt_do_json);


    m_stmt_put_float_pvt_ip_atts_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_pvt_ip_atts_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_float_pvt_ip_atts_rgn.getInt (1);

    // done
    m_conn.commit ();



    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_pvt_ip_atts_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_pvt_ip_atts_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_pvt_ip_atts_rgn updated_rows: " + 
      updated_rows); 



    return;
  }

 
  // ----------------------------------------
  // del_float_ips_rgn ()
  private void del_float_ips_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    // "BEGIN del_floating_ips_rgn " +
    // "        :lease_valid, " +
    // "        :ls_id,  " +
    // "        :ls_fencing_token,  " +
    // "        :fip_ocid); END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object fip_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_floating_ips_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from FLOATING_IPS_RGN_DATA
    fip_ocid = m_manager.getCachedValue (m_threadName, "FLOATING_IPS_RGN_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_floating_ips_rgn ...");

    // "BEGIN del_floating_ips_rgn " +
    // "        :lease_valid, " +
    // "        :ls_id,  " +
    // "        :ls_fencing_token,  " +
    // "        :fip_ocid); END;"

    m_timer.start ();

    m_stmt_del_float_ips_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_ips_rgn.setObject (2, ls_id);
    m_stmt_del_float_ips_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_float_ips_rgn.setObject (4, fip_ocid);

    m_stmt_del_float_ips_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_ips_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_float_ips_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_ips_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_ips_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_ips_rgn lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_float_ips_rgn ()
  private void put_float_ips_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;


    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_floating_ips_rgn ( " +
    //       " :updated_rows " +
    //       " :ls_id " +
    //       " :ls_fencing_token " +
    //       " :fip_ocid " +
    //       " :fip_ip_addr " +
    //       " :fip_compartment_id " +
    //       " :fip_scope " +
    //       " :fip_availability_domain " +
    //       " :fip_lifetime " +
    //       " :fip_public_ip_pool_id " +
    //       " :fip_do_json); END;"

    Object fip_ocid = "0";
    Object fip_ip_addr = "0";
    Object fip_compartment_id = "0";
    Object fip_scope = "0";
    Object fip_availability_domain = "0";
    Object fip_lifetime = "0";
    Object fip_public_ip_pool_id = "0";
    Object fip_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_floating_ips_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_IPS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_IPS_RGN_DATA");

    fip_ocid = row.get ("ID"); 
    fip_ip_addr = row.get ("IP_ADDRESS");
    fip_compartment_id = row.get ("COMPARTMENT_ID");
    fip_scope = row.get ("SCOPE");
    fip_availability_domain = row.get ("AVAILABILITY_DOMAIN"); 
    fip_lifetime = row.get ("LIFETIME");
    fip_public_ip_pool_id = row.get ("PUBLIC_IP_POOL_ID");
    fip_do_json = row.get ("DO_JSON");



    m_manager.log (m_threadName, "Calling put_floating_ips_rgn ...");

    // "BEGIN vcn.put_floating_ips_rgn ( " +
    //       " :updated_rows " +
    //       " :ls_id " +
    //       " :ls_fencing_token " +
    //       " :fip_ocid " +
    //       " :fip_ip_addr " +
    //       " :fip_compartment_id " +
    //       " :fip_scope " +
    //       " :fip_availability_domain " +
    //       " :fip_lifetime " +
    //       " :fip_public_ip_pool_id " +
    //       " :fip_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_ips_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_ips_rgn.setObject (2, ls_id);
    m_stmt_put_float_ips_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_float_ips_rgn.setObject (4, fip_ocid);
    m_stmt_put_float_ips_rgn.setObject (5, fip_ip_addr);

    m_stmt_put_float_ips_rgn.setObject (6, fip_compartment_id);
    m_stmt_put_float_ips_rgn.setObject (7, fip_scope);

    m_stmt_put_float_ips_rgn.setObject (8, fip_availability_domain);
    m_stmt_put_float_ips_rgn.setObject (9, fip_lifetime);

    m_stmt_put_float_ips_rgn.setObject (10, fip_public_ip_pool_id);
    m_stmt_put_float_ips_rgn.setObject (11, fip_do_json);


    m_stmt_put_float_ips_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_ips_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_float_ips_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_ips_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_ips_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_ips_rgn updated_rows: " + 
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

    // "BEGIN ; vcn.put_cache_status (" +
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

    // "BEGIN ; vcn.put_cache_status (" +
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
  // del_float_ip_atts_rgn ()
  private void del_float_ip_atts_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;

    // "BEGIN vcn.del_floating_ip_atts_rgn (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fipatt_ocid); END;"

    Object lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object fipatt_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_floating_ip_atts_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random id from FLOATING_IP_ATTS_RGN_DATA
    fipatt_ocid = m_manager.getCachedValue(m_threadName, "FLOATING_IP_ATTS_RGN_DATA", "ID");

   



    m_manager.log (m_threadName, "Calling del_floating_ip_atts_rgn ...");

    // "BEGIN vcn.del_floating_ip_atts_rgn (" +
    //             "        :lease_valid, " +
    //             "        :ls_id, " +
    //             "        :ls_fencing_token, " +
    //             "        :fipatt_ocid); END;"

    m_timer.start ();

    m_stmt_del_float_ip_atts_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_float_ip_atts_rgn.setObject (2, ls_id);
    m_stmt_del_float_ip_atts_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_float_ip_atts_rgn.setObject (4, fipatt_ocid);

    m_stmt_del_float_ip_atts_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_float_ip_atts_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_float_ip_atts_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_floating_ip_atts_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_floating_ip_atts_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_floating_ip_atts_rgn lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_float_ip_atts_rgn ()
  private void put_float_ip_atts_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_floating_ip_atts_rgn (" +
    //       "   updated_rows, " +
    //       "   ls_id, " +
    //       "   ls_fencing_token, " +
    //       "   fipatt_ocid, " +
    //       "   fipatt_vnic_id, " +
    //       "   fipatt_floating_ip_id, " +
    //       "   fipatt_floating_private_ip_id, " +
    //       "   fipatt_assigned_entity_id, " +
    //       "   fipatt_lifecycle_state, " +
    //       "   fipatt_do_json); END;"

    Object fipatt_ocid = "0";
    Object fipatt_vnic_id = "0";
    Object fipatt_floating_ip_id = "0";
    Object fipatt_floating_private_ip_id = "0";
    Object fipatt_assigned_entity_id = "0";
    Object fipatt_lifecycle_state = "0";
    Object fipatt_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_floating_ip_atts_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from FLOATING_IP_ATTS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "FLOATING_IP_ATTS_RGN_DATA");

    fipatt_ocid = row.get ("ID"); 
    fipatt_vnic_id = row.get ("VNIC_ID"); 
    fipatt_floating_ip_id = row.get ("FLOATING_IP_ID"); 
    fipatt_lifecycle_state = row.get ("LIFECYCLE_STATE"); 
    fipatt_floating_private_ip_id = row.get ("FLOATING_PRIVATE_IP_ID"); 
    fipatt_assigned_entity_id = row.get ("ASSIGNED_ENTITY_ID"); 
    fipatt_do_json = row.get ("DO_JSON"); 




    m_manager.log (m_threadName, "Calling put_floating_ip_atts_rgn ...");

    // "BEGIN vcn.put_floating_ip_atts_rgn (" +
    //       "   updated_rows, " +
    //       "   ls_id, " +
    //       "   ls_fencing_token, " +
    //       "   fipatt_ocid, " +
    //       "   fipatt_vnic_id, " +
    //       "   fipatt_floating_ip_id, " +
    //       "   fipatt_floating_private_ip_id, " +
    //       "   fipatt_assigned_entity_id, " +
    //       "   fipatt_lifecycle_state, " +
    //       "   fipatt_do_json); END;"



    m_timer.start ();

    m_stmt_put_float_ip_atts_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_float_ip_atts_rgn.setObject (2, ls_id);
    m_stmt_put_float_ip_atts_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_float_ip_atts_rgn.setObject (4, fipatt_ocid);
    m_stmt_put_float_ip_atts_rgn.setObject (5, fipatt_vnic_id);

    m_stmt_put_float_ip_atts_rgn.setObject (6, fipatt_floating_ip_id);
    m_stmt_put_float_ip_atts_rgn.setObject (7, fipatt_floating_private_ip_id);

    m_stmt_put_float_ip_atts_rgn.setObject (8, fipatt_assigned_entity_id);

    m_stmt_put_float_ip_atts_rgn.setObject (9, fipatt_lifecycle_state);
    m_stmt_put_float_ip_atts_rgn.setObject (10, fipatt_do_json);

    
    m_stmt_put_float_ip_atts_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_float_ip_atts_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_float_ip_atts_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_floating_ip_atts_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_floating_ip_atts_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_floating_ip_atts_rgn updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_subnets_rgn ()
  private void del_subnets_rgn () throws SQLException
  {
    // locals
    SQLWarning wn;
    boolean warningFlag = false;

    HashMap<String, Object> row;


    // "BEGIN vcn.del_subnets_rgn ( " +
    //       " :lease_valid, " +
    //       " :ls_id, " +
    //       " :ls_fencing_token, " +
    //       " :sn_ocid)    ; END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object sn_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_subnets_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from SUBNETS_RGN_DATA
    sn_ocid = m_manager.getCachedValue(m_threadName, "SUBNETS_RGN_DATA", "ID");


   



    m_manager.log (m_threadName, "Calling del_subnets_rgn ...");

    // "BEGIN vcn.del_subnets_rgn ( " +
    //       " :lease_valid, " +
    //       " :ls_id, " +
    //       " :ls_fencing_token, " +
    //       " :sn_ocid)    ; END;"

    m_timer.start ();

    m_stmt_del_subnets_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_subnets_rgn.setObject (2, ls_id);
    m_stmt_del_subnets_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_subnets_rgn.setObject (4, sn_ocid);

    m_stmt_del_subnets_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_subnets_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_subnets_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_subnets_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_subnets_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_subnets_rgn lease valid: " + lease_valid); 

    return;
  }
  

  // ----------------------------------------
  // put_subnets_rgn ()
  private void put_subnets_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN vcn.put_subnets_rgn (" +
    //           ":updated_rows, " +
    //           ":ls_id, " +
    //           ":ls_fencing_token, " +
    //           ":sn_ocid, " +
    //           ":sn_compartment_id, " +
    //           ":sn_vcn_id, " +
    //           ":sn_route_table_id, " +
    //           ":sn_dhcp_options_id, " +
    //           ":sn_dns_label, " +
    //           ":sn_display_name, " +
    //           ":sn_time_created, " +
    //           ":sn_do_json)   ; END;"

    Object sn_ocid = "0";
    Object sn_compartment_id = "0";
    Object sn_vcn_id = "0";
    Object sn_route_table_id = "0";
    Object sn_dhcp_options_id = "0";
    Object sn_dns_label = "0";
    Object sn_display_name = "0";
    Object sn_time_created = null;
    Object sn_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_subnets_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from SUBNETS_RGN_DATA
    row = m_manager.getCachedRow (m_threadName, "SUBNETS_RGN_DATA");

    sn_ocid = row.get ("ID"); 
    sn_compartment_id = row.get ("COMPARTMENT_ID"); 
    sn_vcn_id = row.get ("VCN_ID"); 
    sn_route_table_id = row.get ("ROUTE_TABLE_ID"); 
    sn_dhcp_options_id = row.get ("DHCP_OPTIONS_ID"); 
    sn_dns_label = row.get ("DNS_LABEL"); 
    sn_display_name = row.get ("DISPLAY_NAME"); 
    sn_time_created = row.get ("TIME_CREATED"); 
    sn_do_json = row.get ("DO_JSON");  




    m_manager.log (m_threadName, "Calling put_subnets_rgn ...");

    // "BEGIN vcn.put_subnets_rgn (" +
    //           ":updated_rows, " +
    //           ":ls_id, " +
    //           ":ls_fencing_token, " +
    //           ":sn_ocid, " +
    //           ":sn_compartment_id, " +
    //           ":sn_vcn_id, " +
    //           ":sn_route_table_id, " +
    //           ":sn_dhcp_options_id, " +
    //           ":sn_dns_label, " +
    //           ":sn_display_name, " +
    //           ":sn_time_created, " +
    //           ":sn_do_json)   ; END;"



    m_timer.start ();

    m_stmt_put_subnets_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_subnets_rgn.setObject (2, ls_id);
    m_stmt_put_subnets_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_subnets_rgn.setObject (4, sn_ocid);
    m_stmt_put_subnets_rgn.setObject (5, sn_compartment_id);

    m_stmt_put_subnets_rgn.setObject (6, sn_vcn_id);
    m_stmt_put_subnets_rgn.setObject (7, sn_route_table_id);

    m_stmt_put_subnets_rgn.setObject (8, sn_dhcp_options_id);

    m_stmt_put_subnets_rgn.setObject (9, sn_dns_label);
    m_stmt_put_subnets_rgn.setObject (10, sn_display_name);

    m_stmt_put_subnets_rgn.setObject (11, sn_time_created);
    m_stmt_put_subnets_rgn.setObject (12, sn_do_json);


    
    m_stmt_put_subnets_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_subnets_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_subnets_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_subnets_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_subnets_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_subnets_rgn updated_rows: " + 
      updated_rows); 



    return;
  }


  // ----------------------------------------
  // del_net_sec_group_rgn ()
  private void del_net_sec_group_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    // "BEGIN vcn.del_network_security_group_rgn (" +
    //                 "        :lease_valid, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_ocid); END;"

    int lease_valid = 0;
    Object ls_id = "0";
    Object ls_fencing_token = 0;
    Object nsg_ocid = "0";

    
    m_manager.log (m_threadName, "Preparing del_network_security_group_rgn input ...");
    

    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");

 
    // select a random row from NETWORK_SECURITY_GROUP_RGN_DAT
    nsg_ocid = m_manager.getCachedValue(m_threadName, "NETWORK_SECURITY_GROUP_RGN_DAT", "ID");

   



    m_manager.log (m_threadName, "Calling del_network_security_group_rgn ...");

    // "BEGIN vcn.del_network_security_group_rgn (" +
    //                 "        :lease_valid, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_ocid); END;"

    m_timer.start ();

    m_stmt_del_net_sec_group_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_del_net_sec_group_rgn.setObject (2, ls_id);
    m_stmt_del_net_sec_group_rgn.setObject (3, ls_fencing_token);
    m_stmt_del_net_sec_group_rgn.setObject (4, nsg_ocid);

    m_stmt_del_net_sec_group_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_del_net_sec_group_rgn.getWarnings()) != null) 
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
      lease_valid = m_stmt_del_net_sec_group_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "del_network_security_group_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("del_network_security_group_rgn", m_timer.getTimeInUs ());



    // lease status
    m_manager.log (m_threadName, "del_network_security_group_rgn lease valid: " + lease_valid); 

    return;
  }
  
// ----------------------------------------
  // put_net_sec_group_rgn ()
  private void put_net_sec_group_rgn () throws SQLException
  {
    // locals
    HashMap<String, Object> row;

    SQLWarning wn;
    boolean warningFlag = false;

    int updated_rows = 0;

    Object ls_id = "0";
    Object ls_fencing_token = 0;

    // "BEGIN ; vcn.put_network_security_group_rgn (" +
    //                 "        :updated_rows, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_ocid, " +
    //                 "        :nsg_dp_id, " +
    //                 "        :nsg_compartment_id, " +
    //                 "        :nsg_vcn_id, " +
    //                 "        :nsg_display_name, " +
    //                 "        :nsg_time_created, " +
    //                 "        :nsg_do_json) END;"

    Object nsg_ocid = "0";
    Object nsg_dp_id = "0";
    Object nsg_compartment_id = "0";
    Object nsg_vcn_id = "0";
    Object nsg_display_name = "0";
    Object nsg_time_created = null;
    Object nsg_do_json = "0";


    m_manager.log (m_threadName, "Preparing put_network_security_group_rgn input ...");


    // select a random row from LEASE_STATUS_DATA
    row = m_manager.getCachedRow (m_threadName, "LEASE_STATUS_DATA");
    ls_id = row.get ("ID");
    ls_fencing_token = row.get ("FENCING_TOKEN");


    // select a random row from NETWORK_SECURITY_GROUP_RGN_DAT
    row = m_manager.getCachedRow (m_threadName, "NETWORK_SECURITY_GROUP_RGN_DAT");
  
    nsg_ocid = row.get ("ID"); 
    nsg_dp_id = row.get ("DP_ID"); 
    nsg_compartment_id = row.get ("COMPARTMENT_ID"); 
    nsg_vcn_id = row.get ("VCN_ID"); 
    nsg_display_name = row.get ("DISPLAY_NAME"); 
    nsg_time_created = row.get ("TIME_CREATED"); 
    nsg_do_json = row.get ("DO_JSON"); 






    m_manager.log (m_threadName, "Calling put_network_security_group_rgn ...");

    // "BEGIN ; vcn.put_network_security_group_rgn (" +
    //                 "        :updated_rows, " +
    //                 "        :ls_id, " +
    //                 "        :ls_fencing_token, " +
    //                 "        :nsg_ocid, " +
    //                 "        :nsg_dp_id, " +
    //                 "        :nsg_compartment_id, " +
    //                 "        :nsg_vcn_id, " +
    //                 "        :nsg_display_name, " +
    //                 "        :nsg_time_created, " +
    //                 "        :nsg_do_json) END;"



    m_timer.start ();

    m_stmt_put_net_sec_group_rgn.registerOutParameter (1, Types.INTEGER);
    m_stmt_put_net_sec_group_rgn.setObject (2, ls_id);
    m_stmt_put_net_sec_group_rgn.setObject (3, ls_fencing_token);

    m_stmt_put_net_sec_group_rgn.setObject (4, nsg_ocid);
    m_stmt_put_net_sec_group_rgn.setObject (5, nsg_dp_id);

    m_stmt_put_net_sec_group_rgn.setObject (6, nsg_compartment_id);
    m_stmt_put_net_sec_group_rgn.setObject (7, nsg_vcn_id);

    m_stmt_put_net_sec_group_rgn.setObject (8, nsg_display_name);

    m_stmt_put_net_sec_group_rgn.setObject (9, nsg_time_created);
    m_stmt_put_net_sec_group_rgn.setObject (10, nsg_do_json);


    
    m_stmt_put_net_sec_group_rgn.execute();


    // If there are warnings, output parameter values are undefined.
    if ((wn = m_stmt_put_net_sec_group_rgn.getWarnings()) != null) 
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
      updated_rows = m_stmt_put_net_sec_group_rgn.getInt (1);

    // done
    m_conn.commit ();


    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "put_network_security_group_rgn elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("put_network_security_group_rgn", 
      m_timer.getTimeInUs ());



    // updated rows
    m_manager.log (m_threadName, "put_network_security_group_rgn updated_rows: " + 
      updated_rows); 



    return;
  }

  // ----------------------------------------
  // query_get_vnic ()
  private void query_get_vnic () throws SQLException
  {
    // locals
    ResultSet rs;

    // "SELECT do_json FROM VNICS_RGN WHERE id = ?"

    Object vnic_id = "0";
    String vnic_do_json = "0";


    m_manager.log (m_threadName, "Preparing query_get_vnic input ...");
    
    // select a random id from VNICS_RGN_DATA
    vnic_id = m_manager.getCachedValue (m_threadName, "VNICS_RGN_DATA", "ID");


    m_manager.log (m_threadName, "Calling query_get_vnic ...");

    m_timer.start ();

    m_stmt_query_get_vnic.setObject (1, vnic_id);
    rs = m_stmt_query_get_vnic.executeQuery();

    while (rs.next ())
    {
      vnic_do_json = rs.getString (1);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_vnic elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_vnic", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_vnic do_json: " + vnic_do_json); 

    return;
  }



  // ----------------------------------------
  // query_get_net_sec_groups ()
  private void query_get_net_sec_groups () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT NSG.do_json FROM NETWORK_SECURITY_GROUP_RGN NSG " + 
    // "INNER JOIN NSG_ASSOCIATIONS_RGN NSG_ASC " + 
    // "ON NSG.id = NSG_ASC.nsg_id " + 
    // "WHERE NSG_ASC.vnic_id = ?"

    Object vnic_id = "0";
    String nsg_do_json = "0";


    m_manager.log (m_threadName, "Preparing query_get_net_sec_groups input ...");
  
    // select a random id from the table
    vnic_id = m_manager.getCachedValue (m_threadName, "NSG_ASSOCIATIONS_RGN_DATA", "VNIC_ID");

    

    m_manager.log (m_threadName, "Calling query_get_net_sec_groups ...");

    m_timer.start ();

    m_stmt_query_get_net_sec_groups.setObject (1, vnic_id);
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


    // "SELECT FIPs.do_json, FIPAs.do_json FROM FLOATING_IP_ATTS_RGN FIPAs " +
    // "INNER JOIN FLOATING_IPS_RGN FIPs " +
    // "ON FIPAs.floating_ip_id = FIPs.id " +
    // "WHERE FIPAs.floating_private_ip_id = ? " +
    // "AND FIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"

    Object floating_private_ip_id = "0";
    String fips_do_json = "0";
    String fipas_do_json = "0";

    m_manager.log (m_threadName, "Preparing query_get_prim_pub_ip input ...");
  
    // select random IDs from the table
    floating_private_ip_id = m_manager.getCachedValue (m_threadName, "FLOATING_IP_ATTS_RGN_DATA", "FLOATING_PRIVATE_IP_ID");


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


    // "SELECT FPIPs.do_json, FPIPAs.do_json FROM FLOATING_PVT_IP_ATTS_RGN FPIPAs " +
    // "INNER JOIN FLOATING_PVT_IPS_RGN FPIPs " +
    // "ON FPIPAs.floating_private_ip_id = FPIPs.id " +
    // "WHERE FPIPAs.vnic_id = ? " +
    // "AND FPIPAs.lifecycle_state IN ('AVAILABLE', 'PROVISIONING')"

    Object vnic_id = "0";
    String fips_do_json = "0";
    String fipas_do_json = "0";

    m_manager.log (m_threadName, "Preparing query_get_prim_priv_ip input ...");
  
    // select random IDs from the table
    vnic_id = m_manager.getCachedValue (m_threadName, "FLOATING_PVT_IP_ATTS_RGN_DATA", "VNIC_ID");




    m_manager.log (m_threadName, "Calling query_get_prim_priv_ip ...");

    m_timer.start ();

    m_stmt_query_get_prim_priv_ip.setObject (1, vnic_id); 
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
  // query_get_ipv6_addr_vnic ()
  private void query_get_ipv6_addr_vnic () throws SQLException
  {
    // locals
    ResultSet rs;


    // "SELECT do_json, time_created FROM VCN.IPV6S_RGN " +
    // "WHERE vnic_id = ? " +
    // "AND rownum <= 32 " +
    // "ORDER BY time_created ASC"

    Object vnic_id = "0";
    String do_json = "0";
    java.sql.Timestamp time_created = null;

    m_manager.log (m_threadName, "Preparing query_get_ipv6_addr_vnic input ...");
  
    // select random IDs from the table
    vnic_id = m_manager.getCachedValue (m_threadName, "IPV6S_RGN_DATA", "VNIC_ID");



    m_manager.log (m_threadName, "Calling query_get_ipv6_addr_vnic ...");

    m_timer.start ();

    m_stmt_query_get_ipv6_addr_vnic.setObject (1, vnic_id); 
    rs = m_stmt_query_get_ipv6_addr_vnic.executeQuery();

    while (rs.next ())
    {
      do_json = rs.getString (1);
      time_created = rs.getTimestamp (2);
    }

    rs.close ();

    // get the elapsed time
    m_timer.stop ();
    m_manager.log (m_threadName, "query_get_ipv6_addr_vnic elapsed (us): " + 
      m_timer.getTimeInUs ()); 

    // store the elapsed time in the manager
    m_manager.setTiming ("query_get_ipv6_addr_vnic", m_timer.getTimeInUs ());



    // output
    m_manager.log (m_threadName, "query_get_ipv6_addr_vnic do_json: " + do_json); 
    m_manager.log (m_threadName, "query_get_ipv6_addr_vnic time_created: " + time_created); 

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


