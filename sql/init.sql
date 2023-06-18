''' SQL QUERY USED IN ORACLE TO CREATE TABLE '''
CREATE TABLE POP_CUST_DATA
(
base_month  NUMBER (30)  ,       
  brand  VARCHAR2(2000)  ,
  term  NUMBER (30)  ,
  div_sub  VARCHAR2(2000)  ,        
  div_sub_name_e  VARCHAR2(2000)  , 
  plan_grp  VARCHAR2(2000)  ,       
  plan_grp_name_e  VARCHAR2(2000)  ,
  plan_sum  VARCHAR2(2000)  ,       
  plan_sum_name_e  VARCHAR2(2000)  ,
  plan_itgrp  VARCHAR2(2000)  ,
  plan_itgrp_name_e  VARCHAR2(2000)  ,
  item_code  VARCHAR2(2000)  ,
  item_code_clean  VARCHAR2(2000)  ,
  item_name  VARCHAR2(2000)  ,
  sao_group  VARCHAR2(2000)  ,
  sao_group_name_e  VARCHAR2(2000)  ,
  ovs_cust_name  VARCHAR2(200)  ,
  sap_customer_no  VARCHAR2(200)  ,
  disty_type  VARCHAR2(2000)  ,
  disty_type_name  VARCHAR2(2000)  ,
  budget_amt  NUMBER (30)  ,
  budget_qty  NUMBER (30)  ,
  budget_price  NUMBER (30)  ,
  result_amt  NUMBER (30)  ,
  result_qty  NUMBER (30)  ,
  result_price  NUMBER (30)  ,
  co_amt  NUMBER (30)  ,
  co_qty  NUMBER (30)  ,
  co_price  NUMBER (30)  ,
  fi_amt  NUMBER (30)  ,
  fi_qty  NUMBER (30)  ,
  fi_price  NUMBER (30)  ,
  bok_amt  NUMBER (30)  ,
  bok_qty  NUMBER (30)  ,
  bok_price  NUMBER (30)  ,
  country_code  VARCHAR2(2000)  ,
  zip_code  VARCHAR2(2000)  ,
  region1  VARCHAR2(2000)  ,
  region1_name_e  VARCHAR2(2000)  ,
  region3  VARCHAR2(2000)  ,
  region3_name_e  VARCHAR2(2000)  ,
  region5  VARCHAR2(2000)  ,
  region5_name_e  VARCHAR2(2000)  ,
  territory_code  VARCHAR2(2000)  ,
  territory_name  VARCHAR2(2000)  ,
  district_code  VARCHAR2(2000)  ,
  district_name  VARCHAR2(2000)  ,
  execution_time_stamp  TimeStamp  ,
  process_date  NUMBER (30)
 );
 
 CREATE TABLE POP_CUST_DATA_FINAL 
(
base_month  NUMBER (30)  ,       
  brand  VARCHAR2(2000)  ,
  term  NUMBER (30)  ,
  div_sub  VARCHAR2(2000)  ,        
  div_sub_name_e  VARCHAR2(2000)  , 
  plan_grp  VARCHAR2(2000)  ,       
  plan_grp_name_e  VARCHAR2(2000)  ,
  plan_sum  VARCHAR2(2000)  ,       
  plan_sum_name_e  VARCHAR2(2000)  ,
  plan_itgrp  VARCHAR2(2000)  ,
  plan_itgrp_name_e  VARCHAR2(2000)  ,
  item_code  VARCHAR2(2000)  ,
  item_code_clean  VARCHAR2(2000)  ,
  item_name  VARCHAR2(2000)  ,
  sao_group  VARCHAR2(2000)  ,
  sao_group_name_e  VARCHAR2(2000)  ,
  ovs_cust_name  VARCHAR2(200)  ,
  sap_customer_no  VARCHAR2(200)  ,
  disty_type  VARCHAR2(2000)  ,
  disty_type_name  VARCHAR2(2000)  ,
  budget_amt  NUMBER (30)  ,
  budget_qty  NUMBER (30)  ,
  budget_price  NUMBER (30)  ,
  result_amt  NUMBER (30)  ,
  result_qty  NUMBER (30)  ,
  result_price  NUMBER (30)  ,
  co_amt  NUMBER (30)  ,
  co_qty  NUMBER (30)  ,
  co_price  NUMBER (30)  ,
  fi_amt  NUMBER (30)  ,
  fi_qty  NUMBER (30)  ,
  fi_price  NUMBER (30)  ,
  bok_amt  NUMBER (30)  ,
  bok_qty  NUMBER (30)  ,
  bok_price  NUMBER (30)  ,
  country_code  VARCHAR2(2000)  ,
  zip_code  VARCHAR2(2000)  ,
  region1  VARCHAR2(2000)  ,
  region1_name_e  VARCHAR2(2000)  ,
  region3  VARCHAR2(2000)  ,
  region3_name_e  VARCHAR2(2000)  ,
  region5  VARCHAR2(2000)  ,
  region5_name_e  VARCHAR2(2000)  ,
  territory_code  VARCHAR2(2000)  ,
  territory_name  VARCHAR2(2000)  ,
  district_code  VARCHAR2(2000)  ,
  district_name  VARCHAR2(2000)  ,
  execution_time_stamp  TimeStamp  ,
  process_date  NUMBER (30)
 );
 
 
select ovs_cust_name,sap_customer_no,item_name,item_code,execution_time_stamp from POP_CUST_DATA_FINAL
group by ovs_cust_name,sap_customer_no,item_name,item_code,execution_time_stamp order by execution_time_stamp;

UPDATE POP_CUST_DATA_FINAL
SET item_code = 'BLANK_item_code'
WHERE item_code IS NULL;

UPDATE POP_CUST_DATA_FINAL
SET item_code = 'BLANK_item_name'
WHERE item_name IS NULL;


(select * from (SELECT POP_CUST_DATA_FINAL.*,ROW_NUMBER() OVER (PARTITION BY ovs_cust_name, sap_customer_no, item_name, item_code
ORDER BY execution_time_stamp DESC) AS RowNumber FROM POP_CUST_DATA_FINAL) where rownumber=1;

 