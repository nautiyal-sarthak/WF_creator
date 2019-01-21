#!/bin/sh
#####################################################################################
# Script Behaviour: Script invoked from Control M for SAIL extracted SRC_TMP_U tables.#
# Input Parameter :Order Date , environment                                         #
# sh aml_COUNRTY_TMP_L_SRC_TMP_L_source_count.sh 20170219 sit                                    #
#                                                                                   #
# Return Code: 0 executed successfully.                                             #
#              1 execution failed and error message sent to Standard Output.        #
# Change History: Initial Version                                                   #
#                                                                                   #
# Created By: SAIL DEV Team                                                         #
# Date:                 20170218                                                    #
# Change Log:   Initial Version    2017-05-17                                       #
#####################################################################################
 if [ $# -ne 2 ] 
 then
         echo "Expecting 2 parameters, Received "$#
         echo "USAGE : <ORMTSR DATE> <ENV>"
         echo "[`date +%c`] ERROR: Unexpected number of parameters! Expected = 2, Received = $#"
         exit 1
 fi

 orderdate=$1
 instance=$2
 sub_system="SRC_TMP_U"
 country="COUNRTY_TMP_U"
 script_name=`basename $0 .sh`

 order_date=`date -d "${orderdate}" +'%Y-%m-%d'`
 instance_lower=`echo $instance | tr '[:upper:]' '[:lower:]'`
 instance_upper=`echo $instance | tr '[:lower:]' '[:upper:]'`
 sub_system_lower=`echo $sub_system | tr '[:upper:]' '[:lower:]'`
 sub_system_upper=`echo $sub_system | tr '[:lower:]' '[:upper:]'`
 country_lower=`echo $country | tr '[:upper:]' '[:lower:]'`
 country_upper=`echo $country | tr '[:lower:]' '[:upper:]'`
 batch_date="${orderdate:0:4}-${orderdate:4:2}-${orderdate:6:2}"

 aml_tmd="${instance_upper}_aml_tmd"
 global_source_count=${instance_upper}_all_global_source_count
 aml_sri_open="DB_TMP_U"
 aml_process_base_path=${AML_SAIL_BASE}/${sub_system_upper}/processing
 log_file_with_path="${aml_process_base_path}/log/${script_name}_${orderdate}_`date +'%Y%m%d_%H%M'`.log"
 businessday="${orderdate:0:4}-${orderdate:4:2}-${orderdate:6:2}"
 HAAS_AML_STATUS="${aml_process_base_path}/log/${country_lower}_${sub_system_lower}_e2e_status.dat"
 
 echo "`date +"%Y-%m-%d %H:%M:%S"` : ********************** ${script_name}  S T A R T S  *****************************        " | tee -a $log_file_with_path
 echo " " | tee -a ${log_file_with_path}
 echo " HOST NAME           : `hostname` " | tee -a ${log_file_with_path}
 echo " BUSINESS DATE       : ${businessday} " | tee -a ${log_file_with_path}
 echo " SCRIPT PATH         : ${AML_SAIL_BASE}/${sub_system_upper}/appl/scripts " | tee -a ${log_file_with_path}
 echo " TABLE LIST PATH     : ${AML_SAIL_BASE}/${sub_system_upper}/processing/config/ " | tee -a ${log_file_with_path}
 echo " LOG FILE            : ${log_file_with_path}" | tee -a ${log_file_with_path}
 echo " " | tee -a ${log_file_with_path}

 hadoop fs -rm ${HDFS_SRC_PATH}/${global_source_count}/source_system='${sub_system_upper}'/country_cd='${country_upper}'/*
 last_status=$?
 if [ $last_status -gt 0 ]
 then
 echo "`date +"%Y-%m-%d %H:%M:%S"` : Older file for ${global_source_count} is not available for country : ${country_upper}  and ${sub_system_upper}" | tee -a ${log_file_with_path}
 else
         echo "`date +"%Y-%m-%d %H:%M:%S"` : Older file for ${global_source_count} has been removed from HDFS for country: ${country_upper} and ${sub_system_upper}" | tee -a $log_file_with_path
 fi

 echo "`date +"%Y-%m-%d %H:%M:%S"` : Starting ${sub_system_upper} ${aml_tmd}.${global_source_count} data population." | tee -a ${log_file_with_path}
 hive -S -e " set hive.exec.parallel=true;set hive.auto.convert.join=false;
 INSERT OVERWRITE TABLE ${aml_tmd}.${global_source_count} PARTITION (source_system='${sub_system_upper}',country_cd='${country_upper}')
 QUERY"

 if [ $? -gt 0 ]
 then
         echo "`date +"%Y-%m-%d %H:%M:%S"` : Error while loading ${aml_tmd}.${global_source_count} table ${sub_system_upper}."| tee -a $log_file_with_path
         exit 1
 else
         echo "`date +"%Y-%m-%d %H:%M:%S"` : Data insertion into table ${aml_tmd}.${global_source_count} has compeleted"| tee -a $log_file_with_path
 fi

 echo "`date +"%Y-%m-%d %H:%M:%S"` : Starting ${sub_system_upper} ${aml_tmd}.${instance_upper}_sail_aml_source_status data population." | tee -a ${log_file_with_path}

 hive -S -e "set hive.exec.dynamic.partition=true;
   INSERT  OVERWRITE  TABLE ${aml_tmd}.${instance_upper}_sail_aml_source_status PARTITION(source_system,country_cd,business_date)
   SELECT gec.table_name,
          gec.extrn_cnt,
          CASE WHEN gsc.source_cnt IS NULL THEN  gec.EXTRN_CNT ELSE gsc.source_cnt END AS source_cnt,
          CASE WHEN gsc.source_cnt IS NULL THEN  gec.EXTRN_CNT ELSE gec.extrn_cnt-gsc.source_cnt END AS diff,
          CASE WHEN (CASE WHEN gsc.source_cnt IS NULL THEN gec.EXTRN_CNT ELSE gsc.source_cnt END)-gec.EXTRN_CNT=0 THEN 'MATCH' ELSE 'MISMATCH' END AS status,
          gec.source_system,
          gec.country_cd,
          gec.business_date
    FROM  ${aml_tmd}.${instance_upper}_all_global_extrn_count gec
          LEFT JOIN ${aml_tmd}.${instance_upper}_all_global_source_count gsc 
                 ON ( UPPER(gec.table_name)=UPPER(gsc.table_name) 
                      AND UPPER(gec.country_cd)=UPPER(gsc.country_cd) 
                      AND gec.business_date=gsc.business_date)
   WHERE UPPER(gec.country_cd)='${country_upper}'
     AND UPPER(gec.source_system)='${sub_system_upper}'
   "

 if [ $? -gt 0 ]
 then
         echo "`date +"%Y-%m-%d %H:%M:%S"` : Error in inserting data into table SAIL_AML_SOURCE_STATUS table for ${sub_system_upper} data."| tee -a $log_file_with_path
         exit 1
 else
         echo "`date +"%Y-%m-%d %H:%M:%S"` : Data Insertion into table SAIL_AML_SOURCE_STATUS successfuly compeleted for ${sub_system_upper}."| tee -a $log_file_with_path
 fi

 echo "`date +"%Y-%m-%d %H:%M:%S"` : Starting ${sub_system_upper} ${aml_tmd}.${instance_upper}_SAIL_E2E_RECON data population." | tee -a ${log_file_with_path}
 hive -S -e "INSERT OVERWRITE TABLE ${aml_tmd}.${instance_upper}_sail_e2e_recon PARTITION (source_system,country,extraction_date)
  SELECT UPPER(sail_t1.table_name) AS table_name ,
         nvl(cast(sail_t1.source_count as int),'NA'),
         nvl(cast(sail_t1.tier1_count as int),'NA'),
         sail_t1.tier1_recon_status,
         sail_ext.source_cnt AS sail_extrn_tier1_count,
         sail_ext.extrn_cnt AS sail_extrn_file_count,
         sail_ext.status AS sail_recon_status,
         '${sub_system_upper}' AS source_system,
         '${country_upper}' AS country,
         sail_ext.business_date AS extraction_date
 FROM (
        SELECT REGEXP_REPLACE(table_name,'${country_upper}_${sub_system_upper}_','') AS ext_tbl_name, business_date,extrn_cnt,source_cnt,status 
         FROM ${aml_tmd}.${instance_upper}_sail_aml_source_status
        WHERE source_system='${sub_system_upper}'  
          AND business_date='${batch_date}'
          AND country_cd='${country_upper}'
        )sail_ext
       INNER JOIN ( 
					SELECT integrated_view.table_name,recon_output.source_value AS source_count ,recon_output.target_value AS tier1_count,integrated_view.overall_recon_status AS tier1_recon_status
					  FROM
						(
							SELECT Regexp_replace(Upper(table_name), '${sub_system_upper}_${country_upper}_', '') AS table_name,overall_recon_status
							  FROM ${sub_system_lower}_prd_ops.${sub_system_lower}_${country_lower}_integrated_recon_ops_view
							 WHERE Trim(partition_date) = '${batch_date}'
							   AND UPPER(country_name)= '${country_upper}'
							   AND UPPER(source_name) = '${sub_system_upper}'
						) integrated_view
					LEFT OUTER JOIN
					(
							SELECT Regexp_replace(Upper(table_name), '${sub_system_upper}_${country_upper}_', '') AS table_name,source_value,target_value,Max(insert_time) AS I_Time
							  FROM ${sub_system_lower}_prd_ops.${sub_system_lower}_${country_lower}_recon_output
							 WHERE LOWER(identifier) LIKE '%rowcount'
							   AND business_date = '${batch_date}'
						  GROUP BY table_name,source_value,target_value
					) recon_output
					ON ( Upper(integrated_view.table_name) = Upper(recon_output.table_name) )
                  )sail_t1
              ON UPPER(TRIM(sail_ext.ext_tbl_name)) = UPPER(TRIM(sail_t1.table_name))"
 if [ $? -gt 0 ]
 then
         echo "`date +"%Y-%m-%d %H:%M:%S"` : ERROR in HIVE connection or in E2E TABLE population for ${country_upper}...!!!"| tee -a ${log_file_with_path}
         exit 1
 else
         echo "`date +"%Y-%m-%d %H:%M:%S"` : ${country_upper} ${sub_system_upper} data has been loaded in ${aml_tmd}.${instance_upper}_SAIL_E2E_RECON." | tee -a ${log_file_with_path}
 fi
 
 hive -S -e "SELECT CONCAT_WS('\u0001',nvl(table_name,''),nvl(source_count,''),nvl(tier1_count,''),nvl(tier1_recon_status,''),
                                       nvl(sail_extrn_tier1_count,''),nvl(sail_extrn_file_count,''),nvl(sail_recon_status,''),
                                       nvl(source_system,''),nvl(country,''),nvl(extraction_date,'') ) 
              FROM ${aml_tmd}.${instance_upper}_sail_e2e_recon  
             WHERE extraction_date='${batch_date}' 
               AND UPPER(country)='${country_upper}'  
               AND UPPER(source_system)='${sub_system_upper}'" > ${HAAS_AML_STATUS}

 if [ $? -gt 0 ]
 then
         echo "`date +"%Y-%m-%d %H:%M:%S"` : ERROR in exporting data from ${aml_tmd}.${instance_upper}_SAIL_E2E_RECON for extraction_date='${batch_date}' AND  UPPER(COUNTRY_CD)='${country_upper}' AND UPPER(SOURCE_SYSTEM)='${sub_system_upper}."| tee -a $log_file_with_path
         echo "`date +"%Y-%m-%d %H:%M:%S"` : ERROR : ${script_name} failed. Please check the log...!!! "| tee -a $log_file_with_path
         exit 1
 else
         echo "`date +"%Y-%m-%d %H:%M:%S"` : Data exporting for ${aml_tmd}.${instance_upper}_SAIL_AML_SOURCE_STATUS extraction_date='${batch_date}' AND  UPPER(COUNTRY_CD)='${country_upper}'  AND  UPPER(SOURCE_SYSTEM)='${sub_system_upper} has been compeleted."| tee -a $log_file_with_path
 fi
