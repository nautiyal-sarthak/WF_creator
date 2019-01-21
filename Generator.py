#!/usr/bin/python
from numpy.core.defchararray import lower
import sys
import os
import xlrd
import pandas as pd
import os.path, shutil
import datetime
import glob, os


__author__ = "SAIL"
__copyright__ = "Copyright 2017, The Sail Project"
__version__ = "1.0.1"
__maintainer__ = "sarthak"
__email__ = "sarthak.nautiyal@sc.com"
__status__ = "SIT"



sys_name = "dotopal" # sys.argv[1]
country = "zw" # sys.argv[2]

sys_name_u = str.upper(sys_name)
country_u = str.upper(country)

sys_name_l = str.lower(sys_name)
country_l = str.lower(country)

USER_NAME = 'sitamlapp'
DEST_PATH = 'sit_hdfs'
DB_ID = 'sithive'
STORAGE = 'SIT_AML_STORAGE'
DATABASE = ''
#SEP = '\\u0001'
SIZE = 20

first = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workflowDefinitionVO>"""

header = """<dataTransferSpecification>
        <businessDayBehavior>NO</businessDayBehavior>"""

cols = """<columns>
            <destination>COLNAME</destination>
            <source>COLNAME</source>
</columns> """

path = """<destinationDatabase>/user/user_name/output/</destinationDatabase>
                <destinationTable>output_file_name</destinationTable>
                <sourceDatabase>db_name</sourceDatabase>
                <sourceTable>table_name</sourceTable>
                <sourceTablePartitionName>BUSINESS_DAY</sourceTablePartitionName>
                <filterCondition>WHERE_CLAUSE</filterCondition>
                <columnOrder>var_column_order</columnOrder>
        </dataTransferSpecification> """

trailer = """<description>Workflow for SRI</description>
        <destinationName>dest_path</destinationName>
        <name>wrk_name</name>
        <sourceName>db_id</sourceName>
        <parameters>businessday</parameters>
        <parameters>country</parameters>
        <batchToPartitionDatabase>storage</batchToPartitionDatabase>
        <type>TABLE_GROUP</type>
        <processing><type>HEADER_AND_FOOTER</type><columnNames>true</columnNames></processing>
        <destinationOptions>
        <entry>
                <key>outputcolseparator</key>
                <value>\\u0001</value>
        </entry>
        <entry>
               <key>nullable</key>
               <value>''</value>
        </entry>
</destinationOptions>
</workflowDefinitionVO> """

mr_change = """rm sit_aml_<SRC_L>_<CC_L>_*.hql
<MR_GET>
sed -i "1i set fs.file.impl.disable.cache=false;set fs.hdfs.impl.disable.cache=false;" sit_aml_<SRC_L>_<CC_L>_*.hql 
<MR_PUT>
rm sit_aml_<SRC_L>_<CC_L>_*.hql
"""


## creating the chunk of size(parameter given in config.ini)
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


## create the all three files 1. workflow  2.workflow_adhoc.txt 3.table_info.txt
def create_file(src, cc, types, name):
    if types == 'wf':
        file = output_folder + '/' + src + "_" + cc + "_workflows.txt"
    elif types == 'adhoc':
        file = output_folder + '/' + src + "_" + cc + "_workflows_adhc.txt"
    elif types == 'table':
        file = output_folder + '/' + src + "_" + cc + "_table_info.txt"
    f = open(file, 'a+')
    f.write(name + '|Y' + '\n')
    f.close()


# get the cols from the ISD
def get_ISD_Col(isd):
    cols = []
    isDetailsSection = False
    for i in range(isd.nrows):
        row = isd.row_values(i)

        if row[0] == "Detail  Record":
            isDetailsSection = True
            continue

        if row[0] == "Footer/Trailer Record":
            isDetailsSection = False
            break

        if isDetailsSection:
            if row[1] == "Record Identifier":
                continue
            cols.append(str(row[1]).lower())

    return cols


# replace the last occurence of a string from a string
def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


## function to create the workflow xml
def create_xml(config_df, isd, wftype):
    wf_name = "sit_aml_" + sys_name_l + "_" + country_l + "_" + wftype
    isd_tbl_list = config_df["isd"].tolist()
    for cnt, tab in enumerate(list(chunks(isd_tbl_list, SIZE))):
        if cnt == 0:
            workflow_name = wf_name + "_wf"
            workflow_file = wf_name + "_wf.xml"

            workflow_name_adhoc = wf_name + "_batch_wf"
            workflow_file_adhoc = wf_name + "_batch_wf.xml"
        else:
            workflow_name = wf_name + "_wf" + "_" + str(cnt)
            workflow_file = wf_name + "_wf" + "_" + str(cnt) + ".xml"

            workflow_name_adhoc = wf_name + "_batch_wf" + "_" + str(cnt)
            workflow_file_adhoc = wf_name + "_batch_wf" + "_" + str(cnt) + ".xml"

        create_file(sys_name_l, country_l, 'wf', workflow_name)
        create_file(sys_name_l, country_l, 'adhoc', workflow_name_adhoc)

        f_source_count = open(output_folder + '/sourcecount.txt' , "a+")
        f_incremental = open(output_folder + '/' + workflow_file, "w+")
        f_adhoc = open(output_folder + '/' + workflow_file_adhoc, "w+")
        f_incremental.write(first + '\n')
        f_adhoc.write(first + '\n')
        for table in tab:
            table = table.strip()
            if len(table.strip()) == 0:
                print("ISD table name cannot be blank")
                os.remove('./running.script')
                sys.exit(1)

            tbl = config_df[config_df["isd"] == table]
            print(tbl)
            DATABASE = str(lower(tbl["database"].tolist()[0])).strip()
            table_name = str(lower(tbl["table_name"].tolist()[0])).strip()
            INC_WHERE = str(lower(tbl["inc_filtercondition"].tolist()[0])).strip()
            HIS_WHERE = str(lower(tbl["batch_filtercondition"].tolist()[0])).strip()
            output_file_name = '_'.join([country_u, sys_name_u, table.upper()])

            create_file(sys_name_l, country_l, 'table', output_file_name.lower())
            f_source_count.write(
                "select '" + output_file_name.upper() + "',COUNT(1),'${businessday}' FROM ${aml_sri_open}." +
                table_name.lower() + " WHERE " + INC_WHERE.lower() + " UNION ALL \n ")

            print("Table Name : " + table)
            f_incremental.write(header + '\n')
            f_adhoc.write(header + '\n')

            col_list = get_ISD_Col((isd.sheet_by_name(table)))
            print("No of columns: %i\n" % len(col_list))
            all_cols = ','.join(col_list)
            for col in col_list:
                f_incremental.write(cols.replace('COLNAME', col) + '\n')
                f_adhoc.write(cols.replace('COLNAME', col) + '\n')

            inc_path_value = path.replace('user_name', USER_NAME).replace('output_file_name', output_file_name).replace(
                'db_name', DATABASE).replace('table_name', table_name).replace('var_column_order', all_cols).replace(
                'WHERE_CLAUSE', INC_WHERE)

            hist_path_value = path.replace('user_name', USER_NAME).replace('output_file_name', output_file_name).replace(
                'db_name', DATABASE).replace('table_name', table_name).replace('var_column_order', all_cols).replace(
                'WHERE_CLAUSE', HIS_WHERE)

            f_incremental.write(inc_path_value + '\n')
            f_adhoc.write(hist_path_value + '\n')


        trailer_value_inc = trailer.replace('dest_path', DEST_PATH).replace('wrk_name', workflow_name).\
            replace('db_id', DB_ID).replace('storage', STORAGE)

        trailer_value_adhoc = trailer.replace('dest_path', DEST_PATH).replace('wrk_name', workflow_name_adhoc). \
            replace('db_id', DB_ID).replace('storage', STORAGE)

        f_incremental.write(trailer_value_inc)
        f_adhoc.write(trailer_value_adhoc)
        f_incremental.close()
        f_adhoc.close()
        f_source_count.close()


# Create count script
def create_source_count_file(file_name):
    f = open(output_folder + "/" + file_name,"w")
    count_template = "./config/source_count_sample.sh"
    r = open(count_template,"r")
    q_list = open(output_folder + '/sourcecount.txt', "r").readlines()
    query = ''.join(q_list)

    for line in r.readlines():
        line = line.replace('COUNRTY_TMP_L',country_l).replace('COUNRTY_TMP_U',country_u)\
            .replace('SRC_TMP_U',sys_name_u.upper()).replace('SRC_TMP_L',sys_name_l)\
            .replace('PRD_DB', DATABASE.upper())\
            .replace('QUERY', rreplace(query, "UNION ALL" ,";" ,1).rstrip('\n'))
        f.write(line)
    f.close()
    r.close()
    os.remove(output_folder + '/sourcecount.txt')

# Method to create the SIT Control m Draft
def CreateControlM():
    with open("./config/ControlM.xml") as f:
        newText = f.read().replace('<CC_U>', country_u).replace('<CC_L>', country_l). \
            replace('<SRC_U>', sys_name_u).replace('<SRC_L>', sys_name_l)

    with open(output_folder + "/EDM_AML_" + country_u + "_" + sys_name_u + ".xml", "w") as f:
        f.write(newText)

# Method to create the logs
def logger(onConsole, msg):
    date = datetime.datetime.today().strftime('%Y-%m-%d')
    f = open("log/" + date + "_" +country_u + "_" + sys_name_u + "_" + ".log", "a+")
    f.write(msg + "\n")

    if (onConsole):
        print(msg)

    f.close()

def getCols(input):
    input = input.lower().replace(" then","#").replace(" null","#").replace(" else","#").replace(" end","#").\
        replace(" between","").replace(" and ","#").replace(" ","").replace("${startdate}","").\
        replace("${businessday}","#").replace(",'_','-')","#").replace(",'-','-')","#").replace("=","#").\
        replace("regexp_replace(","#").replace("cast(","#").replace("to_date(","#").replace("asvarchar(","#").\
        replace(")","#").replace("'","").replace("coalesce(","#").replace("casewhen","#").replace("year(","#").\
        replace(",","#").replace("concat_ws(","#").replace("|","#").replace("date(","").replace("left(","")

    cols = [x.strip() for x in input.split("#") if x.strip() != "" and not x.isdigit()]
    return cols

def getInserScripts(tbl,col):
    script = "insert into table <tblname>(<collst>) values(<vallst>);"

    vallst = []
    for x in col:
        vallst.append("<DT>")

    return script.replace("<tblname>",tbl).replace("<collst>",",".join(col)).replace("<vallst>",",".join(vallst))

def getSelectScripts(tbl,where):
    return "select '" + tbl + "',count(*) from " + tbl + " where " + where + " union all"

def createInsert(config_df):
    col_lst = config_df[["table_name", "inc_filtercondition", "batch_filtercondition"]]
    tuples = [tuple(x) for x in col_lst.values]

    formated_tupels = []
    inc_dict = {}
    batch_dict = {}
    for data in tuples:
        if " from " in data[2].lower():
            tblname = data[0]
            incremental = data[1]
            batch = data[2].lower().split(" in")[0]

            formated_tupels.append((tblname, incremental, batch))

            inner = data[2].lower().split(" in")[1].split("between")[0].replace("select", "").replace("(", "")
            innercollst = inner[:inner.find('from')].replace(" ", "")
            innertable = inner.split(".")[1].split(" ")[0].strip()
            formated_tupels.append((innertable, "", innercollst))
        else:
            formated_tupels.append(data)

    for data in formated_tupels:
        tblname = data[0]
        incremental = data[1]
        batch = data[2]

        if len(incremental) > 0:
            if tblname in inc_dict:
                for x in getCols(incremental):
                    inc_dict[tblname.lower().strip()].append(x)
            else:
                inc_dict[tblname.lower().strip()] = getCols(incremental)

        if len(batch) > 0:
            if tblname in batch_dict:
                for x in getCols(batch):
                    batch_dict[tblname.lower().strip()].append(x)
            else:
                batch_dict[tblname.lower().strip()] = getCols(batch)

    inc_insert = []
    batch_insert = []

    inc_select = []
    batch_select = []

    for tbl in inc_dict:
        inc_insert.append(getInserScripts(tbl, list(set(inc_dict[tbl]))))

    for tbl in batch_dict:
        batch_insert.append(getInserScripts(tbl, list(set(batch_dict[tbl]))))

    index = 1
    leng = len(tuples)

    for data in tuples:
        tblname = data[0]
        incremental = data[1]
        batch = data[2]

        if leng == index:
            inc_select.append(getSelectScripts(tblname, incremental).replace("union all",";"))
            batch_select.append(getSelectScripts(tblname, batch).replace("union all",";"))
        else:
            inc_select.append(getSelectScripts(tblname, incremental))
            batch_select.append(getSelectScripts(tblname, batch))
        index += 1

    with open(output_folder + "/Insert_Select.txt", "w") as f:

        f.write("\n".join(inc_insert))
        f.write("\n\n")
        f.write("\n".join(batch_insert))
        f.write("\n-------------------------------------\n")

        f.write("\n".join(inc_select))
        f.write("\n\n")
        f.write("\n".join(batch_select))

    return


def createDevSteps():

    wflst = []
    deletelst_haas = []
    createlst_haas = []
    deploylst_haas = []
    mrGetlst_haas = []
    mrPutlst_haas = []
    mrlst_haas = []

    for file in glob.glob(output_folder + "/*.xml"):
        wflst.append(file.split(".")[1].split("\\")[1])


    for wf in wflst:
        deletelst_haas.append("./client.sh config -entity workflow -delete " + wf)
        createlst_haas.append(
            "./client.sh config -entity workflow -create /CTRLFW/PSAIL_SIT/" + sys_name_u + "/processing/config/" + wf + ".xml")
        deploylst_haas.append("./client.sh execution -deploy --workflow " + wf + " --write_files")
        mrGetlst_haas.append("hadoop fs -get /apps/edmhdpef/workflows/" + wf + "/" + wf + "-src.hql")
        mrPutlst_haas.append("hadoop fs -put -f " + wf + "-src.hql /apps/edmhdpef/workflows/" + wf + "/")

    mrlst_haas.append(mr_change.replace("<SRC_L>", sys_name_l).replace("<CC_L>", country_l).
                          replace("<MR_GET>", "\n".join(mrGetlst_haas)).replace("<MR_PUT>", "\n".join(mrPutlst_haas)))

    with open("./config/runner.txt") as f:
        HaasText = f.read().replace("<WF_DELETE>","\n".join(deletelst_haas)).\
            replace("<WF_CREATE>","\n".join(createlst_haas)).\
            replace("<WF_DEPLOY>","\n".join(deploylst_haas)).\
            replace("<MR_CHANGE>","\n".join(mrlst_haas)).\
            replace("<CTRY_U>",country_u).\
            replace("<CTRY_L>",country_l).\
            replace("<SRC_U>",sys_name_u).\
            replace("<SRC_L>",sys_name_l)
    with open(output_folder + "/Steps.txt", "w") as f:
        f.write(HaasText)

    return



if __name__ == '__main__':
    output_folder = "./output/" + country_u + '_' + sys_name_u

    if os.path.exists("./running.script"):
        logger(True, "Previous run not completed yet")
        sys.exit()
    else:
        open('./running.script', "a").close()
        logger(True, "Starting the process")

    logger(True, "Reading the Info xls")
    if os.path.exists("./input/" + sys_name_u + "_" + country_u + "/" + sys_name_u + "_" + country_u + "_Info.xls"):
        config_df = pd.read_excel("./input/" + sys_name_u + "_" + country_u + "/" + sys_name_u + "_"
                                  + country_u + "_Info.xls")

        if len(config_df.index.values) == 0:
            logger(True, "No records found in the info xls")
            os.remove('./running.script')
            sys.exit()

        if os.path.exists("./input/" + sys_name_u + "_" + country_u + "/" + sys_name_u + "_" + country_u + ".xls"):
            isd = xlrd.open_workbook("./input/" + sys_name_u + "_" + country_u + "/" + sys_name_u + "_" + country_u
                                     + ".xls")
        else:
            logger(True, "could not find the ISD file")
            os.remove('./running.script')
            sys.exit()

        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
            os.mkdir(output_folder)
        elif not os.path.exists(output_folder):
            os.mkdir(output_folder)

        logger(True, "Starting the xml creation for " + str(len(config_df.index.values)) + " tables")

        isd_master_tbl_list = config_df[config_df["table_type"] == "Master"]
        isd_tran_tbl_list = config_df[config_df["table_type"] == "Transaction"]

        logger(True, "Number of Master tables :" + str(len(isd_master_tbl_list)))
        logger(True, "Number of transaction tables :" + str(len(isd_tran_tbl_list)))

        if len(isd_master_tbl_list) > 0:
            create_xml(isd_master_tbl_list, isd, "master")

        if len(isd_tran_tbl_list) > 0:
            create_xml(isd_tran_tbl_list, isd, "txn")

        createDevSteps()
        logger(True, "Workflow creation completed")
        logger(True, "Starting the process to create the source count file")
        count_file_name = ("aml_%s_%s_source_count.sh" % (country_l, sys_name_l)).lower()
        create_source_count_file(count_file_name)
        logger(True, "Source count file created")

        logger(True, "Creating the Control M job")
        CreateControlM()
        logger(True,"Control m job created")

        logger(True, "Creating the Insert and select scripts")
        createInsert(config_df)
        logger(True, "Insert and select scripts created")

    else:
        logger(True, "info xls not found")
        sys.exit()

    os.remove('./running.script')
    print("*********** Script Completed **********")
