package com.example.dra.service.impl;

import com.example.dra.bean.DatabaseDetails;
import com.example.dra.service.GraphConstructorService;
import com.example.dra.utils.DBUtils;
import org.springframework.beans.factory.annotation.Value;

import java.sql.*;


public class GraphConstructorServiceImpl implements GraphConstructorService {

    @Value("${spring.datasource.password}")
    String PASSWORD1;

    public String processSTSMetadata(DatabaseDetails databaseDetails) {
        return constructGraph(databaseDetails);
    }

    @Override
    public String constructGraph(DatabaseDetails databaseDetails) {

        System.out.println("In constructGraph");
        // create metadata tables
        createMetadataTables(databaseDetails);

        // Populate Data into Nodes table
        String populateNodesResult = populateDataInNodesTable(databaseDetails);

        // Create Helper view, helpful for calculating affinities
        String createHelperViewResult = createHelperViewForAffinityCalculation(databaseDetails);

        // Calculate affinities
        int createCompAffinityProcResult = createComputeAffinityProcedure(databaseDetails);

        executeProcedure(databaseDetails, createCompAffinityProcResult);
        return "GRAPH CONSTRUCTED";
    }

    public boolean createMetadataTables(DatabaseDetails databaseDetails) {
        boolean isNodesTableExists = checkTableIfExists(databaseDetails, "nodes");
        boolean isEdgesTableExists = checkTableIfExists(databaseDetails, "edges");

        System.out.println("isNodesTableExists :: " + isNodesTableExists);
        System.out.println("isEdgesTableExists :: " + isEdgesTableExists);

        boolean result = true;
        // TODO: Remove the creation of Nodes and Edges table in each run, create it as Application tables(Only once)
        if (!isNodesTableExists) {
            //create "Nodes" table
            System.out.println("Creating NODES Table...");
            String createNodeTableQuery = "create table nodes \n" +
                    "( table_set_name       varchar2(128)\n" +
                    ", schema               varchar2(128)\n" +
                    ", table_name           varchar2(128)\n" +
                    ", total_sql            number(10)\n" +
                    ", total_executions     number(10)\n" +
                    ", tables_joined        number(10))";
            result = executeSQLQuery(databaseDetails, createNodeTableQuery);
        }
        if (!isEdgesTableExists) {
            //create "Edges" table
            System.out.println("Creating EDGES Table...");
            String createEdgeTableQuery ="create table edges \n" +
                    "( table_set_name       varchar2(128)\n" +
                    ", table1               varchar2(128)\n" +
                    ", schema1              varchar2(128)\n" +
                    ", table2               varchar2(128)\n" +
                    ", schema2              varchar2(128)\n" +
                    ", join_count           number(10)\n" +
                    ", join_executions      number(10)\n" +
                    ", static_coefficient   decimal(10,5)\n" +
                    ", dynamic_coefficient  decimal(10,5)\n" +
                    ", total_affinity       decimal(10,5))";
            result = executeSQLQuery(databaseDetails, createEdgeTableQuery);
        }
        return result;
    }

    private boolean checkTableIfExists(DatabaseDetails databaseDetails, String tableName) {
        String query = "select count(*) from user_tables where table_name='"+tableName+"';";
        String dbUrlConnectionStr = DBUtils.formDbConnectionStr(databaseDetails);

        Connection connection = null;
        try {
            // Establishing a connection to the database
            connection = DriverManager.getConnection(dbUrlConnectionStr, databaseDetails.getUsername(), databaseDetails.getPassword());
            Statement s = connection.createStatement();
            ResultSet result = s.executeQuery(query);
            return result.getInt(1) != 0;
        } catch (SQLException e) {
            System.out.println("SQLException, Error Code :: "+e.getErrorCode());
            e.printStackTrace();
        } finally {
            // Closing the resources
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private boolean executeSQLQuery(DatabaseDetails databaseDetails, String query) {

        String dbUrlConnectionStr = DBUtils.formDbConnectionStr(databaseDetails);
        //System.out.println(dbUrlConnectionStr);

        Connection connection = null;
        boolean result = true;
        try {
            // Establishing a connection to the database
            connection = DriverManager.getConnection(dbUrlConnectionStr, databaseDetails.getUsername(), databaseDetails.getPassword());
            Statement s = connection.createStatement();
            result = s.execute(query);
        } catch (SQLException e) {
            System.out.println("SQLException, Error Code :: "+e.getErrorCode());
            e.printStackTrace();
        } finally {
            // Closing the resources
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return !result;
    }

    private int executeUpdateSQLQuery(DatabaseDetails databaseDetails, String query) {

        String dbUrlConnectionStr = DBUtils.formDbConnectionStr(databaseDetails);
        System.out.println(dbUrlConnectionStr);

        Connection connection = null;
        CallableStatement callableStatement = null;
        int result = 0;
        try {
            // Establishing a connection to the database

            connection = DriverManager.getConnection(dbUrlConnectionStr, databaseDetails.getUsername(), databaseDetails.getPassword());
            Statement s = connection.createStatement();
            result = s.executeUpdate(query);
        } catch (SQLException e) {
            System.out.println("SQLException, Error Code :: "+e.getErrorCode());
            e.printStackTrace();
        } finally {
            // Closing the resources
            try {
                if (callableStatement != null) {
                    callableStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public String populateDataInNodesTable(DatabaseDetails databaseDetails) {

        String sqlSetName = databaseDetails.getSqlSetName();
        String username = databaseDetails.getUsername();
        String INSERT_INTO_NODES_QUERY =
                "insert into nodes (table_set_name, schema, table_name, total_sql, total_executions) \n" +
                "select table_set_name, table_owner, table_name, count(distinct sql_id), sum(executions)\n" +
                "from ( \n" +
                "    select distinct table_set_name, table_owner, table_name, sql_id, executions \n" +
                "    from (\n" +
                "        select '"+sqlSetName+"' table_set_name,\n" +
                "            case when v.operation='INDEX' then v.TABLE_NAME\n" +
                "                 when v.operation='TABLE ACCESS' then v.object_name\n" +
                "                 else NULL end table_name,\n" +
                "            v.object_owner as table_owner,\n" +
                "            v.sql_id,\n" +
                "            v.executions\n" +
                "        from (\n" +
                "            select p.object_name, p.operation, p.object_owner, \n" +
                "                p.sql_id, p.executions, i.table_name\n" +
                "            from dba_sqlset_plans p, all_indexes i\n" +
                "            where p.object_name=i.index_name(+) \n" +
                "            and sqlset_name='"+sqlSetName+"'\n" +
                "            and object_owner = upper('"+username+"')\n" +
                "        ) v  \n" +
                "    )\n" +
                ") \n" +
                "group by table_set_name, table_owner, table_name\n" +
                "having table_name is not null";
        executeSQLQuery(databaseDetails, INSERT_INTO_NODES_QUERY);
        return "NODES DATA POPULATED";
    }



    @Override
    public String populateDataInEdgesTable(DatabaseDetails databaseDetails) {
        return "EDGES DATA POPULATED";
    }

    @Override
    public String createHelperViewForAffinityCalculation(DatabaseDetails databaseDetails) {

        String CREATE_HELPER_VIEW_PROC = "create view tableset_sql as \n" +
                "select distinct table_name, sql_id \n" +
                "from (\n" +
                "    select '"+databaseDetails.getSqlSetName()+"' table_set_name,\n" +
                "    case when v.operation='INDEX' then v.TABLE_NAME\n" +
                "        when v.operation='TABLE ACCESS' then v.object_name\n" +
                "        else NULL end table_name,\n" +
                "    v.object_owner as table_owner,\n" +
                "    v.sql_id,\n" +
                "    v.executions\n" +
                "    from ( \n" +
                "        select p.object_name, p.operation, p.object_owner,\n" +
                "            p.sql_id, p.executions, i.table_name\n" +
                "        from dba_sqlset_plans p, all_indexes i\n" +
                "        where p.object_name=i.index_name(+) \n" +
                "        and sqlset_name='"+databaseDetails.getSqlSetName()+"' \n" +
                "        and object_owner = '"+databaseDetails.getUsername()+"'\n" +
                "    ) v\n" +
                ")";
        System.out.println("CREATE_HELPER_VIEW_PROC :: " + CREATE_HELPER_VIEW_PROC);
        executeUpdateSQLQuery(databaseDetails, CREATE_HELPER_VIEW_PROC);
        return "TABLESET_SQL HELPER VIEW CREATED";
    }


    @Override
    public int createComputeAffinityProcedure(DatabaseDetails databaseDetails) {

        String COMPUTE_AFFINITY_PROCEDURE = "create or replace procedure compute_affinity_tkdra as\n" +
                "cursor c is\n" +
                "select table_name, schema from nodes;\n" +
                "tblnm varchar2(128);\n" +
                "ins_sql varchar2(4000);\n" +
                "upd_sql varchar2(4000);\n" +
                "begin\n" +
                "    for r in c loop\n" +
                "        ins_sql:= q'{\n" +
                "            insert into "+databaseDetails.getUsername()+".edges \n" +
                "            ( table_set_name\n" +
                "            , table1\n" +
                "            , schema1\n" +
                "            , table2\n" +
                "            , schema2\n" +
                "            , join_count\n" +
                "            , join_executions\n" +
                "            , static_coefficient\n" +
                "            , dynamic_coefficient\n" +
                "            , total_affinity) \n" +
                "            select \n" +
                "                '"+databaseDetails.getSqlSetName()+"' table_set_name,\n" +
                "                tbl1, \n" +
                "                '"+databaseDetails.getUsername()+"', \n" +
                "                tbl2, \n" +
                "                '"+databaseDetails.getUsername()+"', \n" +
                "                join_count, \n" +
                "                join_executions, \n" +
                "                round(join_count/(all_sql-join_count),5) static_coefficient, \n" +
                "                round(join_executions/(all_executions-join_executions),5) dynamic_coefficient, \n" +
                "                (round(join_count/(all_sql-join_count),5)*0.5 + \n" +
                "                 round(join_executions/(all_executions-join_executions),5)*0.5) total_affinity\n" +
                "            from (\n" +
                "                select \n" +
                "                    v2.tbl1, \n" +
                "                    v2.tbl2, \n" +
                "                    (select sum(total_sql) \n" +
                "                        from nodes \n" +
                "                        where table_name=v2.tbl1 \n" +
                "                        or table_name=v2.tbl2 ) all_sql,\n" +
                "                    (select sum(total_executions) \n" +
                "                        from nodes \n" +
                "                        where table_name=v2.tbl1 \n" +
                "                        or table_name=v2.tbl2 ) all_executions,\n" +
                "                    v2.join_count, \n" +
                "                    v2.join_executions \n" +
                "                from (\n" +
                "                    select \n" +
                "                        v1.tbl1, \n" +
                "                        v1.tbl2, \n" +
                "                        count(distinct v1.sql_id) join_count, \n" +
                "                        sum(v1.executions) join_executions \n" +
                "                    from (\n" +
                "                        select distinct \n" +
                "                            v.tbl1, \n" +
                "                            case when v.operation='INDEX' then v.TABLE_NAME  \n" +
                "                                when v.operation='TABLE ACCESS' then v.tbl2 \n" +
                "                                else NULL end tbl2,\n" +
                "                            sql_id,\n" +
                "                            executions \n" +
                "                        from ( \n" +
                "                            select \n" +
                "                                '}'||r.table_name||q'{' tbl1, \n" +
                "                                s.object_name tbl2, \n" +
                "                                i.table_name table_name, \n" +
                "                                sql_id, \n" +
                "                                operation, \n" +
                "                                executions \n" +
                "                            from dba_sqlset_plans s, all_indexes i \n" +
                "                            where sqlset_name='"+databaseDetails.getSqlSetName()+"' \n" +
                "                            and object_owner=upper('"+databaseDetails.getUsername()+"') \n" +
                "                            and s.object_name = i.index_name(+) \n" +
                "                            and sql_id in (\n" +
                "                                select distinct sql_id \n" +
                "                                from dba_sqlset_plans \n" +
                "                                where sqlset_name='"+databaseDetails.getSqlSetName()+"' \n" +
                "                                and object_name='}'||r.table_name||q'{' \n" +
                "                                and  object_owner=upper('"+databaseDetails.getUsername()+"')\n" +
                "                            ) \n" +
                "                        ) v \n" +
                "                    ) v1  \n" +
                "                    group by v1.tbl1, v1.tbl2   \n" +
                "                    having v1.tbl2 is not null \n" +
                "                    and v1.tbl1 <> v1.tbl2 \n" +
                "                ) v2 \n" +
                "            )\n" +
                "        }';\n" +
                "        execute immediate ins_sql;\n" +
                "\n" +
                "        upd_sql:= q'{\n" +
                "            update "+databaseDetails.getUsername()+".nodes \n" +
                "            set tables_joined=(select count(distinct table_name) \n" +
                "            from (\n" +
                "                select \n" +
                "                    '"+databaseDetails.getSqlSetName()+"' table_set_name,\n" +
                "                    case when v.operation='INDEX' then v.TABLE_NAME \n" +
                "                        when v.operation='TABLE ACCESS' then v.object_name \n" +
                "                        else NULL end table_name,\n" +
                "                    v.object_owner as table_owner,\n" +
                "                    v.sql_id, \n" +
                "                    v.executions \n" +
                "                from ( \n" +
                "                    select \n" +
                "                        p.object_name, \n" +
                "                        p.operation, \n" +
                "                        p.object_owner, \n" +
                "                        p.sql_id, \n" +
                "                        p.executions, \n" +
                "                        i.table_name \n" +
                "                    from dba_sqlset_plans p, all_indexes i \n" +
                "                    where p.object_name=i.index_name(+) \n" +
                "                    and sqlset_name='"+databaseDetails.getUsername()+"' \n" +
                "                    and sql_id in (\n" +
                "                        select sql_id \n" +
                "                        from tableset_sql \n" +
                "                        where table_name='}'||r.table_name||q'{') \n" +
                "                        and object_owner = upper('"+databaseDetails.getUsername()+"')\n" +
                "                    ) v\n" +
                "                )\n" +
                "            ) where table_name ='}' || r.table_name || q'{'\n" +
                "        }';\n" +
                "        execute immediate upd_sql;\n" +
                "    end loop;\n" +
                "end;";
        System.out.println("COMPUTE_AFFINITY_PROCEDURE :: " + COMPUTE_AFFINITY_PROCEDURE);
        int result = executeUpdateSQLQuery(databaseDetails, COMPUTE_AFFINITY_PROCEDURE);
        return result;
    }

    public String executeProcedure(DatabaseDetails databaseDetails, int procedure) {

        String dbUrlConnectionStr = DBUtils.formDbConnectionStr(databaseDetails);
        System.out.println(dbUrlConnectionStr);

        String SQL_STORED_PROC_STS = "CALL compute_affinity_tkdra()";
        Connection connection = null;
        CallableStatement callableStatement = null;
        int result = 0;
        try {
            // Establishing a connection to the database
            connection = DriverManager.getConnection(dbUrlConnectionStr, databaseDetails.getUsername(), databaseDetails.getPassword());
            callableStatement = connection.prepareCall(SQL_STORED_PROC_STS);
            result = callableStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Closing the resources
            try {
                if (callableStatement != null) {
                    callableStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(result==0) {
            return "Procedure 'compute_affinity_tkdra' executed Successfully";
        } else {
            return "Error in excuting Procedure 'compute_affinity_tkdra'";
        }
    }
}
