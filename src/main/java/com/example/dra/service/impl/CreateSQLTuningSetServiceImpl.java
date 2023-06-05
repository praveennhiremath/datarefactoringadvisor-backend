package com.example.dra.service.impl;

import com.example.dra.bean.DatabaseDetails;
import com.example.dra.dto.Tables18NodesDto;
import com.example.dra.repository.Tables18NodesRepository;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Service;

import com.example.dra.entity.Tables18NodesEntity;
import com.example.dra.service.CreateSQLTuningSetService;

import jakarta.annotation.PostConstruct;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class CreateSQLTuningSetServiceImpl implements CreateSQLTuningSetService {

	@Resource
	Tables18NodesRepository tables18NodesRepository;

	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	private SimpleJdbcCall simpleJdbcCall;

	@Value("${spring.datasource.password}")
	String PASSWORD;
	
	private static final String SQL_STORED_PROC = ""
            + " CREATE OR REPLACE PROCEDURE get_book_by_id2 "
            + " ("
            + "  table_name OUT TABLES_18_NODES.TABLE_NAME%TYPE,"
            + " ) AS"
            + " BEGIN"
            + "  SELECT TABLE_NAME INTO table_name from TABLES_18_NODES;"
            + " END;";
	
	//private static final String SQL_STORED_PROC_STS = "CALL DBMS_SQLTUNE.create_sqlset(sqlset_name => '6STS')";

	@Override
	public Tables18NodesEntity createSqlSet(String sqlSetName) {
		
		jdbcTemplate.setResultsMapCaseInsensitive(true);
		
	//	System.out.println("Creating Store Procedures and Function...");
     //   jdbcTemplate.execute(SQL_STORED_PROC);

     //   simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate).withProcedureName("get_book_by_id");
        
		 System.out.println("Creating SQL SET Procedure");
		 //jdbcTemplate.execute(SQL_STORED_PROC_STS);
        
		return null;
	}
	
	public void start() {
		Tables18NodesEntity test = createSqlSet("anc");
	}
	
    @PostConstruct
    void init() {
        jdbcTemplate.setResultsMapCaseInsensitive(true);
        simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate).withProcedureName("get_book_by_id");
    }

	@Override
	public List<Tables18NodesDto> getTableName() {

		List<Tables18NodesDto> tables18NodesDtos = new ArrayList<>();
		Tables18NodesDto tables18NodesDto = new Tables18NodesDto();
		tables18NodesDto.setTable_name("table1");
		tables18NodesDtos.add(tables18NodesDto);
		tables18NodesDto = new Tables18NodesDto();
		tables18NodesDto.setTable_name("table2");
		tables18NodesDtos.add(tables18NodesDto);

		List<Tables18NodesEntity> tables18NodesEntities = tables18NodesRepository.findAll();

		for(Tables18NodesEntity tables18NodesEntity : tables18NodesEntities){
			System.out.println("Table Name :: " + tables18NodesEntity.getTableName());
		}


		return tables18NodesDtos;
	}



	@Override
	public String createSQLTuningSet(DatabaseDetails databaseDetails) {

		String CLOUD_DB_URL_STR = "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)\n" +
				"(port="+databaseDetails.getPort()+")(host="+databaseDetails.getHostname()+"))\n" +
				"(connect_data=(service_name="+databaseDetails.getServiceName()+"))\n" +
				"(security=(ssl_server_dn_match=yes)))";
		System.out.println(CLOUD_DB_URL_STR);

		//String url = "jdbc:oracle:thin:@"+databaseDetails.getDatabaseName()+ "_tp?tns_admin=C:/Oracle/atp";
		Connection connection = null;
		CallableStatement callableStatement = null;
		boolean result = false;
		try {
			// Establishing a connection to the database
			System.out.println("Password :: " + PASSWORD);
			connection = DriverManager.getConnection(CLOUD_DB_URL_STR, databaseDetails.getUsername(), PASSWORD);
			System.out.println("Connection :: " + connection.getClientInfo());
			//connection = DriverManager.getConnection(url, username, password);
			// Creating a CallableStatement for invoking DBMS_SQLTUNE
			String SQL_STORED_PROC_STS
					= "CALL DBMS_SQLTUNE.create_sqlset(sqlset_name => '"+databaseDetails.getSqlSetName()+"')";
			callableStatement = connection.prepareCall(SQL_STORED_PROC_STS);
			result = callableStatement.execute();
			System.out.println("result :: " + result);
			//System.out.println("Connection Result :: "+callableStatement.getInt(1));
			if(!result) {
				return "SQL TUNING SET CREATED";
			} else {
				return "SQL TUNING SET NOT CREATED";
			}

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

		if(result) {
			return "SQL TUNING SET CREATED";
		} else {
			return "SQL TUNING SET NOT CREATED";
		}
	}

	@Override
	public String loadSQLTuningSet(DatabaseDetails databaseDetails) {

		String CLOUD_DB_URL_STR = "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)\n" +
				"(port="+databaseDetails.getPort()+")(host="+databaseDetails.getHostname()+"))\n" +
				"(connect_data=(service_name="+databaseDetails.getServiceName()+"))\n" +
				"(security=(ssl_server_dn_match=yes)))";
		System.out.println(CLOUD_DB_URL_STR);

		//String url = "jdbc:oracle:thin:@"+databaseDetails.getDatabaseName()+ "_tp?tns_admin=C:/Oracle/atp";
		Connection connection = null;
		CallableStatement callableStatement = null;
		String resultLoadSts = "";
		try {
			// Establishing a connection to the database
			System.out.println("Password :: " + PASSWORD);
			connection = DriverManager.getConnection(CLOUD_DB_URL_STR, databaseDetails.getUsername(), PASSWORD);
			//connection = DriverManager.getConnection(url, username, password);
			// Creating a CallableStatement for invoking DBMS_SQLTUNE
			String SQL_STORED_PROC_STS
					= "CALL DBMS_SQLTUNE.create_sqlset(sqlset_name => '"+databaseDetails.getSqlSetName()+"')";
			callableStatement = connection.prepareCall(SQL_STORED_PROC_STS);
			callableStatement.execute();

			String resultExecute = executeQueries(databaseDetails);

			resultLoadSts = LoadSQLTuningSet(databaseDetails);
			System.out.println("resultLoadSts :: " + resultLoadSts);
			return resultLoadSts;
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

		return resultLoadSts;
	}

	private String executeQueries(DatabaseDetails databaseDetails) {

		String CLOUD_DB_URL_STR = "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)\n" +
				"(port="+databaseDetails.getPort()+")(host="+databaseDetails.getHostname()+"))\n" +
				"(connect_data=(service_name="+databaseDetails.getServiceName()+"))\n" +
				"(security=(ssl_server_dn_match=yes)))";

		//String url = "jdbc:oracle:thin:@"+databaseDetails.getDatabaseName()+ "_tp?tns_admin=C:/Oracle/atp";
		Connection connection = null;
		CallableStatement callableStatement = null;

		try {
			// Establishing a connection to the database
			connection = DriverManager.getConnection(CLOUD_DB_URL_STR, databaseDetails.getUsername(), PASSWORD);

			Statement s = connection.createStatement();
			for (String query : databaseDetails.getQueries()) {
				s.addBatch(query);
			}
			s.executeBatch();


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
		return "Queries Executed";

	}

	public String LoadSQLTuningSet(DatabaseDetails databaseDetails) {

		String CLOUD_DB_URL_STR = "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)\n" +
				"(port="+databaseDetails.getPort()+")(host="+databaseDetails.getHostname()+"))\n" +
				"(connect_data=(service_name="+databaseDetails.getServiceName()+"))\n" +
				"(security=(ssl_server_dn_match=yes)))";

		String LOAD_STS_PROC = "declare\n" +
				"mycur dbms_sqltune.sqlset_cursor;\n" +
				"begin\n" +
				"open mycur for\n" +
				"select value (P)\n" +
				"from table" +
				"(dbms_sqltune.select_cursor_cache('parsing_schema_name <> ''"+databaseDetails.getUsername()+"'' " +
				"and elapsed_time > 250', null, null, null, null,1, null, 'ALL')) P;\n" +
				"dbms_sqltune.load_sqlset(sqlset_name => '"+databaseDetails.getSqlSetName()+"', " +
				"populate_cursor => mycur," +
				"sqlset_owner => '"+databaseDetails.getUsername()+"');\n" +
				"end;\n";

		Connection connection = null;
		CallableStatement callableStatement = null;

		try {
			// Establishing a connection to the database
			connection = DriverManager.getConnection(CLOUD_DB_URL_STR, databaseDetails.getUsername(), PASSWORD);
			// Creating a CallableStatement for invoking DBMS_SQLTUNE
			callableStatement = connection.prepareCall(LOAD_STS_PROC);
			callableStatement.execute();
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
		return "Loaded STS Successfully";
	}

}
