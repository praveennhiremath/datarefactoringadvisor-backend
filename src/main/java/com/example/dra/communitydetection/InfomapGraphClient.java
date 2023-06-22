package com.example.dra.communitydetection;


import oracle.pg.rdbms.AdbGraphClient;
import oracle.pg.rdbms.AdbGraphClientConfiguration;
import oracle.pgx.api.*;
import oracle.pgx.common.types.PropertyType;

import java.util.concurrent.ExecutionException;

public class InfomapGraphClient {


	public void createGraph1(PgxSession session) {


	}

	public void createGraph(String stsName) throws ExecutionException, InterruptedException {
		System.out.println("In createGraph Method");
		//FileProperties properties = new FileProperties();
		AdbGraphClientConfiguration.AdbGraphClientConfigurationBuilder config = AdbGraphClientConfiguration.builder();
		config.tenant("ocid1.tenancy.oc1..aaaaaaaabls4dzottlktt774tu3knax6crpozycjhrqpm73thfryxwlkmkba");
		config.database("medicalrecordsdb");
		config.cloudDatabaseName("ocid1.autonomousdatabase.oc1.iad.anuwcljrq33dybyakudj7we7dhwckr7wioxeponx2ptjt4g7lyx4mdvvmqyq");
		config.username("ADMIN");
		config.password("Welcome12345");
		config.endpoint("https://bsenjiat5lmurtq-medicalrecordsdb.adb.us-ashburn-1.oraclecloudapps.com/");

		var client = new AdbGraphClient(config.build());
		System.out.println("Client Created using config");
		if (!client.isAttached()) {
			var job = client.startEnvironment(10);
			job.get();
			System.out.println("Job Details: Job Name=" + job.getName() + ", Job Created By= " + job.getCreatedBy());
		}

		ServerInstance instance = client.getPgxInstance();
		System.out.println("Creating Session");
		PgxSession session = instance.createSession(Constants.SESSION_NAME_STR);
		System.out.println("Session Created");

		String statement = "CREATE PROPERTY GRAPH DRA_DEMO_GRAPH\n" +
				"  VERTEX TABLES (\n" +
				"    ADMIN.NODES\n" +
				"      KEY ( table_name )\n" +
				"      PROPERTIES ( schema, tables_joined, table_id, table_name, table_set_name, total_executions, total_sql )\n" +
				"  )\n" +
				"  EDGE TABLES (\n" +
				"    ADMIN.EDGES\n" +
				"      SOURCE KEY ( table1 ) REFERENCES NODES\n" +
				"      DESTINATION KEY ( table2 ) REFERENCES NODES\n" +
				"      PROPERTIES ( dynamic_coefficient, join_count, join_executions, static_coefficient, table1, table2, table_map_id, table_set_name, total_affinity, total_affinity_modified )\n" +
				"  )";

		PgqlResultSet exeRes = session.executePgql(statement);
		System.out.println("GRAPH NAME :: " + exeRes.getGraph().getName());


		/*PgxGraph g = session.getGraph("DRA_DEMO_GRAPH");

		PgxGraph graph = session.readGraphByName("DRA_DEMO_GRAPH", oracle.pgx.api.GraphSource.PG_VIEW);
		// PgxGraph graph = session.readGraphWithProperties(graphConfig);

		System.out.println("Graph : " + graph);

		Analyst analyst = session.createAnalyst();

		// Default Max Iteration for Infomap is set to 1.
		int maxIterations = 1;
		String targetCommunityTableName = null;

		EdgeProperty<Double> weight = graph.getEdgeProperty(properties.readGraphProperty(Constants.EDGE_WEIGHT_COL_STR));
		try {
			VertexProperty<Integer, Double> rank = analyst.weightedPagerank(graph, 1e-16, 0.85, 1000, true, weight);
			VertexProperty<Integer, Long> module = graph.createVertexProperty(PropertyType.LONG, Constants.COMMUNITY_STR);
			System.out.println("Calling Infomap with Max Iterations = " + maxIterations);
			Partition<Integer> promise = analyst.communitiesInfomap(graph, rank, weight, 0.15, 0.0001, maxIterations, module);
			graph.queryPgql("SELECT n." + Constants.COMMUNITY_STR + ",n.TABLE_NAME FROM MATCH (n) order by n." + Constants.COMMUNITY_STR + "").print().close();

		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		session.close();*/

	}
}
