package com.example.dra.service;

import com.example.dra.bean.DatabaseDetails;

public interface GraphConstructorService {

    public String constructGraph(DatabaseDetails databaseDetails);

    public String populateDataInNodesTable(DatabaseDetails databaseDetails);

    public String populateDataInEdgesTable(DatabaseDetails databaseDetails);

    public String createHelperViewForAffinityCalculation(DatabaseDetails databaseDetails);

    public String createComputeAffinityProcedure(DatabaseDetails databaseDetails);

    public String executeProcedure(DatabaseDetails databaseDetails, String procedure);
}
