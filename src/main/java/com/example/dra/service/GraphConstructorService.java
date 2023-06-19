package com.example.dra.service;

import com.example.dra.bean.DatabaseDetails;
import com.example.dra.entity.Edges;

import java.util.List;

public interface GraphConstructorService {

    public String constructGraph(DatabaseDetails databaseDetails);

    public String populateDataInNodesTable(DatabaseDetails databaseDetails);

    public String populateDataInEdgesTable(DatabaseDetails databaseDetails);

    public String createHelperViewForAffinityCalculation(DatabaseDetails databaseDetails);

    public int createComputeAffinityProcedure(DatabaseDetails databaseDetails);

    List<Edges> viewGraph(DatabaseDetails databaseDetails);

    //public String executeProcedure(DatabaseDetails databaseDetails, String procedure);
}
