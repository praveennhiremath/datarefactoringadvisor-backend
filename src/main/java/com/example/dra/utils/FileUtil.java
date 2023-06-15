package com.example.dra.utils;

import com.example.dra.bean.DatabaseDetails;
import com.example.dra.service.impl.CreateSQLTuningSetServiceImpl;
import com.example.dra.service.impl.GraphConstructorServiceImpl;
import org.apache.tomcat.util.codec.binary.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FileUtil {

    @Autowired
    ResourceLoader resourceLoader;

    public static List<String> readLoadStsSimulateFile(){
        Resource resource = new ClassPathResource("workload-simulate-queries.sql");
        //Resource resource=resourceLoader.getResource("classpath:workload-simulate-query.sql");
        List<String> lines = new ArrayList<>();
        try {
            File file = resource.getFile();
            lines = Files.readAllLines(Path.of(file.getPath()));
            lines.removeIf(s -> s.startsWith("--"));
            lines.removeIf(s -> s.isEmpty());
            System.out.println("Number of Queries :: "+lines.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lines;
    }
    public static void main(String[] args) {
        List<String> lines = new FileUtil().readLoadStsSimulateFile();
        for (String line : lines) {
            System.out.println("--- "+line);
        }

        DatabaseDetails databaseDetails = getDummyDatabaseDetailsObj();
        new CreateSQLTuningSetServiceImpl().executeQueries(databaseDetails, lines);
    }

    private static DatabaseDetails getDummyDatabaseDetailsObj() {
        DatabaseDetails databaseDetails = new DatabaseDetails();
        databaseDetails.setUrl(new DBUtils().formDbConnectionStr(databaseDetails));
        databaseDetails.setDatabaseName("medicalrecordsdb");
        databaseDetails.setPort(1511);
        databaseDetails.setHostname("adb.us-ashburn-1.oraclecloud.com");
        databaseDetails.setUsername("ADMIN");
        databaseDetails.setServiceName("bsenjiat5lmurtq_medicalrecordsdb_high.adb.oraclecloud.com");
        return databaseDetails;
    }
}
