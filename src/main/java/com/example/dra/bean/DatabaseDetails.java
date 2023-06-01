package com.example.dra.bean;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class DatabaseDetails {

    private String databaseName;
    private String hostname;
    private int port;
    private String username;
    private String url;
    private String sqlSetName;
    private List<String> queries;

    @Override
    public String toString() {
        return "DatabaseDetails{" +
                "databaseName='" + databaseName + '\'' +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", url='" + url + '\'' +
                ", sqlSetName='" + sqlSetName + '\'' +
                '}';
    }
}
