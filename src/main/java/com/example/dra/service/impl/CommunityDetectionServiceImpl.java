package com.example.dra.service.impl;

import com.example.dra.bean.DatabaseDetails;
import com.example.dra.communitydetection.InfomapGraphClient;
import com.example.dra.service.CommunityDetectionService;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class CommunityDetectionServiceImpl implements CommunityDetectionService {


    @Override
    public void communityDetection(DatabaseDetails databaseDetails) {
        try {
            new InfomapGraphClient().createGraph("abc");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
