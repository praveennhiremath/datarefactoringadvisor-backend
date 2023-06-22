package com.example.dra.controller;

import com.example.dra.ViewGraphResponse;
import com.example.dra.bean.DatabaseDetails;
import com.example.dra.dto.Tables18NodesDto;
import com.example.dra.service.CommunityDetectionService;
import com.example.dra.service.GraphConstructorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.example.dra.service.CreateSQLTuningSetService;

import java.util.List;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/api")
public class DRAController {
	
	@Autowired
	CreateSQLTuningSetService createSQLTuningSetService;

	@Autowired
	GraphConstructorService graphConstructorService;

	@Autowired
	CommunityDetectionService communityDetectionService;
	
	@GetMapping("/gettablename")
	public List<Tables18NodesDto> getTableName() {
		List<Tables18NodesDto> result = createSQLTuningSetService.getTableName();
		return result;
	}

	@PostMapping("/createsqltuningset")
	public String createSQLTuningSet(@RequestBody DatabaseDetails databaseDetails) {
		String result = createSQLTuningSetService.createSQLTuningSet(databaseDetails);
		return result;
	}

	@PostMapping("/dropsqltuningset")
	public String dropSQLTuningSet(@RequestBody DatabaseDetails databaseDetails) {
		String result = createSQLTuningSetService.dropSQLTuningSet(databaseDetails);
		return result;
	}

	@PostMapping("/loadsqltuningset")
	public String loadSQLTuningSet(@RequestBody DatabaseDetails databaseDetails) {
		String result = createSQLTuningSetService.loadSQLTuningSet(databaseDetails);
		return result;
	}

	@PostMapping("/collectsqltuningset")
	public String collectSQLTuningSet(@RequestBody DatabaseDetails databaseDetails) {
		String result = createSQLTuningSetService.collectSQLTuningSet(databaseDetails);
		return result;
	}

	@GetMapping("/getsqltuningsetlist")
	public List<String> getSQLTuningSetList() {
		DatabaseDetails databaseDetails = new DatabaseDetails();
		return createSQLTuningSetService.getSQLTuningSetList(databaseDetails);
	}

	@GetMapping("/viewgraph")
	public ViewGraphResponse viewGraph(@RequestBody DatabaseDetails databaseDetails) {
		return graphConstructorService.viewGraph(databaseDetails);
	}

	@PostMapping("/communitydetection")
	public void communityDetection(@RequestBody DatabaseDetails databaseDetails) {
		communityDetectionService.communityDetection(databaseDetails);
	}
}
