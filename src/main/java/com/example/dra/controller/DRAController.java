package com.example.dra.controller;

import com.example.dra.bean.DatabaseDetails;
import com.example.dra.dto.Tables18NodesDto;
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
	
}
