package com.example.demoelastic.controller;

import com.example.demoelastic.model.FileInfo;
import com.example.demoelastic.service.SearchFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/fileInfo")
public class FileController {

    @Autowired
    SearchFileService searchFileService;

    @GetMapping("/all")
    public List<FileInfo> getAllFile() throws IOException {
        return searchFileService.getAllFileInfo();
    }


    @GetMapping("/get")
    public List<FileInfo> getByFileName(@RequestParam(name = "name")String name) throws IOException {
        searchFileService.findByNameCount(name);
        return searchFileService.findByName(name);
    }

    @GetMapping("/getAllByScroll")
    public List<FileInfo> getAllByScroll() throws IOException {
        return searchFileService.getAllByScroll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> findById(@PathVariable String id) throws IOException {
        return ResponseEntity.ok().body(searchFileService.getFileById(id));
    }

    @PostMapping("/add")
//    public FileInfo addNewFile(@RequestBody FileInfo fileInfo) throws IOException {
//        return searchFileService.addNewFile(fileInfo);
//    }
    public ResponseEntity<?> addNewFile(@RequestBody FileInfo fileInfo) throws IOException {

        return ResponseEntity.ok().body(searchFileService.addNewFile(fileInfo));
    }

    @GetMapping("/find")
    public List<FileInfo> getByNameAndTime(@RequestParam(name = "type") String type, @RequestParam(name = "time") long time) throws IOException {
        return searchFileService.findFileByTypeAndTime(type,time);

    }
    @GetMapping("/findByTime")
    public List<FileInfo> getByTime(@RequestParam(name = "createTime") long createTime, @RequestParam(name = "updateTime") long updateTime) throws IOException {
        return searchFileService.getByTime(createTime,updateTime);
    }

    @GetMapping("/delete/{id}")
    public ResponseEntity<?> deleteById(@PathVariable String id) throws IOException {
            searchFileService.deleteById(id);
        return ResponseEntity.ok().body(null);
    }

    @PostMapping("/addMulti")
    public ResponseEntity<?> addMulti(@RequestBody FileInfo[] fileInfos) throws IOException {
        return ResponseEntity.ok().body(searchFileService.addMultiFile(Arrays.asList(fileInfos)));
    }

    @GetMapping("/count")
    public ResponseEntity<?> getCount() throws IOException {
        return ResponseEntity.ok().body(searchFileService.getCountProfile());
    }

}
