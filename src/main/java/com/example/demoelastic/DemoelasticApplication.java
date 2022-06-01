package com.example.demoelastic;

import com.example.demoelastic.service.SearchFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.IOException;

@SpringBootApplication
public class DemoelasticApplication {
    @Autowired
    SearchFileService searchFileService;

    public static void main(String[] args) {
        SpringApplication.run(DemoelasticApplication.class, args);
    }
    @PostConstruct
    public void check() throws IOException {
//        searchFileService.findByName("abc");
//        searchFileService.explainApi();
    }

}
