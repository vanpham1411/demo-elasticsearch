package com.example.demoelastic.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FileInfo {
    private String id;
    private String file_url;
    private String file_name;
    private long source_id;
    private int is_processed;
    private int is_deleted;
    private long create_time;
    private long update_time;
}
