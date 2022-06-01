package com.example.demoelastic.service;

import com.example.demoelastic.model.FileInfo;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SearchFileService {
    @Autowired
    RestHighLevelClient client;


    Logger logger = LoggerFactory.getLogger(this.getClass());

    public List<FileInfo> getAllFileInfo() throws IOException {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchAllQuery());
//        Source filtering
//        khong lay truong _source trong ket qua tra ve
//        sourceBuilder.fetchSource(false);
//        chi lay nhung Fields trong includeFields

//        String[] includeFields = new String[] {"*time","is*","source_id"};
//        String[] excludeFields = new String[] {"file*"};
//        sourceBuilder.fetchSource(includeFields, excludeFields);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);

        logger.info("get All File: "+response);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();

        for(SearchHit hit : searchHits) {
            fileInfos.add(convertToFileInfo(hit));
        }
        return fileInfos;
    }

    //get all by scroll API
    public List<FileInfo> getAllByScroll() throws IOException {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.size(3);
        request.source(sourceBuilder);
        request.scroll(TimeValue.timeValueSeconds(15));
        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();
        logger.info("get SCROLL ID: "+scrollId);
        SearchHit[] searchHits = response.getHits().getHits();
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();
        //Xu ly ket qua tra ve
        for(SearchHit hit : searchHits) {
            fileInfos.add(convertToFileInfo(hit));
        }
        //
        int size = 0;

        while (searchHits != null && searchHits.length >0)
         {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(10));
            SearchResponse searchScrollResponse = client.scroll(scrollRequest,RequestOptions.DEFAULT);
            scrollId = searchScrollResponse.getScrollId();
            searchHits = searchScrollResponse.getHits().getHits();
            size = searchHits.length;
            for(SearchHit hit : searchHits) {
                fileInfos.add(convertToFileInfo(hit));
            }
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse scrollResponse = client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);

        return fileInfos;
    }

    public FileInfo addNewFile(FileInfo fileInfo) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("file_url",fileInfo.getFile_url());
            builder.field("file_name",fileInfo.getFile_name());

            logger.info("get Create Time: {}", fileInfo.getCreate_time());
            if(fileInfo.getCreate_time() != 0) {
                builder.field("create_time",fileInfo.getCreate_time());
            }
            builder.field("update_time",fileInfo.getUpdate_time());
            builder.field("is_deleted",fileInfo.getIs_deleted());
            builder.field("is_processed",fileInfo.getIs_processed());
            builder.field("source_id", fileInfo.getSource_id());
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("posts").
                id(fileInfo.getId()).source(builder);
        IndexResponse indexResponse = client.index(indexRequest,RequestOptions.DEFAULT);
        logger.info("add index: {}",indexResponse);
        logger.info("add new File: {}",indexResponse.getResult());
        return fileInfo;
    }

    public List<String> addMultiFile(List<FileInfo> fileInfos) throws IOException {
        BulkRequest request = new BulkRequest();
        List<String> listId = new ArrayList<>();
        for(FileInfo fileInfo : fileInfos) {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("file_url",fileInfo.getFile_url());
                builder.field("file_name",fileInfo.getFile_name());
                builder.field("create_time",fileInfo.getCreate_time());
                builder.field("update_time",fileInfo.getUpdate_time());
                builder.field("is_deleted",fileInfo.getIs_deleted());
                builder.field("is_processed",fileInfo.getIs_processed());
                builder.field("source_id", fileInfo.getSource_id());
            }
            builder.endObject();
            request.add(new IndexRequest("posts").id(fileInfo.getId()).source(builder));

        }
        BulkResponse bulkResponse = client.bulk(request,RequestOptions.DEFAULT);
        for(BulkItemResponse itemResponse: bulkResponse) {
            listId.add(itemResponse.getResponse().getId());
            logger.info("Item response: {}",itemResponse.getResponse());
        }

        logger.info("Add multi file: {}",bulkResponse);
        return listId;
    }

    public List<FileInfo> findByName(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest("posts");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("file_name",name);
        queryBuilder.fuzziness(Fuzziness.AUTO);
        queryBuilder.prefixLength(3);
        queryBuilder.maxExpansions(10);
        searchSourceBuilder.query(queryBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();
        for (SearchHit hit : searchHits) {
            fileInfos.add(convertToFileInfo(hit));
        }
        return fileInfos;
    }

    public void findByNameCount(String name) throws IOException {
        CountRequest countRequest = new CountRequest();
        SearchSourceBuilder sourceBuilder= new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("file_name",name));
        countRequest.query(QueryBuilders.matchQuery("file_name",name));

        CountResponse response = client.count(countRequest,RequestOptions.DEFAULT);
        logger.info("get count response: {}",response);
    }

    public List<FileInfo> findFileByTypeAndTime(String type, long create_time) throws IOException {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest request1 = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.query(QueryBuilders.matchQuery("file_name",type));
        request1.source(searchSourceBuilder1);
        multiSearchRequest.add(request1);

        SearchRequest request2 = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2.query(QueryBuilders.matchQuery("create-time",create_time));
        request2.source(searchSourceBuilder2);
        multiSearchRequest.add(request2);
        MultiSearchResponse response = client.msearch(multiSearchRequest,RequestOptions.DEFAULT);
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();
        for(int i = 0; i<response.getResponses().length;i++) {
            MultiSearchResponse.Item firstResponse = response.getResponses()[i];
            SearchHit[] searchHits = firstResponse.getResponse().getHits().getHits();
            for(SearchHit hit: searchHits) {
                fileInfos.add(convertToFileInfo(hit));
            }
        }

        logger.info("multi search: {}",response);
        return fileInfos;
    }

    public void explainApi() throws IOException {
        ExplainRequest request = new ExplainRequest("posts","7");
        request.query(QueryBuilders.matchQuery("file_name","test"));
        ExplainResponse response = client.explain(request,RequestOptions.DEFAULT);
        String index = response.getIndex();
        String id = response.getId();
        boolean exists = response.isExists();
        boolean match = response.isMatch();
        boolean hasExplanation = response.hasExplanation();
        Explanation explanation = response.getExplanation();
        GetResult getResult = response.getGetResult();
        logger.info("Explain request: {}",response.getExplanation() );
        logger.info("Explain request: {}",response.hasExplanation() );
        logger.info("Explain response: {}", response.getGetResult());
    }

    public FileInfo getFileById(String id) throws IOException {
        GetRequest getRequest = new GetRequest("posts",id);
        GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
        Map<String,Object> map = getResponse.getSourceAsMap();
        String fileId = getResponse.getId();
        String file_name = (String) map.get("file_name");
        int is_processed = (Integer) map.get("is_processed");
        int is_deleted = (Integer) map.get("is_deleted");
        String file_url = (String) map.get("file_url");
        long create_time = ((Number) map.get("create_time")).longValue();
        long update_time = ((Number) map.get("update_time")).longValue();
        long source_id = ((Number) map.get("source_id")).longValue();
        FileInfo fileInfo = FileInfo.builder()
                .file_name(file_name)
                .file_url(file_url)
                .id(fileId)
                .create_time(create_time)
                .update_time(update_time)
                .is_deleted(is_deleted)
                .is_processed(is_processed)
                .source_id(source_id)
                .build();
        return fileInfo;
    }

    public List<FileInfo> getByTime(long createTime, long updateTime) throws IOException {
        QueryBuilder first = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("create_time").gte(createTime))
                .must(QueryBuilders.rangeQuery("update_time").gte(updateTime));
        QueryBuilder second = QueryBuilders.termQuery("is_deleted","0");
        QueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .should(first)
                .should(second)
                .minimumShouldMatch(2);
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.sort(new FieldSortBuilder("source_id").order(SortOrder.ASC));
        sourceBuilder.query(boolQueryBuilder);
        request.source(sourceBuilder);

        SearchResponse response = client.search(request,RequestOptions.DEFAULT);
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();
        SearchHit[] searchHits = response.getHits().getHits();

        for(SearchHit hit : searchHits) {
            fileInfos.add(convertToFileInfo(hit));
        }
        return fileInfos;
    }

    public void deleteById(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("posts").id(id);
        DeleteResponse response = client.delete(deleteRequest,RequestOptions.DEFAULT);
        logger.info("Delete file has file ID: {}",response);
    }

    private FileInfo convertToFileInfo(SearchHit hit) {

        Map<String,Object> map = hit.getSourceAsMap();
        String file_name = (String) map.get("file_name");
        String id = hit.getId();
//        String id = Integer.toString((Integer) map.get("id")) ;
//        int is_processed = (Integer) map.get("is_processed");
//        int is_deleted = (Integer) map.get("is_deleted");
        int is_processed=-1;
        if(map.get("is_processed") != null)
             is_processed = (Integer) map.get("is_processed");
        int is_deleted=-1;
        if(map.get("is_deleted") != null)
            is_deleted = (Integer) map.get("is_deleted");

        String file_url = (String) map.get("file_url");
        long create_time = ((Number) map.get("create_time")).longValue();
        long update_time = ((Number) map.get("update_time")).longValue();
        long source_id = ((Number) map.get("source_id")).longValue();
        FileInfo fileInfo = FileInfo.builder()
                .file_name(file_name)
                .file_url(file_url)
                .id(id)
                .create_time(create_time)
                .update_time(update_time)
                .is_deleted(is_deleted)
                .is_processed(is_processed)
                .source_id(source_id)
                .build();
        return fileInfo;
    }

    public  Map<Long,Integer> getCountProfile() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().should(QueryBuilders.existsQuery("data.EMAIL"))
                .should(QueryBuilders.existsQuery("data.MOBILE")).minimumShouldMatch(1);

        searchSourceBuilder.query(queryBuilder);
        String[] includeFields = new String[] {"source_id"};
        String[] excludeFields = new String[]{};
//        searchSourceBuilder.fetchSource(includeFields,excludeFields);
        SearchRequest searchRequest = new SearchRequest("cdp-profiles-*");
        searchRequest.source(searchSourceBuilder);
        Map<Long, Integer> mapResult = new HashMap<Long, Integer>();

        searchRequest.scroll(TimeValue.timeValueSeconds(15));
        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        //Xu ly ket qua tra ve
        for(SearchHit hit : searchHits) {
            Long source =(Long) hit.getSourceAsMap().get("source_id");
            if(mapResult.get(source) == null) mapResult.put(source,1);
            else mapResult.put(source,mapResult.get(source)+1);
        }

        while (searchHits != null && searchHits.length >0)
        {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(10));
            SearchResponse searchScrollResponse = client.scroll(scrollRequest,RequestOptions.DEFAULT);
            scrollId = searchScrollResponse.getScrollId();
            searchHits = searchScrollResponse.getHits().getHits();
            for(SearchHit hit : searchHits) {
                Long source =(Long) hit.getSourceAsMap().get("source_id");
                if(mapResult.get(source) == null) mapResult.put(source,1);
                else mapResult.put(source,mapResult.get(source)+1);
            }
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse scrollResponse = client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
        logger.info("result: {}",mapResult);
        return mapResult;

    }
}
