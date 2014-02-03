/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.minified.esdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 *
 * @author agung
 */
public class EsDao {

    private final Client client;
    public final String ALL_INDEX = "libraries_index";
    public final String BOOK_INDEX = "books:index";
    public final String AUTHOR_INDEX = "authors:index";
    public final String BOOK_TYPE = DocumentType.book.toString();
    public final String AUTHOR_TYPE = DocumentType.author.toString();
    private final ObjectMapper m = new ObjectMapper();
    
        
    public EsDao(Client client) {
        this.client = client;
    }
    
    public XContentBuilder getBookIndexMapping() throws IOException{
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
           .startObject(BOOK_TYPE)
            .startObject("_parent")
                .field("type", AUTHOR_TYPE)
            .endObject()
            .startObject("properties")
                .startObject("name")
                    .field("type", "string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("description")
                    .field("type","string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("authorId")
                    .field("type","long")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("authorKey")
                    .field("type","string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("id")
                    .field("type","long")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
           .endObject()
        .endObject();
        return builder;
    }

    public XContentBuilder getAuthorIndexMapping() throws IOException{
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
           .startObject(AUTHOR_TYPE)
            .startObject("properties")
                .startObject("name")
                    .field("type", "string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("publisher")
                    .field("type","string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("id")
                    .field("type","long")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
           .endObject()
        .endObject();
        return builder;
    }
    
    public XContentBuilder getAllIndexMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
           .startObject(AUTHOR_TYPE)
            .startObject("properties")
                .startObject("name")
                    .field("type", "string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("publisher")
                    .field("type","string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("id")
                    .field("type","long")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
           .endObject()
           .startObject(BOOK_TYPE)
            .startObject("_parent")
                .field("type", AUTHOR_TYPE)
            .endObject()
            .startObject("properties")
                .startObject("name")
                    .field("type", "string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("description")
                    .field("type","string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("authorId")
                    .field("type","long")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("authorKey")
                    .field("type","string")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("id")
                    .field("type","long")
                    .field("store", "yes")
                    .field("index", "not_analyzed")
                .endObject()
           .endObject()
        .endObject();
        return builder;
    }
    
    
    public void indexParent(Author author){
//        if(!indexExist(AUTHOR_INDEX)){
//            createIndex(AUTHOR_INDEX);
//        }
        if(!indexExist(ALL_INDEX)){
            createIndex();
        }
        Map<String, Object> data = m.convertValue(author, Map.class);
        final String idxName = ALL_INDEX;
        IndexResponse r = client
                .prepareIndex(idxName, AUTHOR_TYPE, author.getKey())
                .setSource(data)
                .execute()
                .actionGet();
    }
    
    public void indexChild(Book book, Author author){
//        if(!indexExist(BOOK_INDEX)){
//            createIndex(BOOK_INDEX);
//        }
        if(!indexExist(ALL_INDEX)){
            createIndex();
        }
        Map<String,Object> data = m.convertValue(book , Map.class);
        final String idxName = ALL_INDEX;
        IndexRequestBuilder irb = client
                .prepareIndex(idxName, BOOK_TYPE, book.getKey())
                .setParent(author.getKey())
                .setSource(data);
        IndexResponse r = irb.execute().actionGet();
    }

    public void createIndex(){
        client.admin()
                .indices()
                .prepareCreate(ALL_INDEX)
                .execute()
                .actionGet();
        try {
            PutMappingRequestBuilder pm = client.admin()
                    .indices()
                    .preparePutMapping(ALL_INDEX)
                    .setType(AUTHOR_TYPE)
                    .setSource(getAuthorIndexMapping());
            PutMappingResponse r = pm.execute().actionGet();
            System.out.println(">>>"+r.getHeaders());
            
            PutMappingRequestBuilder pm0 = client.admin()
                    .indices()
                    .preparePutMapping(ALL_INDEX)
                    .setType(BOOK_TYPE)
                    .setSource(getBookIndexMapping());
            PutMappingResponse r0 = pm0.execute().actionGet();
            System.out.println(">>>"+r0.getHeaders());
        } catch (IOException ex) {
            Logger.getLogger(EsDao.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void createIndex(String indexName) {
        if(Objects.equal(indexName, BOOK_INDEX)){
            client.admin()
            .indices()
            .prepareCreate(indexName).execute().actionGet();
            
            try {
                PutMappingRequestBuilder pm = client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setType(BOOK_TYPE)
                .setSource(getBookIndexMapping());
                
                PutMappingResponse r = pm.execute()
                .actionGet();
                System.out.println("RESP = "+r.toString());
                
            } catch (IOException ex) {
                Logger.getLogger(EsDao.class.getName())
                        .log(Level.SEVERE, null, ex);
            }
                    
        }else{
            client.admin()
            .indices()
            .prepareCreate(indexName).execute().actionGet();
            
            try{
                 PutMappingRequestBuilder pm = client.admin()
                .indices()
                .preparePutMapping(indexName)
                .setType(AUTHOR_TYPE)
                .setSource(getAuthorIndexMapping());
            }catch(IOException ex){
                Logger.getLogger(EsDao.class.getName())
                        .log(Level.SEVERE, null, ex);
            }
        }
    }

    public Boolean indexExist(String indexName) {
        IndicesExistsRequestBuilder req = client.admin().indices().prepareExists(indexName);
        try {
            IndicesExistsResponse resp = req.execute().get();
            return resp.isExists();
        } catch (InterruptedException ex) {
            Logger.getLogger(EsDao.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(EsDao.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    public List<Map<String, Object>> searchBook(String param){
        QueryBuilder parentQuery = QueryBuilders.queryString(param);
        HasParentQueryBuilder hsp = QueryBuilders.hasParentQuery(AUTHOR_TYPE, parentQuery);
        
        QueryBuilder qb = QueryBuilders.boolQuery()
                .should(hsp)
                .should(QueryBuilders.queryString(param));
        
        SearchResponse resp = client
                .prepareSearch(ALL_INDEX)
                .setTypes(BOOK_TYPE, AUTHOR_TYPE)
                .setSearchType(SearchType.QUERY_THEN_FETCH) 
                .setQuery(qb)
                .execute()
                .actionGet();
        
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : resp.getHits()) {
            if(!Objects.equal(hit.getSource(), null)){
                results.add(hit.getSource());
            }
        }
        return results;
    }
    
    public void removeIndices(){
        client.admin().indices().delete(new DeleteIndexRequest(ALL_INDEX,BOOK_INDEX, AUTHOR_INDEX));
    }
}
