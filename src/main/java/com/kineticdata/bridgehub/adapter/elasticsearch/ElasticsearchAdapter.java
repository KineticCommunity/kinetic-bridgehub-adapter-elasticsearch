package com.kineticdata.bridgehub.adapter.elasticsearch;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.DocumentContext;
import com.kinetcdata.bridgehub.helpers.http.HttpGetWithEntity;
import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.LoggerFactory;
import com.jayway.jsonpath.JsonPath;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name */
    public static final String NAME = "Elasticsearch Bridge";
    public static final String JSON_ROOT_DEFAULT = "$.hits.hits";
    public static final String REGEX_ROOT_PATTERN = "^\\{.*?\\}\\|(\\$\\..*)$";
    public static Pattern jsonRootPattern = Pattern.compile(REGEX_ROOT_PATTERN);
    
    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticsearchAdapter.class);

    /** Adapter version constant. */
    public static String VERSION = "1.0.0";

    private String username;
    private String password;
    private String elasticEndpoint;
    private String jsonRootPath = JSON_ROOT_DEFAULT;

    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String ELASTIC_URL = "Elastic URL";
    }
    
    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.USERNAME),
        new ConfigurableProperty(Properties.PASSWORD).setIsSensitive(true),
        new ConfigurableProperty(Properties.ELASTIC_URL)
    );

    
    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    
    @Override
    public void initialize() throws BridgeError {
        this.username = properties.getValue(Properties.USERNAME);
        this.password = properties.getValue(Properties.PASSWORD);
        this.elasticEndpoint = properties.getValue(Properties.ELASTIC_URL);
        testAuthenticationValues(this.elasticEndpoint, this.username, this.password);
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
        return VERSION;
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }
    
    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {

        String jsonResponse = elasticQuery("count", request);
        Long count = JsonPath.parse(jsonResponse).read("$.count", Long.class);

        // Create and return a Count object.
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        
        Matcher queryRootMatch = jsonRootPattern.matcher(request.getQuery());
        if (queryRootMatch.find()) {
            jsonRootPath = queryRootMatch.group(1);
        }

        String jsonResponse = elasticQuery("search", request);
        List<Record> recordList = new ArrayList<Record>();
        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        
        Record recordResult = new Record(null);
        
        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            if (listRoot.size() == 1) {
                Map<String, Object> recordValues = new HashMap();
                for (String field : request.getFields()) {
                    try {
                        recordValues.put(field, JsonPath.parse(listRoot.get(0)).read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                }
                recordResult = new Record(recordValues);
            } else {
                throw new BridgeError("Multiple results matched an expected single match query");
            }
        } else if (objectRoot instanceof Map) {
            Map<String, Object> recordValues = new HashMap();
            for (String field : request.getFields()) {
                try {
                    recordValues.put(field, JsonPath.parse(objectRoot).read(field));
                } catch (InvalidPathException e) {
                    recordValues.put(field, null);
                }
            }
            recordResult = new Record(recordValues);
        }
        
        return recordResult;
        
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        
        Matcher queryRootMatch = jsonRootPattern.matcher(request.getQuery());
        if (queryRootMatch.find()) {
            jsonRootPath = queryRootMatch.group(1);
        }
        
        String jsonResponse = elasticQuery("search", request);
        List<Record> recordList = new ArrayList<Record>();
        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Map<String,String> metadata = new LinkedHashMap<String,String>();
        metadata.put("count",JsonPath.parse(jsonResponse).read("$.hits.total", String.class));
        
        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            metadata.put("size", String.valueOf(listRoot.size()));
            for (Object arrayElement : listRoot) {
                Map<String, Object> recordValues = new HashMap();
                DocumentContext jsonObject = JsonPath.parse(arrayElement);
                for (String field : request.getFields()) {
                    try {
                        recordValues.put(field, jsonObject.read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                }
                recordList.add(new Record(recordValues));
            }
        } else if (objectRoot instanceof Map) {
            metadata.put("size", "1");
            Map<String, Object> recordValues = new HashMap();
            DocumentContext jsonObject = JsonPath.parse(objectRoot);
            for (String field : request.getFields()) {
                recordValues.put(field, jsonObject.read(field));
            }
            recordList.add(new Record(recordValues));
        }
        
        return new RecordList(request.getFields(), recordList, metadata);
        
    }
    
    
    /*----------------------------------------------------------------------------------------------
     * PUBLIC HELPER METHODS
     *--------------------------------------------------------------------------------------------*/
    
    public String buildUrl(String queryMethod, BridgeRequest request) throws BridgeError {
        Map<String,String> metadata = BridgeUtils.normalizePaginationMetadata(request.getMetadata());
        String pageSize = "1000";
        String offset = "0";
        
        if (StringUtils.isNotBlank(metadata.get("pageSize")) && metadata.get("pageSize").equals("0") == false) {
            pageSize = metadata.get("pageSize");
        }
        if (StringUtils.isNotBlank(metadata.get("offset"))) {
            offset = metadata.get("offset");
        }
        
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        String query = null;
        query = parser.parse(request.getQuery(),request.getParameters());

        // Build up the url that you will use to retrieve the source data. Use the query variable
        // instead of request.getQuery() to get a query without parameter placeholders.
        StringBuilder url = new StringBuilder();
        url.append(this.elasticEndpoint)
            .append("/")
            .append(request.getStructure())
            .append("/_")
            .append(queryMethod);
        
        boolean firstParameter = true;
        
        // if the query is a request body JSON query...
        if (request.getQuery().trim().startsWith("{") == false) {
            firstParameter = addParameter(url, "q", query, firstParameter);
        }
                
        //only set pagination if we're not counting.
        if (queryMethod.equals("count") == false) {
            firstParameter = addParameter(url, "size", pageSize, firstParameter);
            firstParameter = addParameter(url, "from", offset, firstParameter);
            // only set field limitation if we're not counting 
            //   *and* the request specified fields to be returned
            //   *and* the JSON root path has not changed.
            if (request.getFields() != null && 
                request.getFields().isEmpty() == false &&
                jsonRootPath.equals(JSON_ROOT_DEFAULT)
            ) {
                StringBuilder includedFields = new StringBuilder();
                String[] bridgeFields = request.getFieldArray();
                for (int i = 0; i < request.getFieldArray().length; i++) {
                    //strip _source from the beginning of the specified field name as this is redundent to Elasticsearch.
                    includedFields.append(bridgeFields[i].replaceFirst("^_source\\.(.*)", "$1"));
                    //only append a comma if this is not the last field
                    if (i != (request.getFieldArray().length -1)) {
                        includedFields.append(",");
                    }
                }
                firstParameter = addParameter(url, "_source", includedFields.toString(), firstParameter);
            }
            //only set sorting if we're not counting *and* the request specified a sort order.
            if (request.getMetadata("order") != null) {
                List<String> orderList = new ArrayList<String>();
                //loop over every defined sort order and add them to the Elasicsearch URL
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(request.getMetadata("order")).entrySet()) {
                    String key = entry.getKey().replaceFirst("^_source\\.(.*)", "$1");
                    if (entry.getValue().equals("DESC")) {
                        orderList.add(String.format("%s:desc", key));
                    }
                    else {
                        orderList.add(String.format("%s:asc", key));
                    }
                }
                String order = StringUtils.join(orderList,",");
                addParameter(url, "sort", order, firstParameter);
            }
            
        }

        return url.toString();
        
    }

    /*----------------------------------------------------------------------------------------------
     * PRIVATE HELPER METHODS
     *--------------------------------------------------------------------------------------------*/
    private void addBasicAuthenticationHeader(HttpGetWithEntity get, String username, String password) {
        String creds = username + ":" + password;
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));
    }
    private boolean addParameter(StringBuilder url, String parameterName, String parameterValue, boolean firstParameter) {
        if (firstParameter == true) {
            url.append("?");
            firstParameter = false;
        } else {
            url.append("&");
        }
        url.append(URLEncoder.encode(parameterName))
            .append("=")
            .append(URLEncoder.encode(parameterValue));
        return firstParameter;
    }
    
    private String elasticQuery(String queryMethod, BridgeRequest request) throws BridgeError{
        
        String result = null;
        String url = buildUrl(queryMethod, request);
        
        // Initialize the HTTP Client, Response, and Get objects.
        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        HttpGetWithEntity get = new HttpGetWithEntity();
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new BridgeError(e);
        }
        get.setURI(uri);
        
        // If the bridge qualfication starts with a curly brace, assume JSON and Request Body searching.
        if (request.getQuery().trim().startsWith("{")) {
            
            HttpEntity entity;
            try {
                ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
                String parsedQuery = parser.parse(request.getQuery(), request.getParameters());
                
                // Set the parsed query as the request body payload.
                entity = new ByteArrayEntity(parsedQuery.getBytes("UTF-8"));
                get.setEntity(entity);
            } catch (UnsupportedEncodingException e) {
                throw new BridgeError(e);
            }
        }

        // Append the authentication to the call. This example uses Basic Authentication but other
        // types can be added as HTTP GET or POST headers as well.
        if (this.username != null && this.password != null) {
            addBasicAuthenticationHeader(get, this.username, this.password);
        }

        // Make the call to the REST source to retrieve data and convert the response from an
        // HttpEntity object into a Java string so more response parsing can be done.
        try {
            response = client.execute(get);
            Integer responseStatus = response.getStatusLine().getStatusCode();
            
            if (responseStatus >= 300 || responseStatus < 200) {
                throw new BridgeError("The Elasticsearch server returned a HTTP status code of " + responseStatus + ", 200 was expected.");
            }
            
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
            logger.trace("Request response code: "+response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Elasticsearch server");
        }
        logger.trace("Elasticsearch response - Raw Output: "+ result);
        
        return result;
    }
    
    private void testAuthenticationValues(String restEndpoint, String username, String password) throws BridgeError {
        logger.debug("Testing the authentication credentials");
        HttpGetWithEntity get = new HttpGetWithEntity();
        URI uri;
        try {
            uri = new URI(String.format("%s/_cat/health",restEndpoint));
        } catch (URISyntaxException e) {
            throw new BridgeError(e);
        }
        get.setURI(uri);
        
        if (username != null && password != null) {
            addBasicAuthenticationHeader(get, this.username, this.password);
        }

        DefaultHttpClient client = new DefaultHttpClient();
        HttpResponse response;
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            Integer responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == 401) {
                throw new BridgeError("Unauthorized: The inputted Username/Password combination is not valid.");
            }
            if (responseCode < 200 || responseCode >= 300) {
                throw new BridgeError("Unsuccessful HTTP response - the server returned a " + responseCode + " status code, expected 200.");
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Elasticsearch health check API."); 
        }
    }

}