package com.kineticdata.bridgehub.adapter.elasticsearch;

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
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.LoggerFactory;

public class ElasticsearchAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name */
    public static final String NAME = "Elasticsearch Bridge";
    
    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticsearchAdapter.class);

    /** Adapter version constant. */
    public static String VERSION = "1.0.0";

    private String username;
    private String password;
    private String elasticEndpoint;

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
        
        // Parse the Response String into a JSON Object
        JSONObject json = (JSONObject)JSONValue.parse(jsonResponse);
        // Get the count value from the response object
        Object countObj = json.get("count");
        // Assuming that the countObj is a string, parse it to an integer
        Long count = (Long)countObj;

        // Create and return a Count object.
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        
        String jsonResponse = elasticQuery("search", request);
        
        JSONObject json = (JSONObject)JSONValue.parse(jsonResponse);
        JSONObject hits = (JSONObject)json.get("hits");
        Long recordCount = (Long)hits.get("total");
        
        Record recordResult = new Record(null);
        
        if (recordCount == 1) {
            JSONArray hitsArray = (JSONArray)hits.get("hits");
            Map<String, Object> fieldValues = new HashMap<String, Object>();
            mapToFields((JSONObject)hitsArray.get(0), new StringBuilder(), fieldValues);
            recordResult = new Record(fieldValues);
        } else if (recordCount > 1) {
            throw new BridgeError("Multiple results matched an expected single match query");
        }
        
        // Create and return a Record object.
        return recordResult;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        
        String jsonResponse = elasticQuery("search", request);
        JSONObject json = (JSONObject)JSONValue.parse(jsonResponse);
        JSONObject hits = (JSONObject)json.get("hits");
        JSONArray hitsArray = (JSONArray)hits.get("hits");
        
        List<Record> recordList = new ArrayList<Record>();
        List<String> fieldList = new ArrayList<String>();
        Set<String> uniqueFields = new LinkedHashSet<String>();
        
        for (Object o : hitsArray) {
            // Convert the standard java object to a JSONObject
            JSONObject jsonObject = (JSONObject)o;
            // Create a record based on that JSONObject and add it to the list of records
            Map<String, Object> fieldValues = new HashMap<String, Object>();
            mapToFields(jsonObject, new StringBuilder(), fieldValues);
            recordList.add(new Record(fieldValues));
            uniqueFields.addAll(fieldValues.keySet());
        }
        
        // Create the metadata that needs to be returned.
        Map<String,String> metadata = new LinkedHashMap<String,String>();        
        metadata.put("count",String.valueOf(recordList.size()));
        metadata.put("size", String.valueOf(recordList.size()));

        if (request.getFields() != null && request.getFields().isEmpty() == false) {
            fieldList = request.getFields();
        } else if (recordList.isEmpty() == false) {
            fieldList = new ArrayList<String>(uniqueFields);
        }
        
        return new RecordList(fieldList, recordList, metadata);
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
        try {
            query = URLEncoder.encode(parser.parse(request.getQuery(),request.getParameters()), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
            throw new BridgeError("There was a problem URL encoding the bridge qualification");
        }        

        // Build up the url that you will use to retrieve the source data. Use the query variable
        // instead of request.getQuery() to get a query without parameter placeholders.
        StringBuilder url = new StringBuilder();
        url.append(this.elasticEndpoint)
            .append("/")
            .append(request.getStructure())
            .append("/_")
            .append(queryMethod)
            .append("?q=")
            .append(query);
                
        //only set pagination if we're not counting.
        if (queryMethod.equals("count") == false) {
            url.append("&size=")
                .append(pageSize)
                .append("&from=")
                .append(offset);
            //only set field limitation if we're not counting *and* the request specified fields to be returned.
            if (request.getFields() != null && request.getFields().isEmpty() == false) {
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
                url.append("&_source=")
                    .append(URLEncoder.encode(includedFields.toString()));
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
                url.append("&sort=")
                    .append(URLEncoder.encode(order));
            }
            
        }

        return url.toString();
        
    }
    
    public void mapToFields (JSONObject currentObject, StringBuilder currentFieldPrefix, Map<String, Object> bridgeFields) throws BridgeError {
        
        for (Object jsonKey : currentObject.keySet()) {
            StringBuilder preservedFieldPrefix = new StringBuilder(currentFieldPrefix);
            String strKey = (String)jsonKey;
            Object jsonValue = currentObject.get(jsonKey);
            if (jsonValue instanceof JSONObject) {
                if (currentFieldPrefix.length() > 0) {
                    currentFieldPrefix.append(".");
                }
                currentFieldPrefix.append(strKey);
                mapToFields((JSONObject)jsonValue, currentFieldPrefix, bridgeFields);
            }
            if (jsonValue instanceof String || jsonValue instanceof Number) {
                StringBuilder bridgeKey = new StringBuilder();
                if (currentFieldPrefix.length() > 0) {
                    bridgeKey.append(currentFieldPrefix).append(".");
                }
                bridgeKey.append(strKey);
                bridgeFields.put(
                    bridgeKey.toString(),
                    jsonValue.toString()
                );
            }
            currentFieldPrefix = preservedFieldPrefix;
        }
        
    }
    

    /*----------------------------------------------------------------------------------------------
     * PRIVATE HELPER METHODS
     *--------------------------------------------------------------------------------------------*/
    private HttpGet addBasicAuthenticationHeader(HttpGet get, String username, String password) {
        String creds = username + ":" + password;
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));
        return get;
    }
    
    private String elasticQuery(String queryMethod, BridgeRequest request) throws BridgeError{
        
        String result = null;
        String url = buildUrl(queryMethod, request);
        
        // Initialize the HTTP Client, Response, and Get objects.
        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        HttpGet get = new HttpGet(url.toString());

        // Append the authentication to the call. This example uses Basic Authentication but other
        // types can be added as HTTP GET or POST headers as well.
        if (this.username != null && this.password != null) {
            get = addBasicAuthenticationHeader(get, this.username, this.password);
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
        HttpGet get = new HttpGet(String.format("%s/_cat/health",restEndpoint));
        
        if (username != null && password != null) {
            get = addBasicAuthenticationHeader(get, this.username, this.password);
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