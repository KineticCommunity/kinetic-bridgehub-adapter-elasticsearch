package com.kineticdata.bridgehub.adapter.elasticsearch;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.QualificationParser;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONValue;

public class ElasticsearchQualificationParser extends QualificationParser {
    
    //public static String PARAMETER_PATTERN = "<%=\\s*parameter\\[\\\"?(.*?)\\\"?\\]\\s*%>";
    
    @Override
    public String encodeParameter(String name, String value) {
        String result = null;
        //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
        //Next three lines: escape the following characters with a backslash: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /  
        String regexReservedCharactersPattern = "(\\*|\\+|\\-|\\=|\\~|\\>|\\<|\\\"|\\?|\\^|\\$|\\{|\\}|\\(|\\)|\\:|\\!|\\/|\\[|\\]|\\\\|\\s)";
        if (StringUtils.isNotEmpty(value)) {
            result = value.replaceAll(regexReservedCharactersPattern, "\\\\$1")
                .replaceAll("\\|\\|", "\\\\||")
                .replaceAll("\\&\\&", "\\\\&&")
                .replaceAll("AND", "\\\\A\\\\N\\\\D")
                .replaceAll("OR", "\\\\O\\\\R")
                .replaceAll("NOT", "\\\\N\\\\O\\\\T");
        }
        return result;
    }
    
    @Override
    public String parse(String query, Map<String, String> parameters) throws BridgeError {

        StringBuffer resultBuffer = new StringBuffer();
        Pattern pattern = Pattern.compile(PARAMETER_PATTERN);
        Matcher matcher = pattern.matcher(query);

        while (matcher.find()) {
            // Retrieve the necessary values
            String parameterName = matcher.group(1);
            // If there were no parameters provided
            if (parameters == null) {
                throw new BridgeError("Unable to parse qualification, "+
                    "the '"+parameterName+"' parameter was referenced but no "+
                    "parameters were provided.");
            }
            String parameterValue = parameters.get(parameterName);
            // If there is a reference to a parameter that was not passed
            if (parameterValue == null) {
                throw new BridgeError("Unable to parse qualification, "+
                    "the '"+parameterName+"' parameter was referenced but "+
                    "not provided.");
            }

            String value;
            // If the query string starts with a curly brace, this is a JSON payload.
            // else it is supposed to be a query used for the q parameter in a URI Search
            if (query.trim().startsWith("{")) {
                // if JSON, escape any JSON special characters.
                // JSON query syntax documentation:
                //   - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html
                //   - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
                value = JSONValue.escape(parameterValue);
            } else {
                // if not JSON, encode the parameter by escaping any Lucene query syntax reserved characters.
                value = encodeParameter(parameterName, parameterValue);
            }
            matcher.appendReplacement(resultBuffer, Matcher.quoteReplacement(value));
        }

        matcher.appendTail(resultBuffer);
        return resultBuffer.toString();
    }
}
