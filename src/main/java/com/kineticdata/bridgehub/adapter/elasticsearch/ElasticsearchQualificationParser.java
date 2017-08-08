package com.kineticdata.bridgehub.adapter.elasticsearch;

import com.kineticdata.bridgehub.adapter.QualificationParser;
import org.apache.commons.lang.StringUtils;

public class ElasticsearchQualificationParser extends QualificationParser {
    @Override
    public String encodeParameter(String name, String value) {
        String result = null;
        //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
        //escape the following characters with a backslash: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
        String regexReservedCharactersPattern = "(\\*|\\+|\\-|\\=|\\~|\\>|\\<|\\\"|\\?|\\^|\\$|\\{|\\}|\\(|\\)|\\:|\\!|\\/|\\[|\\]|\\\\|\\s)";
        if (StringUtils.isNotEmpty(value)) {
            result = value.replaceAll(regexReservedCharactersPattern, "\\\\$1")
                .replaceAll("\\|\\|", "\\||")
                .replaceAll("\\&\\&", "\\&&")
                .replaceAll("AND", "\\A\\N\\D")
                .replaceAll("OR", "\\O\\R")
                .replaceAll("NOT", "\\N\\O\\T");
        };
        return result;
    }
}
