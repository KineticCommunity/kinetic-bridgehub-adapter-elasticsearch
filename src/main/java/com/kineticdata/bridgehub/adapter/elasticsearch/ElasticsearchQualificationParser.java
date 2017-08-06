package com.kineticdata.bridgehub.adapter.elasticsearch;

import com.kineticdata.bridgehub.adapter.QualificationParser;

public class ElasticsearchQualificationParser extends QualificationParser {
    
    private static String[] reservedCharacters = {
        "+","-","=","&&","||",">","<","!","(",")","{","}","[","]","^","\\","~","*","?",":","\\\\","/"
    };
    
    @Override
    public String encodeParameter(String name, String value) {
        return value;
    }
}
