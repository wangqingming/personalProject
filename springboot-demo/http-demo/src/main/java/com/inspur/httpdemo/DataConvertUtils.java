package com.inspur.httpdemo;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataConvertUtils {

    private static final GenericDataType getTargetDataType(String sourceDbType,String targetDbType,GenericDataType dbType ){

       // StringUtils.

        try {

            String content = new String(Files.readAllBytes(Paths.get("duke.java")));
        } catch (IOException e) {
            e.printStackTrace();
        }


        return dbType;
    }
}
