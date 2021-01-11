package com.inspur.log.demo.logdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LogTest4 {
    private static final Logger logger = LoggerFactory.getLogger(LogTest4.class);

    public static void main(String[] args) {
        List<String> list = makeList();

        long start = System.currentTimeMillis();

        //查看性能影响
        logger.debug("debug {}"+ list);//此时会先将list拼接字符串，然后做为参数，调用debug
       // logger.debug("debug {}", list);
        logger.error("耗时" + (System.currentTimeMillis() - start));
        logger.warn("err " + (System.currentTimeMillis() - start));
        logger.error("error");
    }

    private static List<String> makeList() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 10000000; i++) {
            list.add(i + "");
        }
        return list;
    }
}
