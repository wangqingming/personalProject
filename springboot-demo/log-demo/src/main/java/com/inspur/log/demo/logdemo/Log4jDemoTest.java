package com.inspur.log.demo.logdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Log4jDemoTest {
    private static final Logger logger = LoggerFactory.getLogger(Log4jDemoTest.class);

    public static void main(String[] args) {
        List<String> list = makeList();

        long start = System.currentTimeMillis();

        //查看性能影响
        //logger.debug("debug {}"+ list);
        logger.debug("debug {}", list);
        System.out.println("耗时" + (System.currentTimeMillis() - start));


        logger.warn("err " + (System.currentTimeMillis() - start));
        logger.error("error");
    }

    private static List<String> makeList() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            list.add(i + "");
        }
        return list;
    }
}
