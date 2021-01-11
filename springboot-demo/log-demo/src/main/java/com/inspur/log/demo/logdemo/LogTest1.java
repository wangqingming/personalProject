package com.inspur.log.demo.logdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 不要使用 System.out.println();
 * 1,该方法不好控制级别，到时候无法获取我们需要的信息，一个类中要么全打，要么全不打;
 * 2,System.out.println这样的是线程同步的，性能不好。此处的性能不好，只是理论上的，只有大量的并且高并发的 System.out.println才会显示现性能差异
 * 3,应用中不可直接使用日志系统（Log4j、Logback）中的 API，而应依赖使用日志框架 SLF4J 中的 API，使用门面模式的日志框架，有利于维护和各个类的日志处理方式统一。
 */
public class LogTest1 {
    private static final Logger logger = LoggerFactory.getLogger(LogTest1.class);
    //private static final ch.qos.logback.classic.Logger loggers = new ch.qos.logback.classic.Logger();//用中不可直接使用日志系统（Log4j、Logback）中的 API，而应依赖使用日志框架 SLF4J 中的 API，使用门面模式的日志框架，有利于维护和各个类的日志处理方式统一
    public static void main(String[] args) {
        System.out.println("System debug");
        System.out.println("System + info");
        System.out.println("System + warning");
        System.out.println("System error");

        //查看性能影响
        //logger.debug("debug {}"+ list);

        logger.debug("debug");
        logger.info("info");
        logger.warn("info");
        logger.error("error");


    }

}
/**
 * <?xml version="1.0" encoding="UTF-8"?>
 *
 * <configuration>
 *
 *     </appender>
 *     <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
 *         <!-- encoder的默认实现类是ch.qos.logback.classic.encoder.PatternLayoutEncoder -->
 *         <encoder>
 *             <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{5} - %msg%n</pattern>
 *         </encoder>
 *
 *
 *     <!-- name值可以是包名或具体的类名：该包（包括子包）下的类或该类将采用此logger -->
 *     <logger name="com.inspur.log.demo" level="ERROR" additivity="false">
 *         <appender-ref ref="STDOUT" />
 *     </logger>
 *
 *
 *     <!-- root的默认level是DEBUG -->
 *     <root level="DEBUG">
 *         <appender-ref ref="STDOUT" />
 *     </root>
 * </configuration>
 */
