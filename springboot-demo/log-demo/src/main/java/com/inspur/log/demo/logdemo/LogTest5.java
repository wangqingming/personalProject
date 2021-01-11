package com.inspur.log.demo.logdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LogTest5 {
    private static final Logger logger = LoggerFactory.getLogger(LogTest5.class);

    public static void main(String[] args) {

        new LogTest5().method1();

    }

    /**
     * 【强制】异常信息应该包括两类信息：案发现场信息和异常堆栈信息。如果不处理，那么通过 关键字 throws 往上抛出。
     *
     * 现场信息
     * 异常堆栈
     *
     * 8. 【推荐】尽量用英文来描述日志错误信息，如果日志中的错误信息用英文描述不清楚的话使用 中文描述即可，否则容易产生歧义。国际化团队或海外部署的服务器由于字符集问题，【强制】 使用全英文来注释和描述日志错误信息。
     */
    private void method1() {
        //此处使用运行时异常来演示，并不特别合适，因为正常情况下，运行时异常不应该捕获。应该进行判断，只演示日志的记录效果是可以的
        try {
            int i = 10 / 0;
        } catch (Exception e) {
            logger.error(""+ e);//直接拼接e  method1 error
           // logger.error("method1 error ", e);//只有一个e的时候，当做为参数，一定要做为最后一个参数
           // logger.error("method1 {} error {} ",e,"名字");//占位符合，动态数组，异常一定要做为最后一个参数
          //  logger.error("method1 {}, 在 {} 发生error ", new Object[]{"名字",new Date(), e});//数组的方式传递参数，异常一定要做为最后一个参数,用汉语名字不对
            //logger.error("method1 {} error {} ", new Object[]{e, "名字"});//数组的方式传递参数，异常一定要做为最后一个参数,不做为最后一个参数的情况下，当做字符串来处理


        }
    }

    private void method2() {

        try {
            int i = 10 / 0;
        } catch (Exception e) {
            logger.error(""+ e);
        }
    }

}
