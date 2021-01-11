package personal.wqm.springbootdemo;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CSVWriter {

    @Test
    public void testWrite() throws Exception {
        FileOutputStream fos = new FileOutputStream("E:/abcd.csv");
        OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");
        String headers[] = "dbname,ac0018,name,mobile,tel,addr,org_addr,freight,totalcommodityvalue,totalorderamount,paytime,buyerid,platform,email,refundamount,discountamount,ordersubmissiontime,tracking_number,express_company,shippedqty,univalent,unitDiscountAmount,barcode,sku,tradename,brand,tradename_category".split(",");


        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(headers);
        CSVPrinter csvPrinter = new CSVPrinter(osw, csvFormat);

//        csvPrinter = CSVFormat.DEFAULT.withHeader("姓名", "年龄", "家乡").print(osw);

        String content[] = "wqm,00043436,wqm,18762716887,0531,山东,山东,裁切,fafa,jfklajfla,2020年11月18日,fafa,fafha,fa,uophjkl,8907901,7979019011,697657980,顺丰,fafa,709998,80790809,faa,fafa,faa,u1o,uouopo".split(",");

        for (int i = 0; i < 30; i++) {
            csvPrinter.printRecord(content);
            if (i % 10000000 == 0) {
                System.out.println(i + "条生成完成");

            }
        }

        csvPrinter.flush();
        csvPrinter.close();

    }

    @Test
    public void testRead() throws IOException {
        InputStream is = new FileInputStream("E:/abcd.csv");
        InputStreamReader isr = new InputStreamReader(is, "GBK");
        Reader reader = new BufferedReader(isr);
       // String headers[] = "dbname,ac0018,name,mobile,tel,addr,org_addr,freight,totalcommodityvalue,totalorderamount,paytime,buyerid,platform,email,refundamount,discountamount,ordersubmissiontime,tracking_number,express_company,shippedqty,univalent,unitDiscountAmount,barcode,sku,tradename,brand,tradename_category".split(",");

        CSVParser parser = CSVFormat.EXCEL.withHeader().parse(reader);
        System.out.println(parser.getHeaderMap());;
        Map<String, Integer> headerMap = parser.getHeaderMap();

        List<String> headerList = new ArrayList<>();
        headerMap.forEach((key,value)->{
            headerList.add(String.valueOf(key));
        });

        String[] strings = new String[headerList.size()];

        String headers[] =  headerList.toArray(strings);


        Iterator<CSVRecord> csvIterator = parser.iterator();
        while (csvIterator.hasNext()) {
            CSVRecord record = csvIterator.next();
            Map<String,String> map=  record.toMap();
            System.out.println(map);

        }


    }

}
