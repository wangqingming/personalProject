package com.inspur.httpdemo;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Vote {
    public static void main(String[] args) throws Exception {
        String url = "https://reg.ai-expo.org.cn/pingjiang/aiexpo_vote.aspx/Toupiao";

        Map<String, String> paramaters = new HashMap<>();
        paramaters.put("id", "36");
        paramaters.put("type", "jyDB");
        String content = JSON.toJSONString(paramaters);
        for (int i = 0; i < 5000; i++) {

            String result = post(content, url);
            System.out.println(result + i);
            //Thread.sleep(30);

        }


    }

    public static String post(String json, String url) {
        String result = "";
        HttpPost post = new HttpPost(url);
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();

            post.setHeader("Content-Type", "application/json;charset=utf-8");
            StringEntity postingString = new StringEntity(json, "utf-8");
            post.setEntity(postingString);
            HttpResponse response = httpClient.execute(post);

            InputStream in = response.getEntity().getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
            StringBuilder strber = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                strber.append(line + '\n');
            }
            br.close();
            in.close();
            result = strber.toString();
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                result = "服务器异常";
            }
        } catch (Exception e) {
            System.out.println("请求异常");
            throw new RuntimeException(e);
        } finally {
            post.abort();
        }
        return result;
    }
}
