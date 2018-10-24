package com.jiuxiu.yxstat.utils;

import net.sf.json.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author hadoop
 */
public class HttpClient {

    /**
     * 发送http post请求
     *
     * @param callURL   请求的目标地址
     * @param resultMap 请求参数
     * @return
     */
    public static String post(String callURL, Map<String, Object> resultMap) throws IOException {

        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(400000).setConnectTimeout(400000)
                .setSocketTimeout(400000).setExpectContinueEnabled(false)
                .build();

        HttpPost httppost = new HttpPost(callURL);

        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();

        httppost.setHeader("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");

        if (resultMap != null) {
            List<BasicNameValuePair> formParams = new ArrayList<>();
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
            }
            UrlEncodedFormEntity uefEntity = new UrlEncodedFormEntity(formParams , "UTF-8");
            httppost.setEntity(uefEntity);
        }

        CloseableHttpResponse response = httpClient.execute(httppost);

        HttpEntity rspEntity = response.getEntity();
        InputStream in = rspEntity.getContent();

        String temp;
        BufferedReader data = new BufferedReader(new InputStreamReader(in, "utf-8"));
        StringBuffer result = new StringBuffer();
        while ((temp = data.readLine()) != null) {
            result.append(temp);
        }
        response.close();
        httpClient.close();
        return result.toString();
    }


    /**
     * 发送http post请求
     *
     * @param callURL 请求的目标地址
     * @return
     */
    public static String jsonPost(String callURL, String json) throws IOException {

        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(400000).setConnectTimeout(400000)
                .setSocketTimeout(400000).setExpectContinueEnabled(false)
                .build();

        HttpPost httppost = new HttpPost(callURL);

        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();

        httppost.setHeader("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");

        StringEntity rsqEntity = new StringEntity(json, "utf-8");
        rsqEntity.setContentEncoding("UTF-8");
        rsqEntity.setContentType("application/json");
        httppost.setEntity(rsqEntity);
        StringBuffer result;
        CloseableHttpResponse response;
        response = httpClient.execute(httppost);

        HttpEntity rspEntity = response.getEntity();
        InputStream in = rspEntity.getContent();

        String temp;
        BufferedReader data = new BufferedReader(new InputStreamReader(in, "utf-8"));
        result = new StringBuffer();
        while ((temp = data.readLine()) != null) {
            result.append(temp);
        }
        response.close();
        httpClient.close();
        return result.toString();
    }


    /**
     * 发送http post请求
     *
     * @param callURL 请求的目标地址
     * @return
     */
    public static String jsonPost(String callURL, Map<String,String> header , String json) throws IOException {

        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(400000).setConnectTimeout(400000)
                .setSocketTimeout(400000).setExpectContinueEnabled(false)
                .build();

        HttpPost httppost = new HttpPost(callURL);

        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();


        if(header != null && !header.isEmpty()){
            for (Map.Entry entry: header.entrySet()) {
                httppost.setHeader(entry.getKey().toString(), entry.getValue().toString());
            }
        }

        StringEntity rsqEntity = new StringEntity(json, "utf-8");
        rsqEntity.setContentEncoding("UTF-8");
        rsqEntity.setContentType("application/json");
        httppost.setEntity(rsqEntity);
        StringBuffer result;
        CloseableHttpResponse response;
        response = httpClient.execute(httppost);

        HttpEntity rspEntity = response.getEntity();
        InputStream in = rspEntity.getContent();

        String temp;
        BufferedReader data = new BufferedReader(new InputStreamReader(in, "utf-8"));
        result = new StringBuffer();
        while ((temp = data.readLine()) != null) {
            result.append(temp);
        }
        response.close();
        httpClient.close();
        return result.toString();
    }



    /**
     * 发送http post请求
     *
     * @param callURL   请求的目标地址
     * @param header    请求头信息
     * @param resultMap 请求参数
     * @param paramType 参数格式：1位键值对 key-value形式 其他为json格式
     * @return
     * @throws IOException
     * @throws ClientProtocolException
     */
    public static String post(String callURL, Map<String, Object> header, Map<String, Object> resultMap, String paramType)
            throws ClientProtocolException, IOException {

        RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout(400000).setConnectTimeout(400000)
                .setSocketTimeout(400000).setExpectContinueEnabled(false)
                .build();

        HttpPost httppost = new HttpPost(callURL);
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();

        /** 頭信息 **/
        if (header != null) {
            for (Map.Entry<String, Object> entry : header.entrySet()) {
                httppost.addHeader(entry.getKey(), entry.getValue().toString());
            }
        }

        if (resultMap != null) {
            if (paramType != null && paramType.equals("1")) {
                // key value 形式
                List<NameValuePair> formParams = new ArrayList<>();
                for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                    formParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
                }
                UrlEncodedFormEntity uefEntity = new UrlEncodedFormEntity(formParams, "UTF-8");
                httppost.setEntity(uefEntity);
            } else {
                StringEntity rsqentity = new StringEntity(JSONObject.fromObject(resultMap).toString(), "utf-8");
                rsqentity.setContentEncoding("UTF-8");
                rsqentity.setContentType("application/json");
                httppost.setEntity(rsqentity);
            }
        }
        CloseableHttpResponse response;
        response = httpClient.execute(httppost);

        HttpEntity rspEntity = response.getEntity();
        InputStream in = rspEntity.getContent();

        String temp;
        BufferedReader data = new BufferedReader(new InputStreamReader(in, "utf-8"));
        StringBuffer result = new StringBuffer();
        while ((temp = data.readLine()) != null) {
            result.append(temp);
        }
        response.close();
        httpClient.close();
        return result.toString();
    }


    public static String xml(String callURL, String xmlData) throws IOException {

        RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout(400000).setConnectTimeout(400000)
                .setSocketTimeout(400000).setExpectContinueEnabled(false)
                .build();

        HttpPost httppost = new HttpPost(callURL);
        CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();

        StringEntity entity = new StringEntity(xmlData, "UTF-8");
        httppost.setHeader("User-Agent", "Mozilla/5.0");

        httppost.setEntity(entity);
        CloseableHttpResponse response;
        response = httpClient.execute(httppost);

        HttpEntity rspEntity = response.getEntity();
        InputStream in = rspEntity.getContent();

        String temp;
        BufferedReader data = new BufferedReader(new InputStreamReader(in, "utf-8"));
        StringBuffer result = new StringBuffer();
        while ((temp = data.readLine()) != null) {
            result.append(temp);
        }
        response.close();
        httpClient.close();
        return result.toString();
    }
}
