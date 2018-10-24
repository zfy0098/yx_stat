package com.jiuxiu.yxstat.utils;

import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Created with IDEA by hadoop on 2018/8/27.
 *
 * @author Choufy
 */
public class EncryptUtil {


    public static String md5Password(String password) throws NoSuchAlgorithmException {

        MessageDigest digest = MessageDigest.getInstance("md5");
        byte[] result = digest.digest(password.getBytes());
        StringBuffer buffer = new StringBuffer();
        for (byte b : result) {
            // 与运算
            int number = b & 0xff;
            String str = Integer.toHexString(number);
            if (str.length() == 1) {
                buffer.append("0");
            }
            buffer.append(str);
        }
        return buffer.toString();
    }

    /**
     * 使用 HMAC-SHA1 签名方法对data进行签名
     *
     * @param data 被签名的字符串
     * @param key  密钥
     * @return 加密后的字符串
     */
    public static String genHMAC(String data, String key) throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] result = null;
        //根据给定的字节数组构造一个密钥,第二参数指定一个密钥算法的名称
        SecretKeySpec signinKey = new SecretKeySpec(key.getBytes(), "HmacSHA1");
        //生成一个指定 Mac 算法 的 Mac 对象
        Mac mac = Mac.getInstance("HmacSHA1");
        //用给定密钥初始化 Mac 对象
        mac.init(signinKey);
        //完成 Mac 操作
        byte[] rawHmac = mac.doFinal(data.getBytes());
        result = Base64.getEncoder().encode(rawHmac);
        return new String(result);
    }

}
