package com.dongqiudi.dc.util;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Created by matrix on 17/12/20.
 */
public class UUIDUtils {private static String algorithm = "DES";
    private static String transformation = "DES/CBC/PKCS5PADDING";

    private static String initVector = "RCh2M8xE";
    private static String key = "T3qAL3Mh";

    private static SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(), algorithm);
    private static IvParameterSpec ivSpec = new IvParameterSpec(initVector.getBytes());

    public static String encrypt(String value) throws Exception {
        value += "zQcN6aR4";
        Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivSpec);

        byte[] encrypted = cipher.doFinal(value.getBytes());
        String uuid = Base64.encodeBase64String(encrypted);
        return "@" + uuid.replaceAll("\\s+", "");

    }

    public static String decrypt(String value) throws Exception {
        Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivSpec);

        byte[] encrypted = cipher.doFinal(Base64.decodeBase64(value));
        String uuid = new String(encrypted);
        return uuid.substring(0, uuid.length()-8);
    }

    public static void main(String[] args) throws Exception {
        String uuid = "@++ENnT+xWlE1PY3mfOVXlDuQvfTgm/bqC8PFMh1pLaWH+VWeQNZ9yIsYRsLkuTphmi/B4M7mK+Y=";
        uuid = "@++Emhat2TjcWvpKhILZo65QEAmrYAcZh";
        System.out.println(uuid);

        String uuidEN = UUIDUtils.decrypt(uuid.substring(1));
        System.out.println(uuidEN);

        String uuidDE = UUIDUtils.encrypt(uuidEN);
        System.out.println(uuidDE);

    }
}
