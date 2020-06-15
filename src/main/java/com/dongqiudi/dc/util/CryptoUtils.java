package com.dongqiudi.dc.util;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * 封装加解密
 * Created by matrix on 17/7/29.
 */
public class CryptoUtils {
    private static String algorithm = "AES";
    private static String transformation = "AES/CFB/NoPadding";

    private static String initVector = "dqdscryptoaeskey";
    private static String key = "dqdscryptoaeskey32lengthsaltislo";

    private static SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(), algorithm);
    private static IvParameterSpec ivSpec = new IvParameterSpec(initVector.getBytes());

    public static String encrypt(String value) throws Exception {
        Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivSpec);

        byte[] encrypted = cipher.doFinal(value.getBytes());
        return Base64.encodeBase64String(encrypted);
    }

    public static String decrypt(String value) throws Exception {
        Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivSpec);

        byte[] encrypted = cipher.doFinal(Base64.decodeBase64(value));
        return new String(encrypted);
    }
}
