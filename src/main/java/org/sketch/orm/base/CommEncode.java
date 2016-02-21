package org.sketch.orm.base;

public class CommEncode {

    private static int USERID_LEN = 3;

    public static String generateId() {
        String result = random(USERID_LEN);
        return result;
    }

    // 传入的字符串的长度
    public static String random(int length) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int x = 10 + (int) (Math.random() * 90);
            result.append(String.valueOf(x));
        }
        return result.toString();
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(CommEncode.generateId());
        }

    }


}

