package com.nexr.dip.common;

import org.junit.Test;

public class UtilsTest {

    @Test
    public void test(){
        String s = "1462953610609";
        long time = Long.valueOf(s);
        String sTime = Utils.getDateString(time);
        System.out.println(sTime);
    }

    @Test
    public void testUTC(){
        String s = "1462953610609";
        long time = Long.valueOf(s);
        String sTime = Utils.formatTime(time, "yyyy-MM-dd HH:mm:ss", "UTC");
        System.out.println(sTime);
    }
}
