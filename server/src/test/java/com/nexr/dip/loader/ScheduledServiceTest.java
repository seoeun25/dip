package com.nexr.dip.loader;

import com.nexr.dip.DipLoaderException;
import com.nexr.dip.common.Utils;
import com.nexr.dip.server.DipContext;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Calendar;

public class ScheduledServiceTest {

    private static ScheduledService scheduledService;

    @BeforeClass
    public static void beforeClass() {
        DipContext.getContext();
        scheduledService = ScheduledService.getInstance();
        try {
            scheduledService.start();
        }catch (DipLoaderException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInitialExecutionTime() {

        try {
            long aTime = scheduledService.getInitialExecutionTime(Calendar.getInstance());
            System.out.println(Utils.formatTime(aTime));

            long time = Utils.parseTime("2015-09-01 10:56:01,000");
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(time);
            long bTime = scheduledService.getInitialExecutionTime(calendar);
            Assert.assertEquals("2015-09-01 11:00:00", Utils.getDateString(bTime));

            time = Utils.parseTime("2015-09-01 11:05:01,000");
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(time);
            bTime = scheduledService.getInitialExecutionTime(calendar);
            Assert.assertEquals("2015-09-01 11:10:00", Utils.getDateString(bTime));
        }catch (Exception e ){
            e.printStackTrace();
        }

    }
}
