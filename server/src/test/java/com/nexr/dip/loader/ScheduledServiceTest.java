package com.nexr.dip.loader;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nexr.dip.DipLoaderException;
import com.nexr.dip.common.Utils;
import com.nexr.dip.server.DipServer;
import junit.framework.Assert;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Calendar;

public class ScheduledServiceTest {

    private static ScheduledService scheduledService;
    private static DipServer dipServer;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("persistenceUnit", "dip-test-hsql");
        Injector injector = Guice.createInjector(new DipServer());
        dipServer = injector.getInstance(DipServer.class);
        startServer();
        System.out.println("after start server");


        try {
            Thread.sleep(2000);
            System.out.println("before scheudler");
            scheduledService = injector.getInstance(ScheduledService.class);
            scheduledService.debugLoaderStatus();
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            shutdownServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutdownServer();
    }

    private static void startServer() {
        try {
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        dipServer.start();
                    } catch (Exception e) {
                        System.out.println("----------  exception during server start ----------");
                        e.printStackTrace();
                    }

                }
            });
            t1.start();

            Thread.sleep(1000);
            System.out.println("---- server started !!! ----");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void shutdownServer() {
        try {
            dipServer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() {
        HttpClient httpClient = new HttpClient();
        httpClient.setFollowRedirects(false);
        try {
            httpClient.start();
            Thread.sleep(100);

            ContentResponse response = httpClient.GET("http://localhost:3838/dip/v1/admin");
            System.out.println("  status : " + response.getStatus());
            Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

            response = httpClient.GET("http://localhost:3838/dip/v1/hello");
            System.out.println("  status : " + response.getStatus());
            Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

        } catch (Exception e) {
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
            Assert.assertEquals("2015-09-01 10:57:00", Utils.getDateString(bTime));

            time = Utils.parseTime("2015-09-01 11:05:01,000");
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(time);
            bTime = scheduledService.getInitialExecutionTime(calendar);
            Assert.assertEquals("2015-09-01 11:06:00", Utils.getDateString(bTime));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
