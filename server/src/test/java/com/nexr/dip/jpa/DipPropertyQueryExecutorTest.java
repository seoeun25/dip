package com.nexr.dip.jpa;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nexr.dip.DipException;
import com.nexr.dip.server.DipServer;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DipPropertyQueryExecutorTest {

    private static JDBCService jdbcService;
    private static DipPropertyQueryExecutor dipPropertyQueryExecutor;

    @BeforeClass
    public static void setupClass() {
        try {
            System.setProperty("persistenceUnit", "dip-test-hsql");
            Injector injector = Guice.createInjector(new DipServer());
            DipServer app = injector.getInstance(DipServer.class);
            jdbcService = injector.getInstance(JDBCService.class);

            Thread.sleep(1000);
            dipPropertyQueryExecutor = injector.getInstance(DipPropertyQueryExecutor.class);

            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() {
        jdbcService.shutdown();
        jdbcService = null;
    }


    @Test
    public void testInsertDipProperty() {

        try {
            String key1 = "dip.schemaregistry.url";
            DipProperty dipProperty = new DipProperty();
            dipProperty.setName(key1);
            dipProperty.setValue("http://hello.host.com:18181/repo");
            dipPropertyQueryExecutor.insert(dipProperty);

            String key2 = "dip.load.task.count";
            dipProperty = new DipProperty();
            dipProperty.setName(key2);
            dipProperty.setValue("15");
            dipPropertyQueryExecutor.insert(dipProperty);

            DipProperty dipProperty1 = dipPropertyQueryExecutor.get(DipPropertyQueryExecutor.DipPropertyQuery
                    .GET_DIPPROPERTY_BY_NAME, new Object[]{key1});

            DipProperty dipProperty2 = dipPropertyQueryExecutor.get(DipPropertyQueryExecutor.DipPropertyQuery
                    .GET_DIPPROPERTY_BY_NAME, new Object[]{key2});

            Assert.assertEquals(key1, dipProperty1.getName());
            Assert.assertEquals("http://hello.host.com:18181/repo", dipProperty1.getValue());

            Assert.assertEquals(key2, dipProperty2.getName());
            Assert.assertEquals("15", dipProperty2.getValue());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetListDipProperty() {
        try {
            String key1 = "dip.hiveserver.user";
            DipProperty dipProperty1 = new DipProperty();
            dipProperty1.setName(key1);
            dipProperty1.setValue("sdip-user");
            dipPropertyQueryExecutor.insert(dipProperty1);

            List<DipProperty> list = dipPropertyQueryExecutor.getList(DipPropertyQueryExecutor.DipPropertyQuery
                    .GET_DIPPROPERTY_ALL, new Object[]{});
            for (DipProperty dipProperty : list) {
                System.out.println(dipProperty.getName() + "=" + dipProperty.getValue());
                if (dipProperty.getName().equals("dip.hiveserver.user")) {
                    Assert.assertEquals("sdip-user", dipProperty.getValue());
                }
            }
        } catch (DipException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
