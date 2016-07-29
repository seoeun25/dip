package com.nexr.jpa;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nexr.Schemas;
import com.nexr.dip.DipException;
import com.nexr.dip.jpa.JDBCService;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.server.DipSchemaRepoServer;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class SchemaInfoQueryExecutorTest {

    private static JDBCService jdbcService;
    private static SchemaInfoQueryExceutor queryExecutor;

    @BeforeClass
    public static void setupClass() {
        try {
            System.setProperty("persistenceUnit", "repo-test-hsql");
            Injector injector = Guice.createInjector(new DipSchemaRepoServer());
            DipSchemaRepoServer app = injector.getInstance(DipSchemaRepoServer.class);
            jdbcService = injector.getInstance(JDBCService.class);

            Thread.sleep(1000);
            queryExecutor = injector.getInstance(SchemaInfoQueryExceutor.class);
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
    public void testInsert() {

        try {
            SchemaInfo schemaInfo1 = new SchemaInfo(Schemas.employee, Schemas.employee_schema1);
            int id1 = (Integer) queryExecutor.insertR(schemaInfo1);
            SchemaInfo schemaInfo2 = new SchemaInfo(Schemas.employee, Schemas.employee_schema2);
            int id2 = (Integer) queryExecutor.insertR(schemaInfo2);

            Thread.sleep(100);
            List<SchemaInfo> all = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_ALL, new Object[]{});
            Assert.assertEquals(1, all.size());
            System.out.println("all topics size : " + all.size());

            // different wording, same schema
            Assert.assertNotSame(id1, id2);
            Assert.assertNotSame(schemaInfo1.toString(), schemaInfo2.toString());
            Assert.assertEquals(new Schema.Parser().parse(schemaInfo1.getSchemaStr()), new Schema.Parser().parse(schemaInfo2.getSchemaStr()));

            SchemaInfo schemaInfo3 = new SchemaInfo(Schemas.employee, Schemas.employee_schema3);
            int id3 = (Integer) queryExecutor.insertR(schemaInfo3);

            SchemaInfo ftthInfo = new SchemaInfo(Schemas.ftth_if, Schemas.ftth_if_schema);
            int id4 = (Integer) queryExecutor.insertR(ftthInfo);
            System.out.println("all latest id : " + id4);


            Thread.sleep(100);

            SchemaInfo latest = queryExecutor.getListMaxResult1(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICLATEST, new
                    Object[]{Schemas.employee});
            System.out.println("latest employee id : " + latest.getId());
            Assert.assertEquals(schemaInfo3.getId(), latest.getId());


            Thread.sleep(100);

            all = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_ALL, new Object[]{});
            System.out.println("all topics size : " + all.size());
            Assert.assertEquals(2, all.size());

            SchemaInfo schemaInfo_2 = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYID, new Object[]{id2});
            Assert.assertEquals(id2, schemaInfo_2.getId());
            Assert.assertNotSame(schemaInfo_2.getId(), latest.getId());


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetNegative() {
        try {
            Object[] params = new Object[]{"abc"};
            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYSCHEMA, params);

            Assert.fail("Should there no schemaInfo");
        } catch (DipException e) {
            // succeed
        }
    }

}

