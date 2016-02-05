package com.nexr.server;

import com.nexr.dip.DipException;
import com.nexr.Schemas;
import com.nexr.dip.jpa.JDBCService;
import com.nexr.jpa.SchemaInfoQueryExceutor;
import com.nexr.schemaregistry.SchemaInfo;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class JDBCServiceTest {

    private static JDBCService jdbcService;
    private static SchemaInfoQueryExceutor queryExecutor;

    private static long id;

    @BeforeClass
    public static void setupClass() {
        try {
            jdbcService = JDBCService.getInstance("schemarepo", "repo-master-mysql");
            //initJDBConfiguration();
            jdbcService.start();
            Thread.sleep(1000);
            queryExecutor = new SchemaInfoQueryExceutor(jdbcService);
            initData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() {
        jdbcService.shutdown();
        jdbcService = null;
    }

    private static void initJDBConfiguration() {
//        DipSchemaRepoContext.getContext().setConfig(JDBCServiceOld.CONF_URL, "jdbc:derby:memory:myDB;create=true");
//        DipSchemaRepoContext.getContext().setConfig(JDBCServiceOld.CONF_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver");
    }

    private static void initData() {
        try {
            SchemaInfo schemaInfo = new SchemaInfo(Schemas.ftth_if, "{\"namespace\" : \"" + System.currentTimeMillis() + ":a\"}");

            System.out.println("initData: " +queryExecutor.insertR(schemaInfo) + " / " + Schemas.ftth_if);
            Thread.sleep(1000);
            schemaInfo = new SchemaInfo(Schemas.ftth_if, Schemas.ftth_if_schema);
            System.out.println("initData: " +queryExecutor.insertR(schemaInfo) + " / " + Schemas.ftth_if);
            id = schemaInfo.getId();
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsert() {

        try {
            Object ftthifId = insert(Schemas.ftth_if, Schemas.ftth_if_schema);
            Thread.sleep(1000);
            Object employeeId = insert(Schemas.employee, Schemas.employee_schema);

            // already exist
            Thread.sleep(1000);
            Object ftthifId2 = insert(Schemas.ftth_if, Schemas.ftth_if_schema);
            Assert.assertEquals(ftthifId.toString(), ftthifId2.toString());

            Thread.sleep(1000);
            long currentTime = System.currentTimeMillis();
            String topicName = "ftthif-" + currentTime;
            // same schema under the different subject
            Object obj = insert(topicName, Schemas.ftth_if_schema);
            Assert.assertFalse(ftthifId.toString().equals(obj.toString()));
            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICLATEST, new Object[]{topicName});
            Assert.assertNotNull(schemaInfo);
            Assert.assertEquals(Schemas.ftth_if_schema, schemaInfo.getSchemaStr());

            Thread.sleep(1000);
            // different schema under the exist schema
            String schemaStr = "{\"namespace\" : \"" + currentTime + "\"}";
            obj = insert(Schemas.ftth_if, schemaStr);

            Thread.sleep(1000);
            schemaInfo = queryExecutor.getListMaxResult1(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICLATEST, new Object[]{Schemas.ftth_if});
            //Assert.assertEquals(schemaStr, schemaInfo.getSchemaStr());
            Assert.assertEquals(obj.toString(), String.valueOf(schemaInfo.getId()));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Object insert(String name, String schemaStr) {
        SchemaInfo schemaInfo = null;
        try {
            schemaInfo = queryExecutor.getListMaxResult1(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICLATEST, new Object[]{name});
        } catch (DipException e) {
            // not exist, need to insert
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        try {
            if (schemaInfo == null || !schemaInfo.getSchemaStr().equals(schemaStr)) {
                schemaInfo = new SchemaInfo(name, schemaStr);
                Long obj = (Long)queryExecutor.insertR(schemaInfo);
                System.out.println("-- new registered id :" + obj + " / " + name);
                schemaInfo.setId(obj.longValue());
            } else {
                System.out.println("already exist : " + schemaInfo.getId() + " / " + name);
            }

        } catch (Exception e) {
            Assert.fail(e.toString());
        }
        return new Long(schemaInfo.getId());
    }

    @Test
    public void getGetSchemaByTopicLatest() {
        try {
            SchemaInfoQueryExceutor queryExecutor = new SchemaInfoQueryExceutor(jdbcService);
            Object[] params = new java.lang.Object[]{Schemas.ftth_if};

            SchemaInfo schemaInfo = queryExecutor.getListMaxResult1(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICLATEST, params);
            Assert.assertNotNull(schemaInfo);
            System.out.println("---- by schema(ftth_if) : \n" + schemaInfo.toString());
            Assert.assertEquals(Schemas.ftth_if, schemaInfo.getName());

        } catch (DipException e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void getGetSchemaByTopicAndID() {
        try {
            SchemaInfoQueryExceutor queryExecutor = new SchemaInfoQueryExceutor(jdbcService);
            Object[] params = new java.lang.Object[]{Schemas.ftth_if, id};

            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICANDID, params);
            Assert.assertNotNull(schemaInfo);
            System.out.println("---- by schema(ftthi) : \n" + schemaInfo.toString());
            Assert.assertEquals(Schemas.ftth_if, schemaInfo.getName());
            Assert.assertEquals(id, schemaInfo.getId());

        } catch (DipException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetNegative() {
        try {
            SchemaInfoQueryExceutor queryExecutor = new SchemaInfoQueryExceutor(jdbcService);
            Object[] params = new java.lang.Object[]{"abc"};
            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYSCHEMA, params);

            Assert.fail("Should there no schemaInfo");
        } catch (DipException e) {
            // succeed
        }
    }

    @Test
    public void testGetSchemaInfos() {
        try {
            SchemaInfoQueryExceutor queryExecutor = new SchemaInfoQueryExceutor(jdbcService);
            Object[] params = new java.lang.Object[]{Schemas.ftth_if_schema};
            List<SchemaInfo> schemaList = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYSCHEMA, params);
            for (SchemaInfo schemaInfo : schemaList) {
                System.out.println("---- by schema(ftth_if) : \n" + schemaInfo.toString());
                Assert.assertEquals(Schemas.ftth_if_schema, schemaInfo.getSchemaStr());
            }

            long id = 5;
            params = new java.lang.Object[]{id};
            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYID, params);
            System.out.println("---- by id(" + id + ") : \n" + schemaInfo.toString());
            Assert.assertEquals(id, schemaInfo.getId());

            id = 6;
            params = new java.lang.Object[]{id};
            schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYID, params);
            System.out.println("---- by id(" + id + ") : \n" + schemaInfo.toString());
            Assert.assertEquals(id, schemaInfo.getId());

        } catch (DipException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetSchemaAllLatest() {
        try {
            SchemaInfoQueryExceutor queryExecutor = new SchemaInfoQueryExceutor(jdbcService);
            Object[] params = new java.lang.Object[]{};
            List<SchemaInfo> schemaList = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_ALL, params);
            for (SchemaInfo schemaInfo : schemaList) {
                System.out.println("---- schema all latest : \n" + schemaInfo.toString());
            }
        } catch (DipException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}

