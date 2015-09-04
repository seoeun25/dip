package com.nexr.dip.server;

import com.nexr.dip.DipLoaderException;
import com.nexr.dip.jpa.DipProperty;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.openjpa.lib.jdbc.DecoratingDataSource;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.text.MessageFormat;
import java.util.List;
import java.util.Properties;

public class JDBCService implements AppService {

    public static final String CONF_PREFIX = "dip.";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_DB_SCHEMA = CONF_PREFIX + "schema.name";
    public static final String CONF_CONN_DATA_SOURCE = CONF_PREFIX + "connection.data.source";
    public static final String CONF_CONN_PROPERTIES = CONF_PREFIX + "connection.properties";
    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";
    public static final String CONF_CREATE_DB_SCHEMA = CONF_PREFIX + "create.db.schema";
    public static final String CONF_VALIDATE_DB_CONN = CONF_PREFIX + "validate.db.connection";
    public static final String CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL = CONF_PREFIX + "validate.db.connection.eviction.interval";
    public static final String CONF_VALIDATE_DB_CONN_EVICTION_NUM = CONF_PREFIX + "validate.db.connection.eviction.num";
    public static final String CONF_VALIDATE_DB_CONN_QUERY = CONF_PREFIX + "validate.db.connection.query";

    public static String persistentUnit = "master-mysql";
    private static Logger LOG = LoggerFactory.getLogger(JDBCService.class);
    private EntityManagerFactory factory;

    private static JDBCService instance;

    private JDBCService() {

    }

    public static JDBCService getInstance() {
        if (instance == null) {
            instance = new JDBCService();
        }
        return instance;
    }

    private BasicDataSource getBasicDataSource() {
        BasicDataSource basicDataSource = null;
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        Object connectionFactory = spi.getConfiguration().getConnectionFactory();
        if (connectionFactory instanceof DecoratingDataSource) {
            DecoratingDataSource decoratingDataSource = (DecoratingDataSource) connectionFactory;
            basicDataSource = (BasicDataSource) decoratingDataSource.getInnermostDelegate();
        } else if (connectionFactory instanceof BasicDataSource) {
            basicDataSource = (BasicDataSource) connectionFactory;
        }
        return basicDataSource;
    }

    public void start() throws DipLoaderException {
        String dbSchema = DipContext.getContext().getConfig(CONF_DB_SCHEMA);
        String url = DipContext.getContext().getConfig(CONF_URL);
        String driver = DipContext.getContext().getConfig(CONF_DRIVER);
        String user = DipContext.getContext().getConfig(CONF_USERNAME);
        String password = DipContext.getContext().getConfig(CONF_PASSWORD).trim();
        String maxConn = DipContext.getContext().getConfig(CONF_MAX_ACTIVE_CONN).trim();
        String dataSource = DipContext.getContext().getConfig(CONF_CONN_DATA_SOURCE);
        String connPropsConfig = DipContext.getContext().getConfig(CONF_CONN_PROPERTIES);
        boolean autoSchemaCreation = Boolean.parseBoolean(DipContext.getContext().getConfig(CONF_CREATE_DB_SCHEMA));
        boolean validateDbConn = Boolean.parseBoolean(DipContext.getContext().getConfig(CONF_VALIDATE_DB_CONN));
        String evictionInterval = DipContext.getContext().getConfig(CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL).trim();
        String evictionNum = DipContext.getContext().getConfig(CONF_VALIDATE_DB_CONN_EVICTION_NUM).trim();
        String validationQuery = DipContext.getContext().getConfig(CONF_VALIDATE_DB_CONN_QUERY);

        if (!url.startsWith("jdbc:")) {
            throw new DipLoaderException("invalid JDBC URL, must start with 'jdbc:'");
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new DipLoaderException("invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }

        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, password, maxConn);
        Properties props = new Properties();
        if (autoSchemaCreation || validateDbConn) {
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true";
            if (validateDbConn) {
                String interval = "timeBetweenEvictionRunsMillis=" + evictionInterval;
                String num = "numTestsPerEvictionRun=" + evictionNum;
                connProps += "," + interval + "," + num;
                connProps += ",ValidationQuery=" + validationQuery;
                connProps = MessageFormat.format(connProps, dbSchema);
            }
            if (autoSchemaCreation) {
                props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
            }
        } else {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
        }
        if (connPropsConfig != null) {
            connProps += "," + connPropsConfig;
        }
        props.setProperty("openjpa.ConnectionProperties", connProps);

        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        factory = Persistence.createEntityManagerFactory(persistentUnit, props);

        EntityManager entityManager = getEntityManager();
        entityManager.find(DipProperty.class, 1);

        LOG.info("All entities initialized");
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        String logMsg = spi.getConfiguration().getConnectionProperties().replaceAll("Password=.*?,", "Password=***,");
        LOG.info("JDBC configuration: {}", logMsg);
        entityManager.getTransaction().commit();
        entityManager.close();

        getBasicDataSource();
    }

    public EntityManager getEntityManager() {
        return factory.createEntityManager();
    }

    public Object executeGet(String namedQueryName, Query query, EntityManager em) {
        try {

            Object obj = null;
            try {
                obj = query.getSingleResult();
            } catch (NoResultException e) {
                // return null when no matched result
            }
            return obj;
        } finally {
            processFinally(em, namedQueryName, false);
        }
    }

    public List<?> executeGetList(String namedQueryName, Query query, EntityManager em) {
        try {

            List<?> resultList = null;
            try {
                resultList = query.getResultList();
            } catch (NoResultException e) {
                // return null when no matched result
            }
            return resultList;
        } finally {
            processFinally(em, namedQueryName, false);
        }
    }

    public int executeUpdate(String namedQueryName, Query query, EntityManager em) throws DipLoaderException {
        try {

            LOG.trace("Executing Update/Delete Query [{0}]", namedQueryName);
            em.getTransaction().begin();
            int ret = query.executeUpdate();
            if (em.getTransaction().isActive()) {
                em.getTransaction().commit();
            }
            return ret;
        }
        catch (PersistenceException e) {
            throw new DipLoaderException("Failed to update", e);
        }
        finally {
            processFinally(em, namedQueryName, true);
        }
    }

    private void processFinally(EntityManager em, String name, boolean checkActive) {
        if (checkActive) {
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("[{}] ended with an active transaction, rolling back", name);
                    em.getTransaction().rollback();
                }
            } catch (Exception ex) {
                LOG.warn("Could not check/rollback transaction after [{}], {}", name + ex.getMessage(), ex);
            }
        }
        try {
            if (em.isOpen()) {
                em.close();
            } else {
                LOG.warn("[{0}] closed the EntityManager, it should not!", name);
            }
        } catch (Exception ex) {
            LOG.warn("Could not close EntityManager after [{}], {}", name + ex.getMessage(), ex);
        }
    }

    @Override
    public void shutdown() {
        if (factory != null && factory.isOpen()) {
            factory.close();
        }
    }
}
