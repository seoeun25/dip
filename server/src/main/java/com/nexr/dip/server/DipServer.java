package com.nexr.dip.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.nexr.dip.AppService;
import com.nexr.dip.Context;
import com.nexr.dip.DipException;
import com.nexr.dip.jpa.DipProperty;
import com.nexr.dip.jpa.DipPropertyQueryExecutor;
import com.nexr.dip.jpa.JDBCService;
import com.nexr.dip.jpa.LoadResultQueryExecutor;
import com.nexr.dip.loader.HDFSClient;
import com.nexr.dip.loader.ScheduledService;
import com.nexr.dip.loader.TopicManager;
import com.nexr.dip.module.DipWebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;


/**
 * DipServer is the web based server. DipServer load the messages from the kafka cluster to HDFS periodically and connect the
 * result data files to the Hive.
 */
public class DipServer extends AbstractModule implements AppService {

    private static Logger LOG = LoggerFactory.getLogger(DipServer.class);
    private static DipServer dipServer;

    @Inject
    private ScheduledService scheduledService;

    @Inject
    private Context context;

    @Inject
    private JDBCService jdbcService;

    @Inject
    private DipPropertyQueryExecutor dipPropertyQueryExecutor;

    @Inject
    private DipWebServer jettyWebServer;

    public DipServer() {

    }

    public static void main(String[] args) {
        String cmd = args[0];

        if ("start".equals(cmd)) {
            Injector injector = Guice.createInjector(ImmutableList.of(new DipServer()));
            DipServer app = injector.getInstance(DipServer.class);

            ShutdownInterceptor shutdownInterceptor = new ShutdownInterceptor(app);
            Runtime.getRuntime().addShutdownHook(shutdownInterceptor);


            try {
                app.start();
            } catch (DipException e) {
                e.printStackTrace();
            }
        } else if ("stop".equals(cmd)) {

        }
    }

    @Override
    protected void configure() {
        String persistUnit = System.getProperty("persistenceUnit") == null ? "dip-master-mysql" : System.getProperty("persistenceUnit");
        bindConstant().annotatedWith(Names.named("persistenceUnit")).to(persistUnit);
        bindConstant().annotatedWith(Names.named("persistenceName")).to("dip");
        bindConstant().annotatedWith(Names.named("siteConfig")).to("dip.conf");
        bindConstant().annotatedWith(Names.named("defaultConfig")).to("dip-default.conf");
        bind(Context.class).in(Singleton.class);
        bind(JDBCService.class).in(Singleton.class);
        bind(ScheduledService.class).in(Singleton.class);
        bind(DipWebServer.class).in(Singleton.class);
        bind(DipPropertyQueryExecutor.class).in(Singleton.class);
        bind(LoadResultQueryExecutor.class).in(Singleton.class);
        bind(HDFSClient.class).in(Singleton.class);

    }

    private void init() {
        try {
            initConfigFromDB();

            Properties properties = context.getProperties();
            for (String key : properties.stringPropertyNames()) {
                LOG.debug("[Configs ] " + key + " = " + properties.getProperty(key));
            }

            Thread.sleep(1000);
            scheduledService.start();
            //scheduledService.debugLoaderStatus();


        } catch (Exception e) {
            LOG.error("Fail to init services ", e);
        }
    }


    private void initConfigFromDB() {
        Properties properties = getPersistTopicProperties();
        for (String name : properties.stringPropertyNames()) {
            context.setConfig(name, properties.getProperty(name));
        }
    }

    public Properties getPersistTopicProperties() {
        Properties properties = new Properties();
        try {
            List<DipProperty> dipPropertyList = dipPropertyQueryExecutor.getList(DipPropertyQueryExecutor.DipPropertyQuery.GET_DIPPROPERTY_ALL, new
                    Object[]{});
            for (DipProperty dipProperty : dipPropertyList) {
                properties.put(dipProperty.getName(), dipProperty.getValue());
            }
        } catch (DipException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public void start() throws DipException {
        LOG.info("========= DipServer Starting ......   ========");

        init();

        try {
            jettyWebServer.start();
            LOG.info("DipServer Started !! ");
            jettyWebServer.join();
        } catch (Exception e) {
            LOG.error("Error starting Jetty. DipServer may not be available.", e);
        }

    }

    public void shutdown() throws DipException {

        try {
            shutdownService();

            jettyWebServer.shutdown();
        } catch (Exception ex) {
            LOG.error("Error stopping Jetty. DipServer may not be available.", ex);
        }
        LOG.info("========= DipServer Shutdown ======== \n");

    }

    private boolean prepareShutdown() throws Exception {
        LOG.info("Start Shutdown");
        boolean shutdown = false;
        for (int i = 0; i < 5; i++) {
            shutdown = scheduledService.prepareShutdown();
            if (shutdown) {
                LOG.info("ScheduledService is shutdown. Safe to stop. \n");
                break;
            } else {
                LOG.warn("ScheduledService has running topic !!! See the resource !!! \n");
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    LOG.info("", e);
                }
            }
        }
        LOG.info("------------");
        scheduledService.debugTaskMangerStatus(true);
        LOG.info("------------");
        return shutdown;
    }

    private void shutdownService() {
        try {
            prepareShutdown();
            Thread.sleep(1000);
        } catch (Exception e) {
            LOG.info("Fail to prepareShutdown", e);
        }

        if (jdbcService != null) {
            jdbcService.shutdown();
        }
        if (scheduledService != null) {
            scheduledService.shutdown();
        }

    }

    private static class ShutdownInterceptor extends Thread {

        private AppService app;

        public ShutdownInterceptor(AppService app) {
            this.app = app;
        }

        public void run() {
            System.out.println("Call the shutdown routine");
            try {
                app.shutdown();
            } catch (DipException e) {
                e.printStackTrace();
            }
        }
    }

}
