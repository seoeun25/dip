package com.nexr.dip.server;

import com.nexr.dip.AppService;
import com.nexr.dip.DipException;
import com.nexr.dip.DipLoaderException;
import com.nexr.dip.jpa.DipProperty;
import com.nexr.dip.jpa.DipPropertyQueryExecutor;
import com.nexr.dip.jpa.JDBCService;
import com.nexr.dip.loader.ScheduledService;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;


/**
 * DipServer is the web based server. DipServer load the messages from the kafka cluster to HDFS periodically and connect the
 * result data files to the Hive.
 */
public class DipServer implements AppService {

    public static int DEFAULT_PORT = 17171;
    private static Logger LOG = LoggerFactory.getLogger(DipServer.class);
    private static DipServer dipServer;
    public int PORT = DEFAULT_PORT;
    private Server jettyServer;

    private DipContext dipContext;
    private JDBCService jdbcService;
    private ScheduledService scheduledService;

    private DipServer() {

    }

    public static DipServer getInstance() {
        if (dipServer == null) {
            dipServer = new DipServer();
        }
        return dipServer;
    }

    public static void main(String[] args) {
        String cmd = args[0];

        if ("start".equals(cmd)) {
            AppService app = DipServer.getInstance();
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

    private void init() {
        try {
            initContext();
            initJDBCService();
            initConfigFromDB();

            Properties properties = DipContext.getContext().getProperties();
            for (String key : properties.stringPropertyNames()) {
                LOG.info("[Configs ] " + key + " = " + properties.getProperty(key));
            }

            initServices();

            Thread.sleep(1000);
            scheduledService.debugLoaderStatus();

        } catch (Exception e) {
            LOG.error("Fail to init services ", e);
        }
    }

    private void initContext() {
        dipContext = DipContext.getContext();
        String sPort = dipContext.getConfig("dip.port");
        PORT = sPort == null ? DEFAULT_PORT : Integer.parseInt(sPort);
    }

    private void initJDBCService() throws DipException {
        jdbcService = JDBCService.getInstance("dip", "dip-master-mysql");
        jdbcService.start();
    }

    private void initServices() throws DipLoaderException {
        scheduledService = ScheduledService.getInstance();
        scheduledService.start();
    }

    private void initConfigFromDB() {
        Properties properties = getJDBCTopicProperties(jdbcService);
        for (String name : properties.stringPropertyNames()) {
            DipContext.getContext().setConfig(name, properties.getProperty(name));
        }
    }

    public Properties getJDBCTopicProperties(JDBCService jdbcService) {
        Properties properties = new Properties();
        DipPropertyQueryExecutor dipPropsQueryExecutor = new DipPropertyQueryExecutor(jdbcService);
        try {
            List<DipProperty> dipPropertyList = dipPropsQueryExecutor.getList(DipPropertyQueryExecutor.DipPropertyQuery.GET_DIPPROPERTY_ALL, new Object[]{});
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

        initServer();

        try {
            jettyServer.start();
            LOG.info("DipServer Started !! ");
            jettyServer.join();
        } catch (Exception e) {
            LOG.error("Error starting Jetty. DipServer may not be available.", e);
        }

    }

    private void initServer() {

        jettyServer = new Server(PORT);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        jettyServer.setHandler(contexts);

        Context root = new Context(contexts, "/dip", Context.SESSIONS);
        ServletHolder jerseyServlet = new ServletHolder(ServletContainer.class);
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter("com.sun.jersey.config.property.packages", "com.nexr.dip.rest");

        root.addServlet(jerseyServlet, "/*");
        root.setAttribute("scheduledService", scheduledService);
    }

    public void shutdown() throws DipException {

        try {

            shutdownService();

            jettyServer.stop();
            jettyServer.join();
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
