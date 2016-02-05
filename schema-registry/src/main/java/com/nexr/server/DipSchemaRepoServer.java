package com.nexr.server;

import com.nexr.dip.AppService;
import com.nexr.dip.DipException;
import com.nexr.dip.jpa.JDBCService;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DipSchemaRepoServer implements AppService {

    public static int DEFAULT_PORT = 18181;
    private static Logger LOG = LoggerFactory.getLogger(DipSchemaRepoServer.class);
    private static DipSchemaRepoServer schemaRepoServer;
    public int PORT = DEFAULT_PORT;
    private Server jettyServer;

    private JDBCService jdbcService;

    private DipSchemaRepoServer() {

    }

    public static DipSchemaRepoServer getInstance() {
        if (schemaRepoServer == null) {
            schemaRepoServer = new DipSchemaRepoServer();
            schemaRepoServer.init();
        }
        return schemaRepoServer;
    }

    public static void main(String[] args) {
        String cmd = args[0];
        System.out.println("command : " + cmd);

        if ("start".equals(cmd)) {
            AppService app = DipSchemaRepoServer.getInstance();
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
            initServices();
        } catch (DipException e) {
            LOG.error("Fail to init services ", e);
        }
    }

    private void initContext() {
        String sPort = DipSchemaRepoContext.getContext().getConfig("schemarepo.port");
        PORT = sPort == null ? DEFAULT_PORT : Integer.parseInt(sPort);
    }

    private void initServices() throws DipException {
        jdbcService = JDBCService.getInstance("schemarepo", "repo-master-mysql");
        jdbcService.start();
    }

    public void start() throws DipException {
        LOG.info("========= Avro Repo Starting ......   ========");

        init();

        initServer();

        try {
            jettyServer.start();
            LOG.info("Avro Repo Started !! ");
            jettyServer.join();
        } catch (Exception e) {
            LOG.error("Error starting Jetty. Avro Repo may not be available.", e);
        }

    }

    private void initServer() {

        jettyServer = new Server(PORT);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        jettyServer.setHandler(contexts);

        Context root = new Context(contexts, "/repo", Context.SESSIONS);
        ServletHolder jerseyServlet = new ServletHolder(ServletContainer.class);
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter("com.sun.jersey.config.property.packages", "com.nexr.rest");

        root.addServlet(jerseyServlet, "/*");
    }

    public void shutdown() throws DipException {

        try {
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception ex) {
            LOG.error("Error stopping Jetty. Avro Repo may not be available.", ex);
        }
        LOG.info("========= Avro Repo Shutdown ======== \n");

    }

    public JDBCService getJdbcService() {
        return jdbcService;
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
