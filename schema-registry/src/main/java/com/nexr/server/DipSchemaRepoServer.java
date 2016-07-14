package com.nexr.server;

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
import com.nexr.dip.jpa.JDBCService;
import com.nexr.jpa.SchemaInfoQueryExceutor;
import com.nexr.module.JettyWebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DipSchemaRepoServer extends AbstractModule implements AppService {

    private static Logger LOG = LoggerFactory.getLogger(DipSchemaRepoServer.class);

    @Inject
    private Context context;

    @Inject
    private JDBCService jdbcService;

    @Inject
    private JettyWebServer jettyWebServer;


    public DipSchemaRepoServer() {

    }

    public static void main(String[] args) {
        String cmd = args[0];

        if ("start".equals(cmd)) {

            Injector injector = Guice.createInjector(ImmutableList.of(new DipSchemaRepoServer()));
            DipSchemaRepoServer app = injector.getInstance(DipSchemaRepoServer.class);

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
        String persistUnit = System.getProperty("persistenceUnit") == null ? "repo-master-mysql" : System.getProperty("persistenceUnit");
        bindConstant().annotatedWith(Names.named("persistenceUnit")).to(persistUnit);
        bindConstant().annotatedWith(Names.named("persistenceName")).to("schemarepo");
        bindConstant().annotatedWith(Names.named("siteConfig")).to("schemarepo.conf");
        bindConstant().annotatedWith(Names.named("defaultConfig")).to("schemarepo-default.conf");
        bind(Context.class).in(Singleton.class);
        bind(JDBCService.class).in(Singleton.class);
        bind(JettyWebServer.class).in(Singleton.class);
        bind(SchemaInfoQueryExceutor.class).in(Singleton.class);
    }

    public void start() throws DipException {
        LOG.info("========= Schema Repo Starting ......   ========");

        try {
            jettyWebServer.start();
            LOG.info("Schema Repo Started !! ");
            jettyWebServer.join();

        } catch (Exception e) {
            LOG.error("Error starting SchemaRepoServer. It may not be available.", e);
        }

    }

    public void shutdownServices() {
        if (jdbcService != null) {
            jdbcService.shutdown();
            jdbcService = null;
        }
    }

    public void shutdown() throws DipException {

        shutdownServices();
        try {
            jettyWebServer.shutdown();
        } catch (Exception ex) {
            LOG.error("Error stopping Jetty. Schema Repo may not be available.", ex);
        }
        LOG.info("========= Schema Repo Shutdown ======== \n");

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
