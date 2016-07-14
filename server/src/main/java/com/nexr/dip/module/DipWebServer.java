package com.nexr.dip.module;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.nexr.dip.AppService;
import com.nexr.dip.Context;
import com.nexr.dip.DipException;
import com.nexr.dip.jpa.JDBCService;
import com.nexr.dip.jpa.LoadResultQueryExecutor;
import com.nexr.dip.loader.ScheduledService;
import com.nexr.dip.rest.Topic;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;

public class DipWebServer implements AppService {

    public static int DEFAULT_PORT = 17171;
    private static Logger LOG = LoggerFactory.getLogger(DipWebServer.class);
    private Server jettyServer;

    @Inject
    private Context context;

    private Injector baseInjector;

    public DipWebServer() {

    }

    @Inject
    public void configureInjector(Injector injector) {
        this.baseInjector = injector;
    }

    @Override
    public void start() throws DipException {
        jettyServer = new Server(context.getInt("dip.port", DEFAULT_PORT));

        // Create a servlet context and add the jersey servlet.
        ServletContextHandler sch = new ServletContextHandler(jettyServer, "/");

        // Add our Guice listener that includes our bindings
        sch.addEventListener(new GuiceServletConfig(baseInjector));

        // Then add GuiceFilter and configure the server to
        // reroute all requests through this filter.
        sch.addFilter(GuiceFilter.class, "/*", null);

        // Must add DefaultServlet for embedded Jetty.
        // Failing to do this will cause 404 errors.
        // This is not needed if web.xml is used instead.
        sch.addServlet(DefaultServlet.class, "/");

        //root.setAttribute("scheduledService", scheduledService);

        try {
            // Start the server
            jettyServer.start();
        } catch (Exception e) {
            throw new DipException(e);
        }
    }

    @Override
    public void shutdown() throws DipException {
        try {
            jettyServer.stop();
        } catch (Exception e) {
            throw new DipException(e);
        }
    }

    public void join() throws DipException {
        if (jettyServer == null) {
            throw new DipException("jettyServer is null");
        }
        try {
            jettyServer.join();
        } catch (InterruptedException e) {
            throw new DipException(e);
        }
    }

    private static class GuiceServletConfig extends GuiceServletContextListener {

        private final Injector bInjector;

        GuiceServletConfig(Injector injector) {
            this.bInjector = injector;
        }

        @Override
        public void contextInitialized(ServletContextEvent servletContextEvent) {
            super.contextInitialized(servletContextEvent);
        }

        @Override
        protected Injector getInjector() {
            return Guice.createInjector(new JerseyServletModule() {
                @Override
                protected void configureServlets() {
                    bind(Topic.class);
                    serve("/*").with(GuiceContainer.class);
                }

                @Provides
                JDBCService provideJDBCService() {
                    JDBCService jdbcService = bInjector.getInstance(JDBCService.class);
                    LOG.info("---- GuiceServletConfig baseInjector : JDBCService : " + jdbcService);
                    return jdbcService;
                }

                @Provides
                LoadResultQueryExecutor provideLoadResultQueryExecutor() {
                    LoadResultQueryExecutor queryExceutor = bInjector.getInstance(LoadResultQueryExecutor.class);
                    LOG.info("---- GuiceServletConfig baseInjector : queryExceutor : " + queryExceutor);
                    return queryExceutor;
                }

                @Provides
                ScheduledService provideScheduledService() {
                    return bInjector.getInstance(ScheduledService.class);
                }
            });
        }
    }
}
