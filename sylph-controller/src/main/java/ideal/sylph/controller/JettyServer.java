package ideal.sylph.controller;

import ideal.sylph.controller.selvet.WebAppProxyServlet;
import ideal.sylph.spi.SylphContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.MultipartConfigElement;

import java.io.File;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Created by ideal on 17-3-15.
 */
@Deprecated
public final class JettyServer
{
    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private Server server;
    private final ServerConfig serverConfig;
    private final SylphContext sylphContext;

    JettyServer(
            ServerConfig serverConfig,
            SylphContext sylphContext
    )
    {   //ServerConfig 类 设置 了 serverConfig的相关属性
        this.serverConfig = requireNonNull(serverConfig, "serverConfig is null");
        this.sylphContext = requireNonNull(sylphContext, "sylphContext is null");
    }

    public void start()
            throws Exception
    {
        //-------初始化------获取Context句柄------
        int jettyPort = serverConfig.getServerPort();
        int maxFormContentSize = serverConfig.getMaxFormContentSize();

        // 创建Server
        this.server = new Server(jettyPort);
        server.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize",
                maxFormContentSize);

        HandlerList handlers = loadHandlers();  //加载路由
        server.setHandler(handlers);
        logger.info("web Server started... the port {}", jettyPort);
        server.start();
    }

    private HandlerList loadHandlers()
    {
        HandlerList handlers = new HandlerList();
        ServletHolder servlet = new ServletHolder(new ServletContainer(new WebApplication()));
        servlet.getRegistration().setMultipartConfig(new MultipartConfigElement("data/tmp", 1048576, 1048576, 262144));

        //--------------------全局的----------------------
        //selvet
        ServletContextHandler contextHandler = new ServletContextHandler(
                ServletContextHandler.NO_SESSIONS);
        contextHandler.setContextPath("/");
        contextHandler.setAttribute("sylphContext", sylphContext);

        //-------add jersey--------
        contextHandler.addServlet(servlet, "/_sys/*");
        //contextHandler.addServlet(JobMangerSerlvet.class, "/_sys/job_manger/*");
        //contextHandler.addServlet(new SylphServletHolder(new ETLJobServlet()), "/_sys/job_graph_edit/*");
        contextHandler.addServlet(WebAppProxyServlet.class, "/proxy/*");

        final ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
        contextHandler.addServlet(staticServlet, "/css/*");
        contextHandler.addServlet(staticServlet, "/js/*");
        contextHandler.addServlet(staticServlet, "/images/*");
        contextHandler.addServlet(staticServlet, "/fonts/*");
        contextHandler.addServlet(staticServlet, "/favicon.ico");
        contextHandler.addServlet(staticServlet, "/");
        contextHandler.setResourceBase("webapps");

        //-----------------加载所有app的-路由------------------
        //runnerLoader.loadAppHandlers(handlers, contextHandler);

        //----selvet--结束--
        String mainHome = System.getProperty("user.dir");
        File[] webs = new File(mainHome + "/webapps").listFiles();
        if (webs != null) {
            Arrays.stream(webs).forEach(file -> {
                if (file.isDirectory()) {
                    WebAppContext webapp = new WebAppContext();
                    webapp.setContextPath("/_web/" + file.getName().toLowerCase()); //_web/postman
                    webapp.setResourceBase(file.getAbsolutePath());
                    handlers.addHandler(webapp);
                }
            });
        }
        handlers.addHandler(contextHandler);
        return handlers;
    }
}
