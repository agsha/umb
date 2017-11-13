package umb.client;

import com.google.common.collect.ImmutableList;
import org.apache.http.client.fluent.Request;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import umb.common.EmbeddedJetty;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Admin {
    private static final Logger log = LoggerFactory.getLogger(UmbClient.class);
    Server server;
    private void start() throws Exception {
        EmbeddedJetty.P embeddedJettyP = new EmbeddedJetty.P();
        server = embeddedJettyP.get(8080, ImmutableList.of(new EmbeddedJetty.ServletInfo(new HelloServlet(this), "/hello")));
        server.start();
    }

    private void stop() throws Exception {
        server.stop();
    }
    public static class HelloServlet extends HttpServlet {
        private Admin admin;
        public HelloServlet(Admin admin) {
            this.admin = admin;
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
            String method = req.getParameter("method");
            switch (method) {
                case "echo":
                    String word = req.getParameter("word");
                    try {
                        resp.getWriter().write(admin.echo(word));
                    } catch (IOException e) {
                        StateMachine.obj.go(StateMachine.Event.a1, e);
                    }
                    break;
            }
        }
    }

    private String echo(String word) {
        return "Hello, "+word+"!\n";
    }


    /**
     * Methods mainly used for testing
     */
    public static void main(String[] args) throws Exception {

        Admin admin = new Admin();
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                admin.start();
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        latch.await();

        String content = Request.Get("http://localhost:8080/hello?method=echo&word=world")
                .execute().returnContent().asString();
        log.debug(content);
        admin.stop();
    }
}
