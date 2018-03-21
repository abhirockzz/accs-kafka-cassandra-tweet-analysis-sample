package com.oracle.cloud.accs.tweets.producer.lcm;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("tweets/producer")
public class ProducerManagerResource {

    /**
     * start the Tweets Producer service
     * @return 200 OK for success, 500 in case of issues
     */
    @GET
    public Response start() {
        Response r = null;
        try {
            ProducerLifecycleManager.getInstance().start();
            r = Response.ok("Tweets Producer started")
                .build();
        } catch (Exception ex) {
            Logger.getLogger(ProducerManagerResource.class.getName()).log(Level.SEVERE, null, ex);
            r = Response.serverError().build();
        }
        return r;
    }
    
    /**
     * stop Tweets producer
     * @return 200 OK for success, 500 in case of issues
     */
    @DELETE
    public Response stop() {
        Response r = null;
        try {
            ProducerLifecycleManager.getInstance().stop();
            r = Response.ok("Tweets Producer stopped")
                .build();
        } catch (Exception ex) {
            Logger.getLogger(ProducerManagerResource.class.getName()).log(Level.SEVERE, null, ex);
            r = Response.serverError().build();
        }
        return r;
    }

}
