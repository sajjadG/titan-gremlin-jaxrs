package io.github.sajjadg.java.jaxrs;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;


@Provider
@Priority(Priorities.HEADER_DECORATOR)
public class CORSFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext creq, ContainerResponseContext cresp) {

//        Logger.getLogger(CORSFilter.class.getName()).log(Level.INFO, "before: {0}", creq.getHeaders());

        // *(allow from all servers) OR http://crunchify.com/ OR http://example.com/
        cresp.getHeaders().add("Access-Control-Allow-Origin", "*");
        cresp.getHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization, token");
        cresp.getHeaders().add("Access-Control-Allow-Credentials", "true");
        // As a part of the response to a request, which HTTP methods can be used during the actual request.
        cresp.getHeaders().add("Access-Control-Allow-Methods", "GET, POST");
        // How long the results of a request can be cached in a result cache.
        cresp.getHeaders().add("Access-Control-Max-Age", "1209600");

//        Logger.getLogger(CORSFilter.class.getName()).log(Level.INFO, "after: {0}", cresp.getHeaders());
    }
}
