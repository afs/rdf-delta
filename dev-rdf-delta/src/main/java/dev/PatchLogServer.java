package dev;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.jena.atlas.web.HttpException;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client-side representative of a patch log server.
 * This is a persistent representative for the remote log server.
 * (However, this does not mean the remote server is available.)  
 * All interaction with the remote server MUST go through this class.
 */

/*package*/ class PatchLogServer {
    
    static private final Logger LOG = LoggerFactory.getLogger(PatchLogServer.class) ;
    private final String serverURL;
    private final AtomicReference<DeltaLink> deltaLink = new AtomicReference<>(null);

    /*package*/ PatchLogServer(String serverURL) {
        this.serverURL = Objects.requireNonNull(serverURL, "Server URL is null");
    }
    
    /** Return the server URL. */
    public String getServerURL() { return serverURL; }
    
//    // In case late initialization 
//    private /*package*/ void setServerURL(String serverURL) {
//        if ( ! Objects.equals(this.serverURL, serverURL) )
//            // No change.
//            return;
//        if ( this.serverURL == null )
//            LOG.info("Setting server URL to "+serverURL);
//        else
//            LOG.info("Changing server URL "+this.serverURL+" -> "+serverURL) ;
//        
//        // If a chnage, drop previous DeltaLink.
//        if ( deltaLink.get() != null ) {
//            deltaLink.get().close();
//        }
//        this.serverURL = serverURL;
//    }
    
    /*package*/ DeltaLink getDLink() {
        connectedEx();
        return deltaLink.get();
    }

    /** Are we connected (maybe)? */
    /*package*/ boolean connected() {
        return deltaLink.get() != null;
    }
    
    /*package*/ List<DataSourceDescription> dataSources() { 
        return action(dLink->dLink.listDescriptions());
    }

    static class HttpNotConnectedException extends HttpException {

        public HttpNotConnectedException(String message) {
            // Must be n<0 to distinguish from HTTP response codes. 
            super(-404, "Not Connected", message);
        }
        
//        @Override
//        public Throwable fillInStackTrace() { return this; }
    }
    
    /** Are we connected (maybe)? */
    /*package*/ void connectedEx() {
        if ( ! connected() )
            throw new HttpNotConnectedException("Not connected to "+serverURL);
    }
    
    /** Ensure a connection has been established, return true for OK and false for bad conenction. */
    /*package*/ boolean ensureConnected() {
        if ( ! connected() )
            setupDLink();
        return connected();
    }
    
    /** Ensure a connection has been established, return for OK, throw {@link HttpNotConnectedException} for not. */
    /*package*/ void ensureConnectedEx() {
        if ( ! ensureConnected() )
            throw new HttpNotConnectedException("Can not establish connection to "+serverURL);
    }
    
    /** Actively ping the server */
    /*package*/ boolean ping() {
        try {
            getDLink().ping();
            return true ;
        } catch (HttpException ex) { return false ; }
    }
    
    /** Actively ping the server, throw an exception if there are problems */
    /*package*/ void pingEx() {
        try {
            getDLink().ping();
        } catch (HttpException ex) { throw new HttpNotConnectedException("Can not ping the server: "+serverURL); }
    }

    /** Run an action that contacts the patch log server.
     * Return true if the action succeeded. 
     * @param action
     * @return
     */
    /*package*/ boolean exec(Consumer<DeltaLink> action) {
        if ( !connected() )
            return false;
        try {
            action.accept(getDLink());
            return true;
        } catch (HttpException ex) {
            // Registered?
            handleHttpException(ex);
            return false;
        }
    }
    
    /*package*/ <X> X action(Function<DeltaLink, X> action) {
        if ( !connected() )
            return null;
        try {
            return action.apply(getDLink());
        } catch (HttpException ex) {
            handleHttpException(ex);
            return null;
        }
    }
    
    private void handleHttpException(HttpException ex) {
        if ( isConnectionProblem(ex) ) {
            LOG.info("Failed to connect to "+serverURL+" : "+ex.getMessage());
        } else {
            LOG.info("Unrecognized patchlog action problem: "+ex.getMessage(), ex);
        }
        // Broken.
        deltaLink.set(null);
    }

    /*package*/ private boolean isConnectionProblem(HttpException ex) {
        if ( ex instanceof HttpNotConnectedException ) 
            return true;
        
        if ( ex.getResponseCode() > 0 )
            // Some HTTP status code -> network worked   
            return false;
        if ( ex.getCause() instanceof ConnectException /*IOException*/ )
            return true;
        if ( ex.getCause() instanceof IOException )
            return true;
        LOG.warn("Unrecognized exception: "+ex.getMessage(), ex);
        return true;
        // Unknown.
    }
    
    /** connect to the server and register. */
    synchronized private void setupDLink() {
        // Check again - now inside synchronized.
        if ( deltaLink.get() != null )
            return ;
        
        DeltaLink link = DeltaLinkHTTP.connect(serverURL);
        // Ping server.
        Id clientId = Id.create();
        // Network traffic.
        
        try {
            // Try once to see if the server is present and live.
            link.ping();
        } catch (HttpException ex) {
            handleHttpException(ex);
            return ;
        }
        
        try {
            link.register(clientId);
        } catch (HttpException ex) {
            // Connection.
            handleHttpException(ex);
            return ;
        }
        // We count as "connected" now
        deltaLink.set(link);
    }
}
