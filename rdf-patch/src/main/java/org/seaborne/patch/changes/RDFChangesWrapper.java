package org.seaborne.patch.changes;

import org.apache.jena.graph.Node ;
import org.seaborne.patch.RDFChanges ;

/** Wrapper for {@link RDFChanges} */
public class RDFChangesWrapper implements RDFChanges {

    private RDFChanges other ;
    protected RDFChanges get() { return other ; }
    
    public RDFChangesWrapper(RDFChanges other) {
        this.other = other ;
    }
    
    @Override
    public void start() {
        get().start();
    }

    @Override
    public void finish() {
        get().finish();
    }

    @Override
    public void header(String field, Node value) {
        get().header(field, value);
    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        get().add(g, s, p, o);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        get().delete(g, s, p, o);
    }

    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        get().addPrefix(gn, prefix, uriStr);
    }

    @Override
    public void deletePrefix(Node gn, String prefix) {
        get().deletePrefix(gn, prefix);
    }

    @Override
    public void txnBegin() {
        get().txnBegin();
    }

    @Override
    public void txnCommit() {
        get().txnCommit();
    }

    @Override
    public void txnAbort() {
        get().txnAbort();
    }
}
