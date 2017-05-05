package com.cloudaware.deferred;

public class DeferredTaskCreationException extends RuntimeException {
    private static final long serialVersionUID = -1434044266930221L;

    public DeferredTaskCreationException(final Throwable e) {
        super(e);
    }
}
