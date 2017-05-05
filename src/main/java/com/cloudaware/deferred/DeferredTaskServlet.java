package com.cloudaware.deferred;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;

/**
 * Replacement DefferdTaskServlet for flex and guice
 * in WarModule must be: <br/>
 * {@code bind(DeferredTaskServlet.class).in(Singleton.class);}<br/>
 * {@code serve(DeferredTaskContext.DEFAULT_DEFERRED_URL).with(DeferredTaskServlet.class);}<br/>
 */

public final class DeferredTaskServlet extends HttpServlet {

    private static final int SC_403 = 403;
    private static final int SC_405 = 405;
    private static final int SC_400 = 400;
    private static final int SC_500 = 500;
    private static final int SC_200 = 200;
    private static final int SC_415 = 415;
    private static final int SC_203 = 203;

    protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        if (req.getHeader("X-AppEngine-QueueName") == null) {
            resp.sendError(SC_403, "Not a taskqueue request.");
        } else {
            final String method = req.getMethod();
            if (!"POST".equals(method)) {
                final String msg;
                if (method.length() != 0) {
                    msg = "DeferredTaskServlet does not support method: ".concat(method);
                } else {
                    msg = "DeferredTaskServlet does not support method: ";
                }
                final String protocol = req.getProtocol();
                if (protocol.endsWith("1.1")) {
                    resp.sendError(SC_405, msg);
                } else {
                    resp.sendError(SC_400, msg);
                }

            } else {
                req.setAttribute(DeferredTaskContext.DEFERRED_TASK_SERVLET_KEY, this);
                req.setAttribute(DeferredTaskContext.DEFERRED_TASK_REQUEST_KEY, req);
                req.setAttribute(DeferredTaskContext.DEFERRED_TASK_RESPONSE_KEY, resp);
                req.setAttribute(DeferredTaskContext.DEFERRED_MARK_RETRY_KEY, Boolean.valueOf(false));

                try {
                    this.performRequest(req, resp);
                    if (((Boolean) req.getAttribute(DeferredTaskContext.DEFERRED_MARK_RETRY_KEY)).booleanValue()) {
                        resp.setStatus(SC_500);
                    } else {
                        resp.setStatus(SC_200);
                    }

                    return;
                } catch (DeferredTaskException e) {
                    resp.setStatus(SC_415);
                    this.log(new StringBuilder().append("Deferred task failed exception: ").append(e).toString());
                } catch (RuntimeException re) {
                    final Boolean doNotRetry = (Boolean) req.getAttribute(DeferredTaskContext.DEFERRED_DO_NOT_RETRY_KEY);
                    if (doNotRetry != null && doNotRetry.booleanValue()) {
                        if (doNotRetry.booleanValue()) {
                            resp.setStatus(SC_203);
                            this.log(
                                    new StringBuilder()
                                            .append(DeferredTaskServlet.class.getName())
                                            .append(" - Deferred task failed but doNotRetry specified. Exception: ")
                                            .append(re).toString()
                            );
                        }

                        return;
                    }

                    throw new ServletException(re);
                } finally {
                    req.removeAttribute(DeferredTaskContext.DEFERRED_TASK_SERVLET_KEY);
                    req.removeAttribute(DeferredTaskContext.DEFERRED_TASK_REQUEST_KEY);
                    req.removeAttribute(DeferredTaskContext.DEFERRED_TASK_RESPONSE_KEY);
                    req.removeAttribute(DeferredTaskContext.DEFERRED_DO_NOT_RETRY_KEY);
                }

            }
        }
    }

    protected void performRequest(final HttpServletRequest req, final HttpServletResponse resp) throws DeferredTaskException {
        this.readRequest(req, resp).run(req);
    }

    protected DeferredTask readRequest(final HttpServletRequest req, final HttpServletResponse resp) throws DeferredTaskException {
        final String contentType = req.getHeader("content-type");
        if (contentType != null && DeferredTaskContext.RUNNABLE_TASK_CONTENT_TYPE.equals(contentType)) {
            try {
                final ObjectInputStream objectStream = new ObjectInputStream(req.getInputStream()) {
                    protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                        final String name = desc.getName();

                        try {
                            return Class.forName(name, false, classLoader);
                        } catch (ClassNotFoundException e) {
                            return super.resolveClass(desc);
                        }
                    }

                    protected Class<?> resolveProxyClass(final String[] interfaces) throws IOException, ClassNotFoundException {
                        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                        ClassLoader nonPublicLoader = null;
                        boolean hasNonPublicInterface = false;
                        final Class[] classObjs = new Class[interfaces.length];

                        for (int i = 0; i < interfaces.length; ++i) {
                            final Class cl = Class.forName(interfaces[i], false, classLoader);
                            if ((cl.getModifiers() & 1) == 0) {
                                if (hasNonPublicInterface) {
                                    if (nonPublicLoader != cl.getClassLoader()) {
                                        throw new IllegalAccessError("conflicting non-public interface class loaders");
                                    }
                                } else {
                                    nonPublicLoader = cl.getClassLoader();
                                    hasNonPublicInterface = true;
                                }
                            }

                            classObjs[i] = cl;
                        }

                        try {
                            return Proxy.getProxyClass(hasNonPublicInterface ? nonPublicLoader : classLoader, classObjs);
                        } catch (IllegalArgumentException e) {
                            throw new ClassNotFoundException((String) null, e);
                        }
                    }
                };
                final DeferredTask deferredTask = (DeferredTask) objectStream.readObject();
                return deferredTask;
            } catch (ClassNotFoundException e) {
                throw new DeferredTaskException(e);
            } catch (IOException e) {
                throw new DeferredTaskException(e);
            } catch (ClassCastException e) {
                throw new DeferredTaskException(e);
            }
        } else {
            throw new DeferredTaskException(new IllegalArgumentException(
                    new StringBuilder()
                            .append("Invalid content-type header. received: '")
                            .append(contentType == null ? "null" : contentType)
                            .append("' expected: '")
                            .append("application/x-binary-app-engine-java-runnable-task")
                            .append("'").toString()
            ));
        }
    }

    protected static class DeferredTaskException extends Exception {
        public DeferredTaskException(final Exception e) {
            super(e);
        }
    }

}
