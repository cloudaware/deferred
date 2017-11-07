package com.cloudaware.deferred;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudtasks.v2beta2.CloudTasks;
import com.google.api.services.cloudtasks.v2beta2.CloudTasksScopes;
import com.google.api.services.cloudtasks.v2beta2.model.AppEngineHttpRequest;
import com.google.api.services.cloudtasks.v2beta2.model.AppEngineRouting;
import com.google.api.services.cloudtasks.v2beta2.model.CreateTaskRequest;
import com.google.api.services.cloudtasks.v2beta2.model.Task;
import com.google.common.collect.Maps;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

public final class DeferredTaskContext {
    public static final String RUNNABLE_TASK_CONTENT_TYPE = "application/x-binary-app-engine-java-runnable-task";
    public static final String DEFAULT_DEFERRED_URL = "/_ah/queue/__deferred__";
    public static final String DEFERRED_TASK_SERVLET_KEY = DeferredTaskContext.class.getName().concat(".httpServlet");
    public static final String DEFERRED_TASK_REQUEST_KEY = DeferredTaskContext.class.getName().concat(".httpServletRequest");
    public static final String DEFERRED_TASK_RESPONSE_KEY = DeferredTaskContext.class.getName().concat(".httpServletResponse");
    public static final String DEFERRED_DO_NOT_RETRY_KEY = DeferredTaskContext.class.getName().concat(".doNotRetry");
    public static final String DEFERRED_MARK_RETRY_KEY = DeferredTaskContext.class.getName().concat(".markRetry");
    private static final DateFormat ISO_DATE_FORMAT;

    private static final String CLOUDTASKS_API_ROOT_URL_PROPERTY = "cloudtasks.api.root.url";
    private static final String CLOUDTASKS_API_KEY_PROPERTY = "cloudtasks.api.key";
    private static final String CLOUDTASKS_API_DEFAULT_PARENT = "cloudtasks.api.default.parent";

    static {
        ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private DeferredTaskContext() {
    }

    public static HttpServlet getCurrentServlet(final HttpServletRequest request) {
        return (HttpServlet) request.getAttribute(DEFERRED_TASK_SERVLET_KEY);
    }

    public static HttpServletRequest getCurrentRequest(final HttpServletRequest request) {
        return (HttpServletRequest) request.getAttribute(DEFERRED_TASK_REQUEST_KEY);
    }

    public static HttpServletResponse getCurrentResponse(final HttpServletRequest request) {
        return (HttpServletResponse) request.getAttribute(DEFERRED_TASK_RESPONSE_KEY);
    }

    public static void setDoNotRetry(final HttpServletRequest request, final boolean value) {
        request.setAttribute(DEFERRED_DO_NOT_RETRY_KEY, Boolean.valueOf(value));
    }

    public static void markForRetry(final HttpServletRequest request) {
        request.setAttribute(DEFERRED_MARK_RETRY_KEY, Boolean.valueOf(true));
    }

    public static Task enqueueDeferred(final DeferredTask deferredTask, final Task taskArg, final String queueName) {
        final String rootUrl = System.getProperty(CLOUDTASKS_API_ROOT_URL_PROPERTY);
        String apiKey = System.getProperty(CLOUDTASKS_API_KEY_PROPERTY);
        if (apiKey == null) {
            apiKey = "";
        }
        try {
            HttpRequestInitializer credential;
            try {
                credential = GoogleCredential.getApplicationDefault().createScoped(CloudTasksScopes.all());
            } catch (IOException e) {
                //Fallback to HttpRequestInitializer, cannot create application default credentials
                credential = new HttpRequestInitializer() {
                    @Override
                    public void initialize(final HttpRequest request) throws IOException {
                    }
                };
            }
            final CloudTasks.Builder builder = new CloudTasks.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    JacksonFactory.getDefaultInstance(),
                    credential
            );
            if (rootUrl != null) {
                builder.setRootUrl(rootUrl);
            }
            final CloudTasks cloudTasks = builder.build();
            final Task task = taskArg == null ? new Task() : taskArg;

            final Map<String, String> headers =
                    task.getAppEngineHttpRequest() == null
                            || task.getAppEngineHttpRequest().getHeaders() == null
                            ? Maps.<String, String>newHashMap()
                            : task.getAppEngineHttpRequest().getHeaders();
            //set values if not exist
            headers.put("content-type", RUNNABLE_TASK_CONTENT_TYPE);
            if (task.getAppEngineHttpRequest() == null) {
                task.setAppEngineHttpRequest(new AppEngineHttpRequest());
            }
            task.getAppEngineHttpRequest().setHeaders(headers);
            task.getAppEngineHttpRequest().setRelativeUrl(DEFAULT_DEFERRED_URL);
            task.getAppEngineHttpRequest().setHttpMethod("POST");
            task.getAppEngineHttpRequest().setPayload(com.google.api.client.util.Base64.encodeBase64String(convertDeferredTaskToPayload(deferredTask)));
            /**
             * Cloud Task Routing Start
             */
            if (task.getAppEngineHttpRequest().getAppEngineRouting() == null) {
                task.getAppEngineHttpRequest().setAppEngineRouting(new AppEngineRouting());
            }
            String version = System.getenv("GAE_VERSION");
            version = version == null ? "default" : version;
            String service = System.getenv("GAE_SERVICE");
            service = service == null ? "default" : service;
            String backend = null;
            if (task.getAppEngineHttpRequest().getHeaders() != null) {
                backend = task.getAppEngineHttpRequest().getHeaders().get("Host");
            }
            //try to extract version and service from backend if on prod env
            if (backend != null && backend.contains(".")) {
                final String[] parts = backend.split("\\.");
                //sample "default.default.appId.apphost.com"
                if (parts.length == 5) {
                    version = parts[0];
                    service = parts[1];
                } else if (parts.length == 6) { //sample "1.default.default.appId.apphost.com"
                    version = parts[1];
                    service = parts[2];
                }
            }
            if (task.getAppEngineHttpRequest().getAppEngineRouting().getService() == null) {
                task.getAppEngineHttpRequest().getAppEngineRouting().setService(service);
            }
            if (task.getAppEngineHttpRequest().getAppEngineRouting().getVersion() == null) {
                task.getAppEngineHttpRequest().getAppEngineRouting().setVersion(version);
            }
            /**
             * Cloud Task Routing End
             */

            final String queue;
            if (!queueName.startsWith("projects/")) {
                queue = System.getProperty(CLOUDTASKS_API_DEFAULT_PARENT) + "/queues/" + (queueName == null ? "default" : queueName);
            } else {
                queue = queueName;
            }
            return cloudTasks.projects().locations().queues().tasks().create(
                    queue,
                    new CreateTaskRequest().setTask(task)
            ).setKey(apiKey).execute();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] convertDeferredTaskToPayload(final DeferredTask deferredTask) {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream(1024);
        try {
            final ObjectOutputStream objectStream = new ObjectOutputStream(stream);
            objectStream.writeObject(deferredTask);
        } catch (IOException e) {
            throw new DeferredTaskCreationException(e);
        }
        return stream.toByteArray();
    }

    /**
     * Add seconds to Now() and serialize it to ISO 8601
     *
     * @param seconds
     * @return date in ISO 8601
     */
    public static String getScheduleTime(final int seconds) {
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, seconds);
        return ISO_DATE_FORMAT.format(calendar.getTime());
    }
}
