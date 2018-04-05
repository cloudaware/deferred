# About

Cloud Tasks compatible implementation of [Deferred Tasks](https://cloud.google.com/appengine/docs/standard/java/taskqueue/push/creating-tasks#using_the_deferred_instead_of_a_worker_service) for java

Core changes:

* `DeferredTaskContext` require `HttpServletRequest` and uses `HttpServletRequest.attributes`
* Queue implementation changed from AppEngine `QueueFactory` to Cloud Tasks

# Usage

Bind `DeferredTaskServlet` to `DeferredTaskContext.DEFAULT_DEFERRED_URL` (`/_ah/queue/__deferred__`)

# Build

`mvn clean package`

# Changelog

* 1.0.5 - Actualize README, minor fix JavaDoc
* 1.0.4 - Fix bug related to dateTimeFormat 
* 1.0.3 - Actualize code according to new API of Cloud Tasks
* 1.0.2 - Use `google-api-services-cloudtasks` from maven central repo 
* 1.0.1 - Fallback to no credentials if can't create application default credentials
* 1.0.0 - Initial release