package com.cloudaware.deferred;

import jakarta.servlet.http.HttpServletRequest;
import java.io.Serializable;

public interface DeferredTask extends Serializable {
    void run(HttpServletRequest request);
}
