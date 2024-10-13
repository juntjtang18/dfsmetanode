package com.fdu.msacs.dfs.metanode;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@Component
public class RestControllerLogger implements ApplicationListener<ApplicationReadyEvent> {

    private final RequestMappingHandlerMapping handlerMapping;

    public RestControllerLogger(RequestMappingHandlerMapping handlerMapping) {
        this.handlerMapping = handlerMapping;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        handlerMapping.getHandlerMethods().forEach((mappingInfo, handlerMethod) -> {
            System.out.println("Mapping: " + mappingInfo + " -> " + handlerMethod);
        });
    }
}
