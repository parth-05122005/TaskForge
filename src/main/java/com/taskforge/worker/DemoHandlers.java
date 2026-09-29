package com.taskforge.worker;

import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DemoHandlers {
    private final Map<String,JobHandler> handlers;

    public DemoHandlers(List<JobHandler> handlers) {
        Map<String,JobHandler> byType=new HashMap<>();
        for(JobHandler handler:handlers) {
            if(byType.putIfAbsent(handler.type(),handler)!=null)
                throw new IllegalStateException("More than one handler registered for job type: "+handler.type());
        }
        this.handlers=Map.copyOf(byType);
    }

    public JobHandler get(String type) {
        JobHandler handler=handlers.get(type);
        if(handler==null)throw new IllegalArgumentException("Unsupported job type: "+type);
        return handler;
    }
}
