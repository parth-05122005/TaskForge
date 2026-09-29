package com.taskforge.worker;
import com.taskforge.job.JobMessage;import org.springframework.stereotype.Component;import java.util.*;
@Component public class DemoHandlers {
 private final Map<String,JobHandler> handlers;
 public DemoHandlers(){List<JobHandler> all=List.of(new Basic("REPORT",false),new Basic("REPORT_GENERATION",false),new Basic("EMAIL_NOTIFICATION",false),new Basic("DATA_PROCESSING",false),new Basic("HTTP_REQUEST",false),new Basic("DEMO_LONG_RUNNING_TASK",false),new Basic("DEMO_FAIL",true));Map<String,JobHandler> m=new HashMap<>();all.forEach(h->m.put(h.type(),h));handlers=Map.copyOf(m);}
 public JobHandler get(String type){JobHandler h=handlers.get(type);if(h==null)throw new IllegalArgumentException("Unsupported job type: "+type);return h;}
 private record Basic(String type,boolean fail) implements JobHandler {public void execute(JobMessage j)throws Exception{if(type.equals("DEMO_LONG_RUNNING_TASK"))Thread.sleep(1000);if(fail)throw new IllegalStateException("Requested demo transient failure");}}
}
