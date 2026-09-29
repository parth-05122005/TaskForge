package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.job.JobMessage;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DemoHandlersTest {
    private final ObjectMapper mapper=new ObjectMapper();
    private final DemoHandlers handlers=new DemoHandlers(List.of(
            new ReportJobHandler(mapper),new ReportGenerationJobHandler(mapper),new EmailNotificationJobHandler(mapper),
            new DataProcessingJobHandler(mapper),new HttpRequestJobHandler(mapper),new LongRunningDemoJobHandler(mapper),new DemoFailJobHandler(mapper)));

    @Test void registersEachSupportedTypeWithItsOwnStrategy(){
        assertInstanceOf(ReportJobHandler.class,handlers.get("REPORT"));
        assertInstanceOf(ReportGenerationJobHandler.class,handlers.get("REPORT_GENERATION"));
        assertThrows(IllegalArgumentException.class,()->handlers.get("NOT_REGISTERED"));
    }

    @Test void rejectsDuplicateTypeRegistration(){
        assertThrows(IllegalStateException.class,()->new DemoHandlers(List.of(new ReportJobHandler(mapper),new ReportJobHandler(mapper))));
    }

    @Test void handlersValidatePayloadAndHonorInterruptionFriendlyDemoBehavior() throws Exception {
        assertThrows(IllegalArgumentException.class,()->handlers.get("EMAIL_NOTIFICATION").execute(message("{\"to\":\"\"}")));
        assertThrows(IllegalArgumentException.class,()->handlers.get("HTTP_REQUEST").execute(message("{\"url\":\"file:///etc/passwd\"}")));
        assertThrows(IllegalStateException.class,()->handlers.get("DEMO_FAIL").execute(message("{\"fail\":true}")));
        assertDoesNotThrow(()->handlers.get("DEMO_LONG_RUNNING_TASK").execute(message("{\"durationMs\":0}")));
    }

    private static JobMessage message(String payload){return new JobMessage(1L,2L,1,"REPORT",payload,"MEDIUM",1,30);}
}
