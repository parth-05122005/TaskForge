package com.taskforge.common;

import com.taskforge.auth.AdminService;
import com.taskforge.job.JobRepository;
import com.taskforge.worker.WorkerRepository;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class AdminControllerTest {
    @Test void auditRequiresBothNonblankResourceFilters(){
        JobRepository jobs=mock(JobRepository.class);
        WorkerRepository workers=mock(WorkerRepository.class);
        AdminService admin=mock(AdminService.class);
        AdminAuditLogRepository audit=mock(AdminAuditLogRepository.class);
        AdminController controller=new AdminController(jobs,workers,admin,audit);

        assertThrows(IllegalArgumentException.class,()->controller.audit(0,50,"USER",null));
        assertThrows(IllegalArgumentException.class,()->controller.audit(0,50,null,"17"));
        assertThrows(IllegalArgumentException.class,()->controller.audit(0,50," ","17"));

        verifyNoInteractions(audit);
    }

    @Test void adminWorkerListRejectsOutOfRangePaging(){
        WorkerRepository workers=mock(WorkerRepository.class);
        AdminController controller=new AdminController(mock(JobRepository.class),workers,mock(AdminService.class),mock(AdminAuditLogRepository.class));

        assertThrows(IllegalArgumentException.class,()->controller.workers(0,101,null));
        verifyNoInteractions(workers);
    }
}
