package com.taskforge.worker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class WorkerControllerTest {
    @Test void workerListRejectsOutOfRangePaging(){
        WorkerRepository workers=mock(WorkerRepository.class);
        WorkerController controller=new WorkerController(workers);

        assertThrows(IllegalArgumentException.class,()->controller.all(-1,20,null));
        assertThrows(IllegalArgumentException.class,()->controller.all(0,101,null));

        verifyNoInteractions(workers);
    }
}
