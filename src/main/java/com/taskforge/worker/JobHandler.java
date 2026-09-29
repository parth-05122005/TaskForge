package com.taskforge.worker;
import com.taskforge.job.JobMessage;
/** Handlers must honor thread interruption and bound their own network I/O. Timeouts cannot safely kill arbitrary Java code. */
public interface JobHandler { String type(); void execute(JobMessage job) throws Exception; }
