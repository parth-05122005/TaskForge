package com.taskforge.worker;
import com.taskforge.job.JobMessage;
public interface JobHandler { String type(); void execute(JobMessage job) throws Exception; }
