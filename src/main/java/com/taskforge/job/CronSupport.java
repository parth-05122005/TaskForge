package com.taskforge.job;

import org.springframework.scheduling.support.CronExpression;
import java.time.*;

public final class CronSupport {
    private CronSupport(){}
    public static String normalize(String expression){
        if(expression==null||expression.isBlank())throw new IllegalArgumentException("cronExpression is required for CRON schedules");
        String value=expression.trim().replaceAll("\\s+"," ");
        if(value.split(" ").length==5)value="0 "+value;
        try{CronExpression.parse(value);}catch(IllegalArgumentException e){throw new IllegalArgumentException("cronExpression must be a valid 5- or 6-field cron expression",e);}
        return value;
    }
    public static Instant next(String expression,String zone,Instant from){
        try{return CronExpression.parse(normalize(expression)).next(from.atZone(ZoneId.of(zone))).toInstant();}
        catch(NullPointerException e){throw new IllegalArgumentException("cronExpression has no future execution",e);}
        catch(DateTimeException e){throw new IllegalArgumentException("Invalid timeZone: "+zone,e);}
    }
}
