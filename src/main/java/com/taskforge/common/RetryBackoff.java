package com.taskforge.common;

public final class RetryBackoff {
    private RetryBackoff() {}

    /** Full jitter reduces synchronized retry bursts across workers and relay instances. */
    public static long seconds(int attempt, long baseSeconds, long maxSeconds) {
        if (attempt < 1) throw new IllegalArgumentException("attempt must be positive");
        if (baseSeconds < 1 || maxSeconds < baseSeconds) throw new IllegalArgumentException("retry delays must satisfy 1 <= base <= max");
        if (baseSeconds == maxSeconds) return maxSeconds;
        int shift=Math.min(30,attempt);
        long multiplier=1L<<shift;
        long ceiling=baseSeconds>maxSeconds/multiplier?maxSeconds:baseSeconds*multiplier;
        long floor=Math.max(1,ceiling/2);
        if(floor==ceiling)return ceiling;
        long exclusiveBound=ceiling==Long.MAX_VALUE?Long.MAX_VALUE:ceiling+1;
        return java.util.concurrent.ThreadLocalRandom.current().nextLong(floor,exclusiveBound);
    }
}
