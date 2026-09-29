package com.taskforge.common;

public final class RetryBackoff {
    private RetryBackoff() {}

    public static long seconds(int attempt, long baseSeconds, long maxSeconds) {
        if (attempt < 1) throw new IllegalArgumentException("attempt must be positive");
        if (baseSeconds < 1 || maxSeconds < baseSeconds) throw new IllegalArgumentException("retry delays must satisfy 1 <= base <= max");
        if (baseSeconds == maxSeconds) return maxSeconds;
        int shift=Math.min(30,attempt);
        long multiplier=1L<<shift;
        return baseSeconds>maxSeconds/multiplier?maxSeconds:baseSeconds*multiplier;
    }
}
