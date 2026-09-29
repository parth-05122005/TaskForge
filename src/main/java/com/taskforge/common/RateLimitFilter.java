package com.taskforge.common;

import com.fasterxml.jackson.databind.ObjectMapper;import org.springframework.data.redis.core.StringRedisTemplate;import org.springframework.data.redis.core.script.DefaultRedisScript;import org.springframework.stereotype.Component;import org.springframework.web.filter.OncePerRequestFilter;
import jakarta.servlet.*;import jakarta.servlet.http.*;import java.io.IOException;import java.time.Instant;import java.util.List;

@Component public class RateLimitFilter extends OncePerRequestFilter {
 private static final DefaultRedisScript<Long> LIMIT=new DefaultRedisScript<>("local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]); end; return n",Long.class);
 private final StringRedisTemplate redis;private final ObjectMapper mapper;public RateLimitFilter(StringRedisTemplate redis,ObjectMapper mapper){this.redis=redis;this.mapper=mapper;}
 @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException{
  String path=request.getRequestURI();if(!path.startsWith("/api/")){chain.doFilter(request,response);return;}
  boolean auth=path.startsWith("/api/auth/");long limit=auth?10:180;String window=Long.toString(Instant.now().getEpochSecond()/60);String ip=request.getRemoteAddr();
  try{Long count=redis.execute(LIMIT,List.of("taskforge:rate:"+window+":"+ip+":"+(auth?"auth":"api")),"70");if(count!=null&&count>limit){response.setStatus(429);response.setContentType("application/json");mapper.writeValue(response.getWriter(),new ApiExceptionHandler.ApiError(Instant.now(),429,"RATE_LIMITED","Request limit exceeded",path,response.getHeader("X-Request-ID")));return;}}
  catch(RuntimeException unavailable){logger.warn("Rate limiter unavailable; allowing request",unavailable);}
  chain.doFilter(request,response);
 }
}
