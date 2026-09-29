package com.taskforge.common;

import jakarta.servlet.*;import jakarta.servlet.http.*;import org.slf4j.MDC;import org.springframework.stereotype.Component;import org.springframework.web.filter.OncePerRequestFilter;
import java.io.IOException;import java.util.UUID;

@Component public class RequestIdFilter extends OncePerRequestFilter {
 public static final String REQUEST_ID_ATTRIBUTE="taskforge.requestId";
 public static String idFor(HttpServletRequest request){Object id=request.getAttribute(REQUEST_ID_ATTRIBUTE);return id instanceof String value?value:java.util.Optional.ofNullable(request.getHeader("X-Request-ID")).orElse("");}
 @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException{
  String requested=request.getHeader("X-Request-ID");String id=requested!=null&&requested.matches("[A-Za-z0-9._-]{1,64}")?requested:UUID.randomUUID().toString();
  request.setAttribute(REQUEST_ID_ATTRIBUTE,id);response.setHeader("X-Request-ID",id);MDC.put("requestId",id);try{chain.doFilter(request,response);}finally{MDC.remove("requestId");}
 }
}
