package com.taskforge.common;
import jakarta.servlet.http.HttpServletRequest;import org.springframework.http.*;import org.springframework.web.bind.MethodArgumentNotValidException;import org.springframework.web.bind.annotation.*;import java.time.Instant;import java.util.*;
@RestControllerAdvice public class ApiExceptionHandler {
 public record ApiError(Instant timestamp,int status,String error,String message,String path,String requestId){}
 @ExceptionHandler(NoSuchElementException.class) ResponseEntity<ApiError> missing(NoSuchElementException e,HttpServletRequest r){return error(HttpStatus.NOT_FOUND,"NOT_FOUND",e.getMessage(),r);}
 @ExceptionHandler(IllegalArgumentException.class) ResponseEntity<ApiError> bad(IllegalArgumentException e,HttpServletRequest r){return error(HttpStatus.BAD_REQUEST,"VALIDATION_ERROR",e.getMessage(),r);}
 @ExceptionHandler(IllegalStateException.class) ResponseEntity<ApiError> conflict(IllegalStateException e,HttpServletRequest r){return error(HttpStatus.CONFLICT,"INVALID_STATE",e.getMessage(),r);}
 @ExceptionHandler(MethodArgumentNotValidException.class) ResponseEntity<ApiError> validation(MethodArgumentNotValidException e,HttpServletRequest r){return error(HttpStatus.BAD_REQUEST,"VALIDATION_ERROR",e.getBindingResult().getFieldErrors().stream().map(x->x.getField()+": "+x.getDefaultMessage()).findFirst().orElse("Invalid request"),r);}
 private ResponseEntity<ApiError> error(HttpStatus s,String code,String message,HttpServletRequest r){String id=Optional.ofNullable(r.getHeader("X-Request-ID")).orElse(UUID.randomUUID().toString());return ResponseEntity.status(s).body(new ApiError(Instant.now(),s.value(),code,message,r.getRequestURI(),id));}
}
