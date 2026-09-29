package com.taskforge.common;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.cors.CorsConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

class SecurityConfigTest {
    private CorsConfiguration forApi(String origins){
        MockHttpServletRequest request=new MockHttpServletRequest();
        request.setRequestURI("/api/jobs");
        return new SecurityConfig().corsConfigurationSource(origins).getCorsConfiguration(request);
    }

    @Test void exactConfiguredOriginsAreAllowedWithoutCredentialCookies(){
        CorsConfiguration cors=forApi("https://console.example.com, https://admin.example.com");

        assertEquals("https://console.example.com",cors.checkOrigin("https://console.example.com"));
        assertNull(cors.checkOrigin("https://unexpected.example.com"));
        assertFalse(cors.getAllowCredentials());
        assertFalse(cors.getAllowedOrigins().contains("*"));
    }

    @Test void emptyAllowlistDeniesCrossOriginRequestsAndWildcardIsRejected(){
        assertNull(forApi("").checkOrigin("https://console.example.com"));
        assertThrows(IllegalArgumentException.class,()->forApi("*"));
    }

    @Test void dashboardGetsRestrictiveBrowserPolicyWithoutChangingSwaggerPolicy(){
        var writer=new SecurityConfig().dashboardPageSecurityHeaders();
        MockHttpServletRequest dashboard=new MockHttpServletRequest();dashboard.setRequestURI("/");
        MockHttpServletResponse dashboardResponse=new MockHttpServletResponse();

        writer.writeHeaders(dashboard,dashboardResponse);

        assertTrue(dashboardResponse.getHeader("Content-Security-Policy").toString().contains("script-src 'self'"));
        assertTrue(dashboardResponse.getHeader("Content-Security-Policy").toString().contains("frame-ancestors 'none'"));
        assertEquals("strict-origin-when-cross-origin",dashboardResponse.getHeader("Referrer-Policy"));
        assertEquals("camera=(), microphone=(), geolocation=()",dashboardResponse.getHeader("Permissions-Policy"));

        MockHttpServletRequest swagger=new MockHttpServletRequest();swagger.setRequestURI("/swagger-ui.html");
        MockHttpServletResponse swaggerResponse=new MockHttpServletResponse();
        writer.writeHeaders(swagger,swaggerResponse);
        assertNull(swaggerResponse.getHeader("Content-Security-Policy"));
    }

    @Test void authenticationAndAuthorizationFailuresReturnConsistentJsonErrors() throws Exception {
        ObjectMapper mapper=new ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        MockHttpServletRequest request=new MockHttpServletRequest();
        request.setRequestURI("/api/admin/statistics");
        request.setAttribute(RequestIdFilter.REQUEST_ID_ATTRIBUTE,"request-42");

        MockHttpServletResponse unauthorized=new MockHttpServletResponse();
        new SecurityConfig().authenticationEntryPoint(mapper).commence(request,unauthorized,new BadCredentialsException("ignored"));
        var authBody=mapper.readTree(unauthorized.getContentAsString());
        assertEquals(401,unauthorized.getStatus());
        assertEquals("UNAUTHORIZED",authBody.path("error").asText());
        assertEquals("/api/admin/statistics",authBody.path("path").asText());
        assertEquals("request-42",authBody.path("requestId").asText());
        assertTrue(authBody.has("timestamp"));
        assertTrue(authBody.has("message"));

        MockHttpServletResponse forbidden=new MockHttpServletResponse();
        new SecurityConfig().accessDeniedHandler(mapper).handle(request,forbidden,new AccessDeniedException("ignored"));
        var forbiddenBody=mapper.readTree(forbidden.getContentAsString());
        assertEquals(403,forbidden.getStatus());
        assertEquals("FORBIDDEN",forbiddenBody.path("error").asText());
        assertEquals("/api/admin/statistics",forbiddenBody.path("path").asText());
        assertEquals("request-42",forbiddenBody.path("requestId").asText());
        assertTrue(forbiddenBody.has("timestamp"));
        assertTrue(forbiddenBody.has("message"));
    }
}
