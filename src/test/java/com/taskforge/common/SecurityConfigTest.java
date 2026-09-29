package com.taskforge.common;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.cors.CorsConfiguration;

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
}
