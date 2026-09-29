package com.taskforge.auth;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

class JwtFilterTest {
    @AfterEach void clearAuthentication() {
        SecurityContextHolder.clearContext();
    }

    @Test void existingJwtDoesNotAuthenticateDisabledAccount() throws Exception {
        JwtService jwt = mock(JwtService.class);
        UserRepository users = mock(UserRepository.class);
        User disabled = mock(User.class);
        when(jwt.subject("still-valid-signature")).thenReturn("disabled@example.com");
        when(users.findByEmail("disabled@example.com")).thenReturn(Optional.of(disabled));
        when(disabled.isEnabled()).thenReturn(false);
        JwtFilter filter = new JwtFilter(jwt, users);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Authorization", "Bearer still-valid-signature");

        filter.doFilter(request, new MockHttpServletResponse(), mock(FilterChain.class));

        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }
}
