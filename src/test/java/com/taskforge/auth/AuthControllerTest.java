package com.taskforge.auth;

import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AuthControllerTest {
    @Test void registrationNormalizesEmailHashesPasswordAndCreatesUserRole(){
        UserRepository users=mock(UserRepository.class);
        PasswordEncoder encoder=mock(PasswordEncoder.class);
        JwtService jwt=mock(JwtService.class);
        when(users.findByEmail("alice@example.com")).thenReturn(Optional.empty());
        when(encoder.encode("long-enough-password")).thenReturn("bcrypt-hash");
        when(users.save(any(User.class))).thenAnswer(call->call.getArgument(0));
        when(jwt.issue(any(User.class))).thenReturn("signed-token");

        var response=new AuthController(users,encoder,jwt).register(new AuthController.Credentials(" Alice@Example.com ","long-enough-password"));

        assertEquals(201,response.getStatusCode().value());
        assertEquals("signed-token",response.getBody().accessToken());
        assertEquals("Bearer",response.getBody().tokenType());
        var captured=org.mockito.ArgumentCaptor.forClass(User.class);
        verify(users).save(captured.capture());
        assertEquals("alice@example.com",captured.getValue().getEmail());
        assertEquals("bcrypt-hash",captured.getValue().getPasswordHash());
        assertEquals("USER",captured.getValue().getRole());
    }

    @Test void disabledUserCannotLogIn(){
        UserRepository users=mock(UserRepository.class);
        PasswordEncoder encoder=mock(PasswordEncoder.class);
        JwtService jwt=mock(JwtService.class);
        User disabled=mock(User.class);
        when(users.findByEmail("disabled@example.com")).thenReturn(Optional.of(disabled));
        when(disabled.isEnabled()).thenReturn(false);

        assertThrows(org.springframework.security.authentication.BadCredentialsException.class,()->new AuthController(users,encoder,jwt).login(new AuthController.Credentials("disabled@example.com","long-enough-password")));
        verifyNoInteractions(encoder,jwt);
    }
}
