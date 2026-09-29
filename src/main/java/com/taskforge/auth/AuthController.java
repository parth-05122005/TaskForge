package com.taskforge.auth;
import jakarta.validation.Valid; import jakarta.validation.constraints.*; import org.springframework.http.*; import org.springframework.security.crypto.password.PasswordEncoder; import org.springframework.web.bind.annotation.*;
@RestController @RequestMapping("/api/auth") public class AuthController {
 public record Credentials(@Email @NotBlank String email,@Size(min=10,max=72) String password){} public record Token(String accessToken,String tokenType){ }
 private final UserRepository users;private final PasswordEncoder encoder;private final JwtService jwt;public AuthController(UserRepository u,PasswordEncoder e,JwtService j){users=u;encoder=e;jwt=j;}
 @PostMapping("/register") ResponseEntity<Token> register(@Valid @RequestBody Credentials c){if(users.findByEmail(c.email()).isPresent())throw new IllegalArgumentException("Email is already registered");User u=users.save(new User(c.email().toLowerCase(),encoder.encode(c.password())));return ResponseEntity.status(HttpStatus.CREATED).body(new Token(jwt.issue(u),"Bearer"));}
 @PostMapping("/login") Token login(@Valid @RequestBody Credentials c){User u=users.findByEmail(c.email().toLowerCase()).filter(x->x.isEnabled()&&encoder.matches(c.password(),x.getPasswordHash())).orElseThrow(()->new org.springframework.security.authentication.BadCredentialsException("Invalid credentials"));return new Token(jwt.issue(u),"Bearer");}
}
