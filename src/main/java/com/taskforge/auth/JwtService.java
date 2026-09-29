package com.taskforge.auth;
import io.jsonwebtoken.Jwts; import io.jsonwebtoken.security.Keys; import org.springframework.beans.factory.annotation.Value; import org.springframework.stereotype.Service; import javax.crypto.SecretKey; import java.nio.charset.StandardCharsets; import java.time.*; import java.util.Date;
@Service public class JwtService {
 private final SecretKey key; private final Duration ttl;
 public JwtService(@Value("${taskforge.jwt-secret}") String secret,@Value("${taskforge.jwt-ttl}") Duration ttl){if(secret.getBytes(StandardCharsets.UTF_8).length<32)throw new IllegalArgumentException("JWT secret must be at least 32 bytes");this.key=Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));this.ttl=ttl;}
 public String issue(User user){Instant now=Instant.now();return Jwts.builder().subject(user.getEmail()).claim("role",user.getRole()).issuedAt(Date.from(now)).expiration(Date.from(now.plus(ttl))).signWith(key).compact();}
 public String subject(String token){return Jwts.parser().verifyWith(key).build().parseSignedClaims(token).getPayload().getSubject();}
}
