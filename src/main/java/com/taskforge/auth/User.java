package com.taskforge.auth;
import jakarta.persistence.*; import java.time.Instant;
@Entity @Table(name="users",uniqueConstraints=@UniqueConstraint(columnNames="email"))
public class User {
 @Id @GeneratedValue(strategy=GenerationType.IDENTITY) private Long id; @Column(nullable=false,unique=true) private String email; @Column(name="password_hash",nullable=false) private String passwordHash; @Column(nullable=false,length=32) private String role="USER"; @Column(nullable=false) private boolean enabled=true; @Column(name="created_at",nullable=false) private Instant createdAt=Instant.now();
 protected User(){} public User(String email,String passwordHash){this.email=email;this.passwordHash=passwordHash;} public Long getId(){return id;} public String getEmail(){return email;} public String getPasswordHash(){return passwordHash;} public String getRole(){return role;} public boolean isEnabled(){return enabled;} public void disable(){enabled=false;} public void grantAdmin(){role="ADMIN";}
}
