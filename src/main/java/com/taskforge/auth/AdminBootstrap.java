package com.taskforge.auth;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="taskforge.role",havingValue="api",matchIfMissing=true)
public class AdminBootstrap implements ApplicationRunner {
    private final UserRepository users;private final PasswordEncoder encoder;private final String email;private final String password;
    public AdminBootstrap(UserRepository users,PasswordEncoder encoder,@Value("${taskforge.bootstrap-admin-email:}") String email,@Value("${taskforge.bootstrap-admin-password:}") String password){this.users=users;this.encoder=encoder;this.email=email;this.password=password;}
    @Override @Transactional public void run(ApplicationArguments args){
        if(email.isBlank()&&password.isBlank())return;
        if(email.isBlank()||password.length()<16||password.length()>72||!email.trim().matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$"))throw new IllegalStateException("Set both bootstrap admin variables; use a valid email and a password of 16 to 72 characters");
        users.createAdminIfAbsent(email.trim().toLowerCase(java.util.Locale.ROOT),encoder.encode(password));
    }
}
