package com.taskforge.common;

import com.taskforge.auth.UserRepository;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Locale;
import java.util.NoSuchElementException;

@Service
public class AdminAuditService {
    private final UserRepository users;
    private final AdminAuditLogRepository audit;

    public AdminAuditService(UserRepository users, AdminAuditLogRepository audit) {
        this.users = users;
        this.audit = audit;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void record(String actorEmail, String resourceType, String resourceId, String action, String detail) {
        String normalizedEmail = actorEmail.trim().toLowerCase(Locale.ROOT);
        Long actorId = users.findByEmail(normalizedEmail)
                .orElseThrow(() -> new NoSuchElementException("Admin user not found"))
                .getId();
        String requestId = null;
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            HttpServletRequest request = attributes.getRequest();
            requestId = RequestIdFilter.idFor(request);
        }
        audit.save(new AdminAuditLog(actorId, resourceType, resourceId, action, detail, requestId));
    }
}
