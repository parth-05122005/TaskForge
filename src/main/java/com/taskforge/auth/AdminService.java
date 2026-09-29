package com.taskforge.auth;
import org.springframework.stereotype.Service;import org.springframework.transaction.annotation.Transactional;import java.util.NoSuchElementException;import com.taskforge.common.AdminAuditLog;import com.taskforge.common.AdminAuditLogRepository;
@Service public class AdminService {
 private final UserRepository users;private final AdminAuditLogRepository audit;
 public AdminService(UserRepository users,AdminAuditLogRepository audit){this.users=users;this.audit=audit;}
 @Transactional public void disable(Long userId,String actorEmail){User actor=users.findByEmail(actorEmail.trim().toLowerCase(java.util.Locale.ROOT)).orElseThrow(()->new NoSuchElementException("Admin user not found"));User user=users.findById(userId).orElseThrow(()->new NoSuchElementException("User not found"));if(!user.isEnabled())return;if("ADMIN".equals(user.getRole())){var enabledAdmins=users.lockEnabledAdmins();if(!users.existsByIdAndEnabledTrue(userId))return;if(enabledAdmins.size()<=1)throw new IllegalStateException("The last enabled admin cannot be disabled");}if(users.disableIfEnabled(userId)==0)return;audit.save(new AdminAuditLog(actor.getId(),user.getId(),"USER_DISABLED","Account disabled by administrator"));}
}
