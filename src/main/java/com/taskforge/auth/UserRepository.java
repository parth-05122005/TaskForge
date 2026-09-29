package com.taskforge.auth;
import org.springframework.data.jpa.repository.JpaRepository; import java.util.Optional;
public interface UserRepository extends JpaRepository<User,Long>{Optional<User> findByEmail(String email);boolean existsByIdAndEnabledTrue(Long id);
 @org.springframework.data.jpa.repository.Lock(jakarta.persistence.LockModeType.PESSIMISTIC_WRITE) @org.springframework.data.jpa.repository.Query("select u from User u where u.role='ADMIN' and u.enabled=true order by u.id") java.util.List<User> lockEnabledAdmins();
 @org.springframework.data.jpa.repository.Modifying @org.springframework.data.jpa.repository.Query(value="update users set enabled=false where id=:id and enabled=true",nativeQuery=true) int disableIfEnabled(@org.springframework.data.repository.query.Param("id") Long id);
 @org.springframework.data.jpa.repository.Modifying @org.springframework.data.jpa.repository.Query(value="insert into users(email,password_hash,role,enabled,created_at) values (:email,:passwordHash,'ADMIN',true,now()) on conflict (email) do nothing",nativeQuery=true) int createAdminIfAbsent(@org.springframework.data.repository.query.Param("email") String email,@org.springframework.data.repository.query.Param("passwordHash") String passwordHash);
}
