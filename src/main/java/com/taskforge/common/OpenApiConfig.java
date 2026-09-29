package com.taskforge.common;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    OpenAPI taskForgeOpenApi() {
        return new OpenAPI()
                .info(new Info().title("TaskForge API").version("0.1.0")
                        .description("Authenticated API for scheduling, executing, and monitoring background jobs."))
                .components(new Components().addSecuritySchemes("bearerAuth",new SecurityScheme()
                        .type(SecurityScheme.Type.HTTP).scheme("bearer").bearerFormat("JWT")));
    }
}
