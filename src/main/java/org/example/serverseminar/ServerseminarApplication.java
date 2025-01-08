package org.example.serverseminar;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class ServerseminarApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerseminarApplication.class, args);

        // 최종적으로 등록된 빈
        ApplicationContext context = SpringApplication.run(ServerseminarApplication.class, args);

        System.out.println("\n\nSpring Context에 등록된 빈 이름 출력");
        for (String beanName : context.getBeanDefinitionNames()) {
            System.out.println(beanName);
        }
    }
}