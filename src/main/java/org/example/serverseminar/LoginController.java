//package org.example.serverseminar;
//
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RequestParam;
//import org.springframework.web.bind.annotation.RestController;
//
//@RestController
//@RequestMapping("/login")
//public class LoginController {
//    private final KafkaTemplate<String, String> kafkaTemplate;
//
//    public LoginController(KafkaTemplate<String, String> kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//    }
//
//    @PostMapping
//    public String login(@RequestParam String username, @RequestParam String password) {
//        boolean loginSuccess = "user".equals(username) && "password".equals(password);
//
//        if (loginSuccess) {
//            kafkaTemplate.send("login-events", "User : " + username, " logged in.");
//            return "Login Successful";
//        } else {
//            return "Login Failed";
//        }
//    }
//}