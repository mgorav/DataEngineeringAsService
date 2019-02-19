package com.gonnect.dataengineering.apis;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PythonController {

    @PostMapping("/py")
    public String greet(@RequestBody String payload) {
        return payload + "Http";
    }
}
